/**
  * (C) Copyright IBM Corporation 2020.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package com.ibm.m4d.mover.datastore.kafka

import com.ibm.m4d.mover.spark._
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.spark.sql.avro.IncompatibleSchemaException
import org.apache.spark.sql.avro.SchemaConverters.{nullSchema, toAvroType}
import org.apache.spark.sql.functions.{col, to_json, when}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.{ConfluentConstants, SchemaManagerFactory}
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}

import java.nio.ByteBuffer
import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * This is a collection of general methods needed to interact with Kafka systems.
  * This involves methods for subscribing to and writing to topics in stream and batch mode
  * as well as methods for serializing/deserializing data and converting it to Spark catalyst format.
  * A confluent compatible registry can also be used for this.
  */
object KafkaUtils {
  private val logger = LoggerFactory.getLogger(KafkaUtils.getClass)
  val LogicalType = "logicalType"
  val DBColumnName = "dbColumnName"
  val DateFormatStr = "yyyy-MM-dd"
  val TimeFormatStr = "HH:mm:ss"
  val TimestampFormatStr = "yyyy-MM-dd HH:mm:ss.SSS"

  val RandomSuffixLength = 10

  /**
    * Generates a Kafka group.id that can be used to read from Kafka.
    *
    * @param asset asset name of the topic to read from
    * @param jobName job name (optional)
    * @return
    */
  private[kafka] def genKafkaGroupId(asset: String, jobName: String = ""): String = {
    if (jobName.isEmpty) {
      asset + "-" + RandomStringUtils.randomAlphanumeric(RandomSuffixLength)
    } else {
      asset + "-" + jobName + "-" + RandomStringUtils.randomAlphanumeric(RandomSuffixLength)
    }
  }

  private def confluentRegistryConfigurationReading(schemaRegistryUrl: String, kafkaTopic: String, isKey: Boolean = false): FromAvroConfig = {
    AbrisConfig.fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(kafkaTopic, isKey = isKey)
      .usingSchemaRegistry(schemaRegistryUrl)
  }

  def mapToValue(df: DataFrame): DataFrame = {
    if (df.isStreaming) {
      df.select("value.*")
    } else {
      df.sparkSession
        .createDataFrame(
          df.select("value.*").rdd,
          df.schema.field("value")
            .dataType.asInstanceOf[StructType]
        )
    }
  }

  def readKafkaStream(spark: SparkSession, kafkaConfig: Kafka): DataFrame = {
    // val kafkaGroupId = KafkaUtils.genKafkaGroupId(assetName)

    // TODO investigate Kafka compaction
    logger.info("Reading from secure Kafka cluster...")
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
      // .option("kafka.group.id", kafkaGroupId)
      .option("subscribe", kafkaConfig.kafkaTopic)
      .option("startingOffsets", "earliest")
      .options(kafkaConfig.getCredentialProperties("kafka."))
      //      .option("endingOffsets", kafkaConfig.getEndingOffset)
      .load()

    // TODO The df should be augmented with the meta-data from the registry, i.e.
    // the avro schema should be attached to each column.
    kafkaBytesToCatalyst(df, kafkaConfig)
  }

  /**
    * Read data from Kafka as a snapshot.
    * Either create a snapshot or don't depending on the createSnapshot parameter.
    *
    * @param spark [[SparkSession]] to use
    * @param kafkaConfig Kafka configuration
    * @return
    */
  private def readFromKafkaRaw(spark: SparkSession, kafkaConfig: Kafka): DataFrame = {
    // val kafkaGroupId = KafkaUtils.genKafkaGroupId(assetName)

    // TODO investigate Kafka compaction
    logger.info("Reading from Kafka cluster...")
    spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
      // .option("kafka.group.id", kafkaGroupId)
      .option("subscribe", kafkaConfig.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .options(kafkaConfig.getCredentialProperties("kafka."))
      //      .option("endingOffsets", kafkaConfig.getEndingOffset)
      .load()
  }

  def extractConfluentSchemaID(bytes: Array[Byte]): Int = {
    val buffer = ByteBuffer.wrap(bytes)
    if (buffer.get() != ConfluentConstants.MAGIC_BYTE) {
      -1
    } else {
      Try(buffer.getInt()).getOrElse(-1)
    }
  }

  /**
    * Read data from Kafka as a snapshot.
    * Either create a snapshot or don't depending on the createSnapshot parameter.
    *
    * @param spark [[SparkSession]] to use
    * @param kafkaConfig Kafka configuration
    * @return
    */
  def readFromKafka(spark: SparkSession, kafkaConfig: Kafka): DataFrame = {
    val df = readFromKafkaRaw(spark, kafkaConfig)

    if (kafkaConfig.raw) {
      val (keyConf, valueConf) = kafkaConfig.schemaRegistryURL match {
        case Some(url) =>
          (
            confluentRegistryConfigurationReading(url, kafkaConfig.kafkaTopic, isKey = true),
            confluentRegistryConfigurationReading(url, kafkaConfig.kafkaTopic, isKey = false)
          )
        case None => throw new IllegalArgumentException("Please define registry to debug!")
      }

      val extractSchemaID = org.apache.spark.sql.functions.udf(extractConfluentSchemaID _)

      df.withColumn("data_key", from_avro(col("key"), keyConf))
        .withColumn("data_value", from_avro(col("value"), valueConf))
        .withColumn("key_schema_id", extractSchemaID(col("key")))
        .withColumn("value_schema_id", extractSchemaID(col("value")))
    } else {
      // TODO The df should be augmented with the meta-data from the registry, i.e.
      // the avro schema should be attached to each column.

      kafkaBytesToCatalyst(df, kafkaConfig)
    }
  }

  private[kafka] def kafkaBytesToCatalyst(df: DataFrame, kafkaConfig: Kafka): DataFrame = {
    import org.apache.spark.sql.functions._
    val sparkDF = kafkaConfig.serializationFormat match {
      case SerializationFormat.Avro =>
        (kafkaConfig.schemaRegistryURL, kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
          case (Some(registry), None, None) => // Load schema from confluent registry
            val keyConf = confluentRegistryConfigurationReading(registry, kafkaConfig.kafkaTopic, isKey = true)
            val valueConf = confluentRegistryConfigurationReading(registry, kafkaConfig.kafkaTopic, isKey = false)
            df.withColumn("key", from_avro(col("key"), keyConf).as("key"))
              .withColumn("value", from_avro(col("value"), valueConf).as("value"))

          case (None, None, Some(valueSchema)) => // se schema that is specified in valueSchema
            df.withColumn("value", from_avro(col("value"), valueSchema).as("value"))

          case (None, Some(keySchema), Some(valueSchema)) => // se schema that is specified in valueSchema
            df.withColumn("key", from_avro(col("key"), keySchema).as("key"))
              .withColumn("value", from_avro(col("value"), valueSchema).as("value"))

          case (_, _, _) => throw new IllegalArgumentException("Case not supported!")
        }

      case SerializationFormat.JSON =>
        (kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
          case (None, Some(valueSchema)) => // Use schema that is specified in valueSchema
            // TODO support multiple schema formats?
            df.withColumn("value", from_json(df("value").cast(StringType), valueSchema, Map.empty[String, String]).as("value"))

          case (Some(keySchema), Some(valueSchema)) => // Use schema that is specified in valueSchema
            // TODO support multiple schema formats?
            df.withColumn("key", from_json(df("key").cast(StringType), keySchema, Map.empty[String, String]).as("key"))
              .withColumn("value", from_json(df("value").cast(StringType), valueSchema, Map.empty[String, String]).as("value"))

          case (None, None) => // Infer schema from first row
            val firstRow = df.select(df("value").cast(StringType)).head().getAs[String](0)
            val schema = df.select(schema_of_json(firstRow)).head().getAs[String](0)
            df.withColumn("value", from_json(df("value").cast(StringType), schema, Map.empty[String, String]).as("value"))

          case (_, _) => throw new IllegalArgumentException("Case not supported!")
        }
      case _ => throw new IllegalArgumentException("Data type not supported for Kafka!")
    }

    sparkDF
  }

  def toKafkaWriteableDF(df: DataFrame, keyColumns: Seq[org.apache.spark.sql.Column], noKey: Boolean = false): DataFrame = {
    val spark = df.sparkSession

    // transform orginal dataframe to a Kafka compatible one
    val transformedDF = df

    if (noKey) {
      val valueSchema = transformedDF.schema
      val valueColumns = transformedDF.schema.fields.map(f => transformedDF(f.name))
      val allColumns = valueSchema.fields.map(_.name)

      val sqlSchema = StructType(Array(
        StructField("value", valueSchema, nullable = false)
      ))

      logger.info("Current schema: " + valueSchema)
      logger.info("Forced SQL Schema: " + sqlSchema.toString())

      import org.apache.spark.sql.functions.struct

      val projectedDF = transformedDF
        .withColumn("value", struct(valueColumns: _*))
        .drop(allColumns: _*)

      // Building correct schema
      val forcedSchemaDF = spark.createDataFrame(projectedDF.rdd, sqlSchema)
      forcedSchemaDF
    } else {
      val (dfWithId, finalKeyColumns) = if (keyColumns.nonEmpty) {
        (transformedDF, keyColumns)
      } else {
        import org.apache.spark.sql.functions.monotonically_increasing_id

        val newDF = transformedDF.withColumn("artificial_id", monotonically_increasing_id())
        (newDF, Seq(newDF("artificial_id")))
      }

      val keyColumnsDF = dfWithId.select(finalKeyColumns: _*)

      val keySchema = StructType(keyColumnsDF.schema.fields)
      val valueSchema = transformedDF.schema
      val valueColumns = transformedDF.schema.fields.map(f => dfWithId(f.name))

      val allColumns = (keySchema.fields.map(_.name) ++ valueSchema.fields.map(_.name)).distinct

      val sqlSchema = StructType(Array(
        StructField("key", keySchema, nullable = false),
        StructField("value", valueSchema, nullable = false)
      ))

      logger.info("Current schema: " + dfWithId.schema)
      logger.info("Forced SQL Schema: " + sqlSchema.toString())

      import org.apache.spark.sql.functions.struct

      val projectedDF = dfWithId
        .withColumn("key", struct(finalKeyColumns: _*))
        .withColumn("value", struct(valueColumns: _*))
        .drop(allColumns: _*)

      // Building correct schema
      val forcedSchemaDF = spark.createDataFrame(projectedDF.rdd, sqlSchema)
      forcedSchemaDF
    }
  }

  def toAvroType(
      catalystType: DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      nameSpace: String = ""
  ): Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType                        => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType                           => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType  => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _  => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case BinaryType => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            toAvroType(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
    if (nullable) {
      Schema.createUnion(nullSchema, schema)
    } else {
      schema
    }
  }

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  private[kafka] def registerSchemaForColumn(
      df: DataFrame,
      schemaRegistryUrl: String,
      kafkaTopic: String,
      columnName: String
  ): ToAvroConfig = {
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)

    val fieldIndex = df.schema.fieldIndex(columnName)
    val field = df.schema.fields(fieldIndex)

    val schema = toAvroType(field.dataType, nullable = false, "name", "namespace")
    val nullableSchema = if (field.nullable) toAvroType(field.dataType, field.nullable, "name", "namespace") else schema
    val subject = SchemaSubject.
      usingTopicNameStrategy(kafkaTopic, isKey = columnName.equalsIgnoreCase("key"))
    val schemaId = schemaManager.getIfExistsOrElseRegisterSchema(schema, subject)

    AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryUrl)
      .withSchema(nullableSchema.toString())
  }

  private[kafka] def catalystToKafka(df: DataFrame, kafkaConfig: Kafka): DataFrame = {
    import za.co.absa.abris.avro.functions.to_avro

    if (df.schema.fieldNames.contains("key")) {
      kafkaConfig.serializationFormat match {
        case SerializationFormat.Avro =>
          (kafkaConfig.schemaRegistryURL, kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
            case (Some(url), None, None) =>
              val keyConfig = registerSchemaForColumn(df, url, kafkaConfig.kafkaTopic, "key")
              val valueConfig = registerSchemaForColumn(df, url, kafkaConfig.kafkaTopic, "value")
              df.select(
                to_avro(df("key"), keyConfig).as("key"),
                to_avro(df("value"), valueConfig).as("value")
              )
            case (None, Some(keySchema), Some(valueSchema)) =>
              df.select(
                to_avro(df("key"), keySchema).as("key"),
                to_avro(df("value"), valueSchema).as("value")
              )
            case (None, None, None) =>
              val keySchema = AvroSchemaUtils.toAvroSchema(df, "key", "key", kafkaConfig.kafkaTopic)
              val valueSchema = AvroSchemaUtils.toAvroSchema(df, "value", "value", kafkaConfig.kafkaTopic)

              df.select(
                to_avro(df("key"), keySchema.toString).as("key"),
                to_avro(df("value"), valueSchema.toString).as("value")
              )
            case (_, _, _) => throw new IllegalArgumentException("Need to specify either schema registry, schema or nothing!")
          }

        case SerializationFormat.JSON =>
          import org.apache.spark.sql.functions.to_json
          df.select(to_json(df("key")).as("key"), to_json(df("value")).as("value"))
        case _ => throw new IllegalArgumentException("Not supported as Kafka format!")
      }
    } else {
      kafkaConfig.serializationFormat match {
        case SerializationFormat.Avro =>
          (kafkaConfig.schemaRegistryURL, kafkaConfig.valueSchema) match {
            case (Some(url), None) =>
              val valueConfig = registerSchemaForColumn(df, url, kafkaConfig.kafkaTopic, "value")
              df.select(to_avro(df("value"), valueConfig).as("value"))
            case (None, Some(valueSchema)) =>
              df.select(to_avro(df("value"), valueSchema).as("value"))
            case (None, None) =>
              val valueSchema = AvroSchemaUtils.toAvroSchema(df, "value", "value", kafkaConfig.kafkaTopic)
              df.select(to_avro(df("value"), valueSchema.toString).as("value"))
            case (_, _) => throw new IllegalArgumentException("Need to specify either schema registry, schema or nothing!")
          }

        case SerializationFormat.JSON =>
          df.select(to_json(df("value")).as("value"))
        case _ => throw new IllegalArgumentException("Not supported as Kafka format!")
      }
    }
  }

  def writeToKafka(df: DataFrame, kafkaConfig: Kafka): Unit = {
    val kafkaDF = catalystToKafka(df, kafkaConfig)

    kafkaDF.write
      .format("kafka")
      .option("topic", kafkaConfig.kafkaTopic)
      .options(kafkaConfig.getCommonProps("kafka."))
      .save()
  }

  def writeToKafkaStream(df: DataFrame, kafkaConfig: Kafka, kafkaTopic: String): DataStreamWriter[Row] = {
    val kafkaDF = catalystToKafka(df, kafkaConfig)

    kafkaDF
      .writeStream
      .format("kafka")
      .option("topic", kafkaConfig.kafkaTopic)
      .options(kafkaConfig.getCommonProps("kafka."))
  }

  def deleteTopic(config: Kafka, topicName: String): Unit = {
    val adminProps = new Properties
    val map = config.getCommonProps()
    // copy the settings from the producer properties.
    map.foreach { case (k, v) => adminProps.put(k, v) }
    // Create admin client
    val adminClient = AdminClient.create(adminProps)
    try { // Define topic
      // Delete topic, which is async call.
      val deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topicName))
      // Since the call is Async, Lets wait for it to complete.
      deleteTopicsResult.values.get(topicName).get
    } catch {
      case e @ (_: InterruptedException | _: ExecutionException) =>
        if (!e.getCause.isInstanceOf[TopicExistsException]) {
          logger.error("Could not create control topic: ", e)
          throw new RuntimeException(e.getMessage, e)
        }
      // TopicExistsException - Swallow this exception, just means the topic already exists.
    } finally if (adminClient != null) adminClient.close()
  }
}
