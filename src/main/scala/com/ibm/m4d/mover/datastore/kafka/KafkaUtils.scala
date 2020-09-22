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

import java.nio.ByteBuffer
import java.security.InvalidParameterException
import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}

import com.ibm.m4d.mover.spark.{SnapshotAggregator, _}
import org.apache.avro.Schema
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.functions.{from_avro, from_confluent_avro}
import za.co.absa.abris.avro.read.confluent.{ConfluentConstants, SchemaManager, SchemaManagerFactory}

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

  private def confluentRegistryConfigurationReading(schemaRegistryUrl: String, kafkaTopic: String): Map[String, String] = {
    Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> kafkaTopic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest",
      SchemaManager.PARAM_KEY_SCHEMA_ID -> "latest"
    )
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
    val df = if (kafkaConfig.user.isEmpty) {
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        // .option("kafka.group.id", kafkaGroupId)
        .option("subscribe", kafkaConfig.kafkaTopic)
        .option("startingOffsets", "earliest")
        //      .option("endingOffsets", kafkaConfig.getEndingOffset)
        .load()
    } else {
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        // .option("kafka.group.id", kafkaGroupId)
        .option("subscribe", kafkaConfig.kafkaTopic)
        .option("startingOffsets", "earliest")
        .options(kafkaConfig.getCredentialProperties("kafka."))
        //      .option("endingOffsets", kafkaConfig.getEndingOffset)
        .load()
    }

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
    logger.info("Reading from secure Kafka cluster...")
    if (kafkaConfig.user.isEmpty) {
      spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        // .option("kafka.group.id", kafkaGroupId)
        .option("subscribe", kafkaConfig.kafkaTopic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        //      .option("endingOffsets", kafkaConfig.getEndingOffset)
        .load()
    } else {
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
      val schemaRegistryConf = kafkaConfig.schemaRegistryURL
        .map(registry => confluentRegistryConfigurationReading(registry, kafkaConfig.kafkaTopic))
        .getOrElse(Map.empty[String, String])

      val extractConfluentSchemaID = (bytes: Array[Byte]) => {
        val buffer = ByteBuffer.wrap(bytes)
        if (buffer.get() != ConfluentConstants.MAGIC_BYTE) {
          -1
        } else {
          Try(buffer.getInt()).getOrElse(-1)
        }
      }

      val extractSchemaID = org.apache.spark.sql.functions.udf(extractConfluentSchemaID, IntegerType)

      df.withColumn("data_key", from_confluent_avro(col("key"), schemaRegistryConf))
        .withColumn("data_value", from_confluent_avro(col("value"), schemaRegistryConf))
        .withColumn("key_schema_id", extractSchemaID(col("key")))
        .withColumn("value_schema_id", extractSchemaID(col("value")))
    } else {
      // TODO The df should be augmented with the meta-data from the registry, i.e.
      // the avro schema should be attached to each column.
      val snapshotDF = if (kafkaConfig.createSnapshot) {
        logger.info("Creating snapshot of Kafka topic...")
        SnapshotAggregator.createSnapshotOnBinary(df)
      } else {
        logger.info("Topic is a refresh only Kafka topic! No need for a snapshot!")
        df
      }

      val sparkDF = kafkaBytesToCatalyst(snapshotDF, kafkaConfig)

      mapToValue(sparkDF)
    }
  }

  private[kafka] def kafkaBytesToCatalyst(df: DataFrame, kafkaConfig: Kafka): DataFrame = {
    import org.apache.spark.sql.functions._
    val sparkDF = kafkaConfig.serializationFormat match {
      case SerializationFormat.Avro =>
        (kafkaConfig.schemaRegistryURL, kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
          case (Some(registry), None, None) => // Load schema from confluent registry
            val schemaRegistryConf = confluentRegistryConfigurationReading(registry, kafkaConfig.kafkaTopic)
            df.select(
              from_confluent_avro(col("key"), schemaRegistryConf).as("key"),
              from_confluent_avro(col("value"), schemaRegistryConf).as("value")
            )

          case (None, None, Some(valueSchema)) => // se schema that is specified in valueSchema
            df.select(from_avro(col("value"), valueSchema).as("value"))

          case (None, Some(keySchema), Some(valueSchema)) => // se schema that is specified in valueSchema
            df.select(
              from_avro(col("key"), keySchema).as("key"),
              from_avro(col("value"), valueSchema).as("value")
            )

          case (_, _, _) => throw new IllegalArgumentException("Case not supported!")
        }

      case SerializationFormat.JSON =>
        (kafkaConfig.schemaRegistryURL, kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
          case (Some(registry), None, None) => // Load schema from confluent registry
            val schemaRegistryConf = confluentRegistryConfigurationReading(registry, kafkaConfig.kafkaTopic)
            val avroSchema = loadSchemaFromRegistry(schemaRegistryConf)
            val dataType = SchemaConverters.toSqlType(avroSchema).dataType
            df.select(
              from_json(df("key").cast(StringType), dataType).as("key"),
              from_json(df("value").cast(StringType), dataType).as("value")
            )

          case (None, None, Some(valueSchema)) => // Use schema that is specified in valueSchema
            // TODO support multiple schema formats?
            df.select(from_json(df("value").cast(StringType), valueSchema, Map.empty[String, String]).as("value"))

          case (None, Some(keySchema), Some(valueSchema)) => // Use schema that is specified in valueSchema
            // TODO support multiple schema formats?
            df.select(
              from_json(df("key").cast(StringType), keySchema, Map.empty[String, String]).as("key"),
              from_json(df("value").cast(StringType), valueSchema, Map.empty[String, String]).as("value")
            )

          case (None, None, None) => // Infer schema from first row
            val firstRow = df.select(df("value").cast(StringType)).head().getAs[String](0)
            val schema = df.select(schema_of_json(firstRow)).head().getAs[String](0)
            df.select(from_json(df("value").cast(StringType), schema, Map.empty[String, String]).as("value"))

          case (_, _, _) => throw new IllegalArgumentException("Case not supported!")
        }
      case _ => throw new IllegalArgumentException("Data type not supported for Kafka!")
    }

    sparkDF
  }

  private def loadSchemaFromRegistry(registryConfig: Map[String, String]): Schema = {
    val valueStrategy = registryConfig.get(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    val keyStrategy = registryConfig.get(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY)

    (valueStrategy, keyStrategy) match {
      case (Some(valueStrategy), None) =>
        SchemaManagerFactory.create(registryConfig).downloadSchema()
      case (None, Some(keyStrategy)) =>
        SchemaManagerFactory.create(registryConfig).downloadSchema()
      case (Some(_), Some(_)) =>
        throw new InvalidParameterException(
          "Both key.schema.naming.strategy and value.schema.naming.strategy were defined. " +
            "Only one of them supposed to be defined!"
        )
      case _ =>
        throw new InvalidParameterException(
          "At least one of key.schema.naming.strategy or value.schema.naming.strategy " +
            "must be defined to use schema registry!"
        )
    }
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

  private[kafka] def catalystToKafka(df: DataFrame, kafkaConfig: Kafka): DataFrame = {
    import za.co.absa.abris.avro.functions.{to_avro, to_confluent_avro}

    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> kafkaConfig.schemaRegistryURL.getOrElse(""),
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> kafkaConfig.kafkaTopic,
      SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> kafkaConfig.kafkaTopic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> kafkaConfig.kafkaTopic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "Value",
      SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "Key",
    )

    val valueRegistryConfig = schemaRegistryConfs +
      (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    val keyRegistryConfig = schemaRegistryConfs +
      (SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

    if (df.schema.fieldNames.contains("key")) {
      kafkaConfig.serializationFormat match {
        case SerializationFormat.Avro =>
          (kafkaConfig.schemaRegistryURL, kafkaConfig.keySchema, kafkaConfig.valueSchema) match {
            case (Some(_), None, None) =>
              df.select(
                to_confluent_avro(df("key"), keyRegistryConfig).as("key"),
                to_confluent_avro(df("value"), valueRegistryConfig).as("value")
              )
            case (None, Some(keySchema), Some(valueSchema)) =>
              df.select(
                to_avro(df("key"), keySchema).as("key"),
                to_avro(df("value"), valueSchema).as("value")
              )
            case (None, None, None) =>
              df.select(
                to_avro(df("key")).as("key"),
                to_avro(df("value")).as("value")
              )
            case (_, _, _) => throw new IllegalArgumentException("Need to specify either schema registry, schema or nothing!")
          }

        case SerializationFormat.JSON =>
          import org.apache.spark.sql.functions.to_json
          val serializedDF = df.select(to_json(df("key")).as("key"), to_json(df("value")).as("value"))
          kafkaConfig.schemaRegistryURL match {
            case Some(registry) => // Write key value schemas to schema registry
              val keyRegistryManager = SchemaManagerFactory.create(keyRegistryConfig)
              val valueRegistryManager = SchemaManagerFactory.create(valueRegistryConfig)
              val keySchema = SchemaConverters.toAvroType(df.schema.fields(df.schema.fieldIndex("key")).dataType, nullable = false, recordName = "Key", nameSpace = kafkaConfig.kafkaTopic)
              val valueSchema = SchemaConverters.toAvroType(df.schema.fields(df.schema.fieldIndex("value")).dataType, nullable = false, recordName = "Value", nameSpace = kafkaConfig.kafkaTopic)
              keyRegistryManager.register(keySchema)
              valueRegistryManager.register(valueSchema)
            case None => // Nothing to do
          }
          serializedDF
        case _ => throw new IllegalArgumentException("Not supported as Kafka format!")
      }
    } else {
      kafkaConfig.serializationFormat match {
        case SerializationFormat.Avro =>
          (kafkaConfig.schemaRegistryURL, kafkaConfig.valueSchema) match {
            case (Some(_), None) =>
              df.select(to_confluent_avro(df("value"), valueRegistryConfig).as("value"))
            case (None, Some(valueSchema)) =>
              df.select(to_avro(df("value"), valueSchema).as("value"))
            case (None, None) =>
              df.select(to_avro(df("value")).as("value"))
            case (_, _) => throw new IllegalArgumentException("Need to specify either schema registry, schema or nothing!")
          }

        case SerializationFormat.JSON =>
          val serializedDF = df.select(to_json(df("value")).as("value"))
          kafkaConfig.schemaRegistryURL match {
            case Some(registry) => // Write key value schemas to schema registry
              val valueRegistryManager = SchemaManagerFactory.create(valueRegistryConfig)
              val valueSchema = SchemaConverters.toAvroType(df.schema.fields(df.schema.fieldIndex("value")).dataType, nullable = false, recordName = "Value", nameSpace = kafkaConfig.kafkaTopic)
              valueRegistryManager.register(valueSchema)
            case None => // Nothing to do
          }
          serializedDF
        case _ => throw new IllegalArgumentException("Not supported as Kafka format!")
      }
    }
  }

  def writeToKafka(df: DataFrame, kafkaConfig: Kafka): Unit = {

    val kafkaDF = catalystToKafka(df, kafkaConfig)

    if (kafkaConfig.user.isEmpty) {
      kafkaDF
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        .option("topic", kafkaConfig.kafkaTopic)
        // compression is transparent to the consumer
        .option("kafka.compression.type", "gzip")
        // need larger batch size than the default 16 KB for compression to be efficient.
        .option("kafka.batch.size", "1048576")
        // set linger such that Kafka waits up to 1 second to fill the batch size.
        // this has a bit of effect on the latency but throughput seems more important.
        .option("kafka.linger.ms", "1000")
        // below options are put to these insane values in order to prevent
        // org.apache.kafka.common.errors.UnknownTopicOrPartitionException
        // when automatic topic creation is used in Kafka.
        .option("kafka.session.timeout.ms", "300000")
        .option("kafka.fetch.max.wait.ms", "300000")
        .option("kafka.request.timeout.ms", "800000")
        .option("kafka.max.request.size", "16000000")
        .option("kafka.retry.backoff.ms", "1000")
        .option("kafka.retries", "300")
        .option("kafka.max.in.flight.requests.per.connection", "1")
        // end of insane values.
        .save()
    } else {
      kafkaDF
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        .options(kafkaConfig.getCredentialProperties("kafka."))
        .option("topic", kafkaConfig.kafkaTopic)
        // compression is transparent to the consumer
        .option("kafka.compression.type", "gzip")
        // need larger batch size than the default 16 KB for compression to be efficient.
        .option("kafka.batch.size", "1048576")
        // set linger such that Kafka waits up to 1 second to fill the batch size.
        // this has a bit of effect on the latency but throughput seems more important.
        .option("kafka.linger.ms", "1000")
        // below options are put to these insane values in order to prevent
        // org.apache.kafka.common.errors.UnknownTopicOrPartitionException
        // when automatic topic creation is used in Kafka.
        .option("kafka.session.timeout.ms", "300000")
        .option("kafka.fetch.max.wait.ms", "300000")
        .option("kafka.request.timeout.ms", "800000")
        .option("kafka.max.request.size", "16000000")
        .option("kafka.retry.backoff.ms", "1000")
        .option("kafka.retries", "300")
        .option("kafka.max.in.flight.requests.per.connection", "1")
        // end of insane values.
        .save()
    }
  }

  def writeToKafkaStream(df: DataFrame, kafkaConfig: Kafka, kafkaTopic: String): DataStreamWriter[Row] = {

    val kafkaDF = catalystToKafka(df, kafkaConfig)

    if (kafkaConfig.user.isEmpty) {
      kafkaDF
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        .option("topic", kafkaTopic)
        // compression is transparent to the consumer
        .option("kafka.compression.type", "gzip")
        // need larger batch size than the default 16 KB for compression to be efficient.
        .option("kafka.batch.size", "1048576")
        // set linger such that Kafka waits up to 1 second to fill the batch size.
        // this has a bit of effect on the latency but throughput seems more important.
        .option("kafka.linger.ms", "1000")
        // below options are put to these insane values in order to prevent
        // org.apache.kafka.common.errors.UnknownTopicOrPartitionException
        // when automatic topic creation is used in Kafka.
        .option("kafka.session.timeout.ms", "300000")
        .option("kafka.fetch.max.wait.ms", "300000")
        .option("kafka.request.timeout.ms", "800000")
        .option("kafka.max.request.size", "16000000")
        .option("kafka.retry.backoff.ms", "1000")
        .option("kafka.retries", "300")
        .option("kafka.max.in.flight.requests.per.connection", "1")
    } else {
      kafkaDF
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.kafkaBrokers)
        .options(kafkaConfig.getCredentialProperties("kafka."))
        .option("topic", kafkaTopic)
        // compression is transparent to the consumer
        .option("kafka.compression.type", "gzip")
        // need larger batch size than the default 16 KB for compression to be efficient.
        .option("kafka.batch.size", "1048576")
        // set linger such that Kafka waits up to 1 second to fill the batch size.
        // this has a bit of effect on the latency but throughput seems more important.
        .option("kafka.linger.ms", "1000")
        // below options are put to these insane values in order to prevent
        // org.apache.kafka.common.errors.UnknownTopicOrPartitionException
        // when automatic topic creation is used in Kafka.
        .option("kafka.session.timeout.ms", "300000")
        .option("kafka.fetch.max.wait.ms", "300000")
        .option("kafka.request.timeout.ms", "800000")
        .option("kafka.max.request.size", "16000000")
        .option("kafka.retry.backoff.ms", "1000")
        .option("kafka.retries", "300")
        .option("kafka.max.in.flight.requests.per.connection", "1")
    }
  }

  def deleteTopic(config: Kafka, topicName: String): Unit = {
    val adminProps = new Properties
    // copy the settings from the producer properties.
    for (key <- config.getProducerProperties.keySet().asScala) {
      adminProps.put(key, config.getProducerProperties.get(key))
    }
    // configure some ridiculous timeouts.
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "800000")
    adminProps.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1000")
    adminProps.put(AdminClientConfig.RETRIES_CONFIG, "300")
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
