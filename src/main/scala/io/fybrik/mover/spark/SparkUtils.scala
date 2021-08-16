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
package io.fybrik.mover.spark

import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * This utility method creates a [[SparkSession]] and includes some other utility methods.
  * TODO Merge this with the spark package object
  */
object SparkUtils {
  private val logger = LoggerFactory.getLogger(SparkUtils.getClass)

  /**
    * Create a [[SparkSession]].
    *
    * @param appName application name to configure
    * @param debug if debug should be enabled
    * @param local if a Spark local session should be enforced
    * @return
    */
  def sparkSession(
      appName: String,
      debug: Boolean = false,
      local: Boolean = false,
      sparkConfig: SparkConfig = SparkConfig.default,
      additionalOptions: Map[String, String] = Map.empty[String, String]
  ): SparkSession = {
    logger.info("Spark config: " + sparkConfig)
    logger.info("Available processors: " + Runtime.getRuntime.availableProcessors())
    val defaultConf = new SparkConf()
    val numThreads = sparkConfig.driverCores
    val numInstances = sparkConfig.numExecutors
    val sparkMaster = if (local || numInstances == 0) {
      numThreads.map(s => s"local[$s]").getOrElse("local")
    } else {
      defaultConf.get("spark.master")
    }

    logger.info(s"Spark master: " + sparkMaster)
    logger.info(s"Debug enabled: " + debug)

    val conf = new SparkConf()
      .setAppNameIf(appName, !defaultConf.contains("spark.app.name"))
      .setMaster(sparkMaster)
      .set("spark.eventLog.enabled", "false")
      .set("spark.ui.enabled", debug.toString)
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.parquet.writeLegacyFormat", "true")
      .set("spark.sql.shuffle.partitions", sparkConfig.shufflePartitions.toString)
      .setAll(additionalOptions)
      .setIf("spark.driver.bindAddress", "localhost", sys.props.get("IS_TEST").contains("true"))
      .setIf("spark.ui.enabled", "false", sys.props.get("IS_TEST").contains("true"))
      .setIf("spark.executor.instances", numInstances.toString, numInstances > 0)
      .setIf("spark.executor.memory", sparkConfig.executorMemory, numInstances > 0)
      .setIf("spark.executor.cores", sparkConfig.executorCores.toString, numInstances > 0)
      .set("spark.kubernetes.container.image", sparkConfig.image)
      .set("spark.kubernetes.container.image.pullPolicy", sparkConfig.imagePullPolicy)
      .set("spark.sql.streaming.checkpointLocation", "/tmp/datamover")
      .set("spark.sql.streaming.schemaInference", "true")
      .setAll(sparkConfig.additionalOptions)

    SparkSession.builder
      .config(conf)
      .getOrCreate()
  }

  /**
    * Maps a dataframe field to an Avro schema.
    * @param field A dataframe field
    * @return An Avro schema.
    */
  private def fieldToAvro(field: StructField): Schema = {
    field.dataType match {
      case BooleanType => {
        val s = Schema.create(Schema.Type.BOOLEAN)
        val vc = new LogicalType("BOOLEAN").addToSchema(s)
        s
      }
      case ByteType => {
        val s = Schema.create(Schema.Type.INT)
        val vc = new LogicalType("TINYINT").addToSchema(s)
        s
      }
      case ShortType => {
        val s = Schema.create(Schema.Type.INT)
        val vc = new LogicalType("SMALLINT").addToSchema(s)
        s
      }
      case IntegerType => Schema.create(Schema.Type.INT)
      case LongType    => Schema.create(Schema.Type.LONG)
      case FloatType   => Schema.create(Schema.Type.FLOAT)
      case DoubleType  => Schema.create(Schema.Type.DOUBLE)
      case DecimalType() => {
        val precision = field.dataType.asInstanceOf[DecimalType].precision
        val scale = field.dataType.asInstanceOf[DecimalType].scale
        val s = Schema.create(Schema.Type.BYTES)
        val vc = LogicalTypes.decimal(precision, scale).addToSchema(s)
        s
      }
      case StringType => Schema.create(Schema.Type.STRING)
      case BinaryType => Schema.create(Schema.Type.BYTES)
      case TimestampType => {
        val s = Schema.create(Schema.Type.LONG)
        val vc = LogicalTypes.timeMicros().addToSchema(s)
        s
      }
      case DateType => {
        val s = Schema.create(Schema.Type.INT)
        val vc = LogicalTypes.date().addToSchema(s)
        s
      }
    }
  }

  /**
    * Returns an Avro schema for the given column field in the given data frame.
    * If the column contains meta-data object, it is assume that this meta-data contains
    * a valid Avro schema definition that is then returned.
    * Otherwise, the dataframe's schema definition is mapped to an Avro schema.
    * @param field The dataframe field
    * @return An Avro schema that describes the column.
    */
  def getAvroFromColumn(field: StructField): Schema = {
    val metaData = field.metadata
    val tmpSchema = Schema.parseJsonToObject(metaData.json)

    if (!tmpSchema.isInstanceOf[org.apache.avro.Schema]) {
      fieldToAvro(field)
    } else {
      tmpSchema.asInstanceOf[org.apache.avro.Schema]
    }
  }

  /**
    * Takes a dataframe and augments the meta-data of all columns with the
    * avro schema specification that is derived from the dataframe's schema.
    * This is the minimal baseline for technical meta-data.
    * @param df A dataframe to be augmented with meta-data
    * @return A dataframe augmented with meta-data
    */
  def augmentMetadata(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) {
      (tmpDf, columnName) =>
        {
          val field = df.schema.fields(df.schema.fieldIndex(columnName))
          // populate meta-data if not already present.
          if (field.metadata.json.equals("{}")) {
            val avroSchema = fieldToAvro(field)
            // primitive types are represented as, for example "int"
            // rather than the longer form {"type" : "int" }.
            // Spark meta-data requires the longer form.
            val avroJson = avroSchema.toString
            val fullJson = if (avroJson.contains("type"))
              avroJson
            else {
              "{\"type\" : " + avroJson + "}"
            }
            val meta = Metadata.fromJson(fullJson)
            tmpDf.withColumn(columnName, col(columnName).as(columnName, meta))
          } else {
            tmpDf
          }
        }
    }
  }

  def getAvroSchema(df: DataFrame, dfName: String): Schema = {
    val fieldList = df.schema.fields.map(field => {
      val meta = field.metadata
      val fSchema = Try(new Schema.Parser().parse(meta.json))
      if (fSchema.isSuccess) {
        if (field.nullable) {
          val nullSchema = Schema.create(Schema.Type.NULL)
          val unionSchema = Schema.createUnion(fSchema.get, nullSchema)
          new Schema.Field(field.name, unionSchema)
        } else {
          new Schema.Field(field.name, fSchema.get)
        }
      } else {
        new Schema.Field(field.name, Schema.create(Schema.Type.NULL))
      }
    }).toList.asJava

    Schema.createRecord(dfName, "", "", false, fieldList)
  }

}
