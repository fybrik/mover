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
package com.ibm.m4d.mover

import com.google.common.base.Stopwatch
import com.google.gson.{JsonParser, JsonPrimitive}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.types.{DecimalType, Metadata, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}
import org.slf4j.LoggerFactory

/**
  * An object that contains many utility methods around Spark especially implicit classes
  * that use the "pimp-my-library" pattern to extend existing Spark classes with new methods.
  */
package object spark {
  lazy private val logger = LoggerFactory.getLogger("com.ibm.cedp.datahub.utils.spark")
  val COSSourceServiceName = "source"
  val COSTargetServiceName = "target"
  val Length = "length"

  /**
    * Retrieve the COS path
    * @param bucketName COS bucket name
    * @param objectName COS object name
    * @param serviceName COS service name as configured in hadoop configuration
    * @return
    */
  def cosUrl(bucketName: String, objectName: String, serviceName: String = COSSourceServiceName): String = {
    "cos://" + bucketName + "." + serviceName + "/" + objectName
  }

  implicit class SparkConfigImplicits(sparkConf: SparkConf) {
    def setAppNameIf(appName: String, predicate: => Boolean): SparkConf = {
      if (predicate) {
        sparkConf.setAppName(appName)
      } else {
        sparkConf
      }
    }

    def setMasterIf(master: String, predicate: => Boolean): SparkConf = {
      if (predicate) {
        sparkConf.setMaster(master)
      } else {
        sparkConf
      }
    }

    def setIf(key: String, value: String, predicate: => Boolean): SparkConf = {
      if (predicate) {
        sparkConf.set(key, value)
      } else {
        sparkConf
      }
    }

    def set(key: String, value: Option[String]): SparkConf = {
      value match {
        case Some(v) => sparkConf.set(key, v)
        case None    => sparkConf
      }
    }
  }

  implicit class DataFrameReaderImplicits(dataframeReader: DataFrameReader) {
    def option(key: String, value: Option[String]): DataFrameReader = {
      value match {
        case Some(v) => dataframeReader.option(key, v)
        case None    => dataframeReader
      }
    }
  }

  implicit class DataFrameWriterImplicits[T](dataFrameWriter: DataFrameWriter[T]) {
    def option(key: String, value: Option[String]): DataFrameWriter[T] = {
      value match {
        case Some(v) => dataFrameWriter.option(key, v)
        case None    => dataFrameWriter
      }
    }
  }

  implicit class DataStreamReaderImplicits(dataStreamReader: DataStreamReader) {
    def option(key: String, value: Option[String]): DataStreamReader = {
      value match {
        case Some(v) => dataStreamReader.option(key, v)
        case None    => dataStreamReader
      }
    }
  }

  implicit class DataStreamWriterImplicits[T](dataStreamWriter: DataStreamWriter[T]) {
    def option(key: String, value: Option[String]): DataStreamWriter[T] = {
      value match {
        case Some(v) => dataStreamWriter.option(key, v)
        case None    => dataStreamWriter
      }
    }
  }

  implicit class HadoopPathImplicits(path: Path) {
    def objectPath(): String = {
      if (path.isRoot) {
        ""
      } else {
        path.toUri.getRawPath.substring(1)
      }
    }

    def successObjectPath(): String = {
      new Path(path, "_SUCCESS").objectPath()
    }
  }

  implicit class StructTypeImplicits(structType: StructType) {
    def field(name: String): StructField = {
      structType.fields(structType.fieldIndex(name))
    }

    def fieldAsStructType(name: String): StructType = {
      val f = field(name)
      f.dataType.asInstanceOf[StructType]
    }
  }

  implicit class DFImplicits(df: DataFrame) {
    def withColumnTypeInfo(colName: String, typeInfo: String): DataFrame = {
      if (typeInfo == null) {
        df
      } else {
        val maybeStructField = df.schema.fields.find(_.name == colName)
        val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
        jsonObject.add(COLUMN_TYPE_INFO, new JsonPrimitive(typeInfo.toUpperCase()))

        df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
      }
    }

    def withLength(colName: String, length: Int): DataFrame = {
      val maybeStructField = df.schema.fields.find(_.name == colName)
      if (maybeStructField.isEmpty) {
        throw new IllegalArgumentException(s"Field '$colName' does not exist!")
      }
      val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
      jsonObject.add(LENGTH, new JsonPrimitive(length))

      df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
    }

    def withStringLength(colName: String, length: Int): DataFrame = {
      val maybeStructField = df.schema.fields.find(_.name == colName)
      if (maybeStructField.isEmpty) {
        throw new IllegalArgumentException(s"Field '$colName' does not exist!")
      }
      if (maybeStructField.get.dataType != StringType) {
        throw new IllegalArgumentException(s"Field '$colName' is not of type StringType!")
      }

      val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
      jsonObject.add(LENGTH, new JsonPrimitive(length))

      df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
    }

    def withTimestampScale(colName: String, scale: Int): DataFrame = {
      val maybeStructField = df.schema.fields.find(_.name == colName)
      if (maybeStructField.isEmpty) {
        throw new IllegalArgumentException(s"Field '$colName' does not exist!")
      }
      if (maybeStructField.get.dataType != TimestampType) {
        throw new IllegalArgumentException(s"Field '$colName' is not of type TimestampType!")
      }
      val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
      jsonObject.add(SCALE, new JsonPrimitive(scale))

      df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
    }

    def withDecimalColumn(colName: String, precision: Int, scale: Int): DataFrame = {
      if (precision > 31) {
        throw new IllegalArgumentException("Precision cannot be higher than 31 for BigSQL!")
      }
      if (scale > 31) {
        throw new IllegalArgumentException("Scale cannot be higher than 31 for BigSQL!")
      }
      df.withColumn(colName, df(colName).cast(new DecimalType(precision, scale)))
    }

    def withTimezoneInfo(colName: String, timezone: String): DataFrame = {
      if (timezone == null) {
        df
      } else {
        val maybeStructField = df.schema.fields.find(_.name == colName)
        val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
        jsonObject.add(COLUMN_TIMEZONE_INFO, new JsonPrimitive(timezone.toUpperCase()))

        df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
      }
    }

    def withNullableInfo(colName: String, nullable: Boolean): DataFrame = {
      val maybeStructField = df.schema.fields.find(_.name == colName)
      val jsonObject = new JsonParser().parse(maybeStructField.get.metadata.json).getAsJsonObject
      jsonObject.add(NULLABLE, new JsonPrimitive(nullable))

      df.withColumn(colName, col(colName).as(colName, Metadata.fromJson(jsonObject.toString)))
    }

    def stripMetadata(): DataFrame = {
      df.schema.fields.foldLeft(df) { (tdf, field) =>
        tdf.withColumn(field.name, col(field.name))
      }
    }

    private def nullField(field: StructField, name: String, f: (StructField) => StructField): StructField = {
      val searchName = if (name.contains(".")) {
        name.substring(0, name.indexOf("."))
      } else {
        name
      }
      if (field.name == searchName) {
        if (field.dataType.isInstanceOf[StructType]) {
          if (field.name == name) {
            f(field)
          } else {
            val subName = name.substring(name.indexOf(".") + 1)
            val newFields = field.dataType.asInstanceOf[StructType].fields.map(field => nullField(field, subName, f))
            StructField(field.name, StructType(newFields), field.nullable, field.metadata)
          }
        } else {
          f(field)
        }
      } else {
        field
      }
    }

    /**
      * Set nullable property of column.
      * @param colName is the column name to change
      * @param nullable is the flag to set, such that the column is  either nullable or not
      */
    def setNullableStateOfColumn(colName: String, nullable: Boolean): DataFrame = {
      // get schema
      val schema = df.schema
      // modify [[StructField] with name `cn`
      val newSchema = StructType(schema.fields.map { field =>
        nullField(field, colName, (f: StructField) => f.copy(nullable = nullable))
      })
      // apply new schema
      df.sqlContext.createDataFrame(df.rdd, newSchema)
    }
  }

  /**
    * Time an action while it's executed.
    * @param f method to time
    * @tparam A return parameter of method
    * @return
    */
  def time[A](f: => A): A = {
    val stopwatch = Stopwatch.createStarted()
    val result = f
    stopwatch.stop()
    logger.info("Execution time: " + stopwatch.toString())
    result
  }

  /**
    * Cease execution temporarliy for debugging purposes. (e.g. to inspect the Spark UI)
    *
    * @param debug is debugging active
    * @param millisToSleep milli seconds to sleep
    */
  def sleepForDebugging(debug: Boolean, millisToSleep: Long = 1 * 60 * 1000L): Unit = {
    if (debug) {
      logger.info("Sleeping in order to give time for debugging...")
      Thread.sleep(millisToSleep)
    }
  }

  val COLUMN_REMARK = "columnRemark"
  val COLUMN_TYPE_INFO = "columnTypeInfo"
  val COLUMN_TIMEZONE_INFO = "timezone"
  val LENGTH = "length"
  val SCALE = "scale"
  val NULLABLE = "nullable"
}
