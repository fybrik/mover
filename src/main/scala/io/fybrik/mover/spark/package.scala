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
package io.fybrik.mover

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
}
