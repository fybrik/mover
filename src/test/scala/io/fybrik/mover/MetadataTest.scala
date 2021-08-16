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

import io.fybrik.mover.spark.{SparkTest, SparkUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
  * Tests meta data augmentations.
  */
class MetadataTest extends AnyFunSuite with Matchers with SparkTest {

  test("Dataframe is augmented with meta-data") {

    withSparkSession { spark =>
      val schema = StructType(List(
        StructField("aByte", ByteType, true),
        StructField("aShort", ShortType, true),
        StructField("anInt", IntegerType, true),
        StructField("aLong", LongType, true),
        StructField("aFloat", FloatType, true),
        StructField("aDouble", DoubleType, true),
        StructField("aDec", DecimalType(5, 3), true),
        StructField("aDec2", DecimalType(15, 6), true),
        StructField("aString", StringType, true),
        StructField("aBinary", BinaryType, true),
        StructField("aTimestamp", TimestampType, true),
        StructField("aDate", DateType, true),

        StructField("aByteNotNull", ByteType, false),
        StructField("aShortNotNull", ShortType, false),
        StructField("anIntNotNull", IntegerType, false),
        StructField("aLongNotNull", LongType, false),
        StructField("aFloatNotNull", FloatType, false),
        StructField("aDoubleNotNull", DoubleType, false),
        StructField("aDecNotNull", DecimalType(5, 3), false),
        StructField("aDec2NotNull", DecimalType(15, 6), false),
        StructField("aStringNotNull", StringType, false),
        StructField("aBinaryNotNull", BinaryType, false),
        StructField("aTimestampNotNull", TimestampType, false),
        StructField("aDateNotNull", DateType, false),
      ))

      val data = List(
        Row(
          1.toByte, 2.toShort, 3.toInt, 4.toLong, 5.5.toFloat, 6.6.toDouble,
          Decimal(7.7), Decimal(1234.1234), "Hello World", "binary",
          new java.sql.Timestamp(123456), new java.sql.Date(123456),
          1.toByte, 2.toShort, 3.toInt, 4.toLong, 5.5.toFloat, 6.6.toDouble,
          Decimal(7.7), Decimal(1234.1234), "Hello World", "binary",
          new java.sql.Timestamp(123456), new java.sql.Date(123456)
        )
      ).asJava

      val df = spark.createDataFrame(data, schema)

      val augmented = SparkUtils.augmentMetadata(df)

      augmented.printSchema()

      augmented.schema.fields.foreach(field => {
        val meta = field.metadata
        println(meta.json)
      })
    }
  }
}
