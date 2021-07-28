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
package io.fybrik.mover.datastore.cos

import io.fybrik.mover.WriteOperation
import io.fybrik.mover.WriteOperation.{Append, Overwrite, Update}
import io.fybrik.mover.spark.SparkTest
import io.fybrik.mover.transformation.MyClass
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.nio.file.Files

/**
  * This suite tests the [[FileFormat]] classes on their read/write functionallity.
  */
class FileFormatSuite extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks with SparkTest {
  val data = Seq(
    MyClass(1, "a", 1.0),
    MyClass(2, "b", 2.0),
    MyClass(3, "c", 3.0),
  )

  val table = Table(
    "format",
    "parquet",
    "orc",
    "csv",
    "json"
  )

  it should "write and read files in different formats" in {
    withSparkSession { spark =>
      val originDF = spark.createDataFrame(data)
      forAll(table) { formatStr =>
        import spark.implicits._
        val path = Files.createTempDirectory(formatStr)
        val format = FileFormat.parse(formatStr)

        format.write(originDF, path.toAbsolutePath.toString, Overwrite, Seq.empty[String])
        format.write(originDF, path.toAbsolutePath.toString, Append, Seq.empty[String])
        intercept[IllegalArgumentException](format.write(originDF, path.toAbsolutePath.toString, Update, Seq.empty[String]))

        val read = format.read(spark, path.toAbsolutePath.toString)
        val readData = read.withColumn("i", read("i").cast(StringType).cast(IntegerType)).as[MyClass].collect()

        readData should contain theSameElementsAs (data ++ data)
        FileUtils.deleteDirectory(path.toFile)
      }
    }
  }

  it should "write and read files in different formats as stream" in {
    withSparkSession { spark =>
      val originDF = spark.createDataFrame(data)
      forAll(table) { formatStr =>
        import spark.implicits._
        val path = Files.createTempDirectory(formatStr)
        val path2 = Files.createTempDirectory(formatStr)
        val format = FileFormat.parse(formatStr)

        format.write(originDF, path.toAbsolutePath.toString, WriteOperation.Overwrite, Seq.empty[String])

        val intermediateDF = format.readStream(spark, path.toAbsolutePath.toString)
        intercept[IllegalArgumentException](format.writeStream(originDF, path.toAbsolutePath.toString, WriteOperation.Update, Seq.empty[String]))
        intercept[IllegalArgumentException](format.writeStream(originDF, path.toAbsolutePath.toString, WriteOperation.Overwrite, Seq.empty[String]))
        val exec = format.writeStream(intermediateDF, path2.toAbsolutePath.toString, WriteOperation.Append, Seq.empty[String])
          .trigger(Trigger.Once())
          .start()
        exec.awaitTermination()

        val read = format.read(spark, path2.toAbsolutePath.toString)
        val readData = read.withColumn("i", read("i").cast(StringType).cast(IntegerType)).as[MyClass].collect()

        readData should contain theSameElementsAs (data)
        FileUtils.deleteDirectory(path.toFile)
        FileUtils.deleteDirectory(path2.toFile)
      }
    }
  }

  it should "write and read partitioned data" in {
    withSparkSession { spark =>
      val originDF = spark.createDataFrame(data)
      val formatStr = "parquet"
      import spark.implicits._
      val path = Files.createTempDirectory(formatStr)
      val format = FileFormat.parse(formatStr)

      format.write(originDF, path.toAbsolutePath.toString, WriteOperation.Overwrite, Seq("i"))

      val read = format.read(spark, path.toAbsolutePath.toString)
      val readData = read.withColumn("i", read("i").cast(StringType).cast(IntegerType)).as[MyClass].collect()

      readData should contain theSameElementsAs (data)
    }
  }
}
