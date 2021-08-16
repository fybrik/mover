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

import java.io.File
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigUtil}
import io.fybrik.mover.datastore.cos.{COS, COSBuilder}
import io.fybrik.mover.datastore.local.{Local, LocalBuilder}
import io.fybrik.mover.spark._
import org.apache.commons.io.FileUtils
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
  * This test class is meant for debugging. It's an integration test of the [[SparkTransfer]] program.
  */
class AppTest extends ExtendedFunSuite with Matchers with SparkTest {
  testIfExists("run cos-to-cos")("src/main/resources/cos-to-cos.conf") {
    System.setProperty("IS_TEST", "true")
    Transfer.main(Array("src/main/resources/cos-to-cos.conf"))

    val conf = ConfigFactory.parseFile(new File("src/main/resources/cos-to-cos.conf"))
    val cos = COSBuilder.buildTarget(conf).get.asInstanceOf[COS]
    withSparkSessionExtra(cos.additionalSparkConfig()) { spark =>
      val df = spark.read.parquet(cosUrl(cos.bucket, cos.objectKey, COSSourceServiceName))

      // The original plants.parq file has 5 columns
      // The transferred one with the column filter should have 4
      df.schema.fieldNames should have size 4
    }
  }

  ignoreIf("run kafka-to-cos")(fileExists("src/main/resources/kafka-to-cos.conf") && portListening(9092)) {
    System.setProperty("IS_TEST", "true")
    Transfer.main(Array("src/main/resources/kafka-to-cos.conf"))

    val conf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-cos.conf"))
    val cos = COSBuilder.buildTarget(conf).get.asInstanceOf[COS]
    withSparkSessionExtra(cos.additionalSparkConfig()) { spark =>
      val df = spark.read.parquet(cosUrl(cos.bucket, cos.objectKey, COSSourceServiceName))

      // The original plants.parq file has 5 columns
      // The transferred one with the column filter should have 4
      df.schema.fieldNames should have size 4
    }
  }

  test("run local-to-local") {
    System.setProperty("IS_TEST", "true")
    Transfer.main(Array("src/main/resources/local-to-local.conf"))

    FileUtils.forceDeleteOnExit(new File("test.parq"))

    val conf = ConfigFactory.parseFile(new File("src/main/resources/local-to-local.conf"))
    val cos = new LocalBuilder().buildTarget(conf).get.asInstanceOf[Local]
    withSparkSession { spark =>
      val df = spark.read.parquet(cos.path)

      df.schema.fieldNames should have size 2
      val rows = df.collect().map(r => (r.getString(0), r.getDouble(1)))
      rows should contain theSameElementsAs Seq(
        ("XXXXXXXXXX", 1.0),
        ("XXXXXXXXXX", 2.0),
        ("XXXXXXXXXX", 3.0)
      )
    }
    Files.exists(new File("test.parq").toPath) shouldBe true
    Finalizer.main(Array("src/main/resources/local-to-local.conf"))
    Files.exists(new File("test.parq").toPath) shouldBe false
  }

  test("run misconfigured finalizer") {
    System.setProperty("IS_TEST", "true")
    intercept[IllegalArgumentException](Finalizer.main(Array()))
    val changedConfig = ConfigFactory.parseFile(new File("src/main/resources/local-to-local.conf"))
      .withoutPath("destination.local")
    val tempConf = Files.createTempFile("test", ".json")
    Files.write(tempConf, changedConfig.root().render(ConfigRenderOptions.concise()).getBytes())
    intercept[IllegalArgumentException](Finalizer.main(Array(tempConf.toString)))
    FileUtils.forceDeleteOnExit(tempConf.toFile)
  }
}
