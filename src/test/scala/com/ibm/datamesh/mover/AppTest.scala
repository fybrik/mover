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
package com.ibm.datamesh.mover

import java.io.File

import com.ibm.datamesh.mover.datastore.cos.{COS, COSBuilder}
import com.ibm.datamesh.mover.spark.{SparkTest, _}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers

/**
  * This test class is meant for debugging. It's an integration test of the [[SparkTransfer]] program.
  */
class AppTest extends ExtendedFunSuite with Matchers with SparkTest {
  testIfExists("run cos-to-cos")("src/main/resources/cos-to-cos.conf") {
    Transfer.main(Array("src/main/resources/cos-to-cos.conf"))

    val conf = ConfigFactory.parseFile(new File("src/main/resources/cos-to-cos.conf"))
    val cos = COSBuilder.buildTarget(conf).get.asInstanceOf[COS]
    withSparkSessionCOS(cos) { spark =>
      val df = spark.read.parquet(cosUrl(cos.bucket, cos.objectKey, COSSourceServiceName))

      // The original plants.parq file has 5 columns
      // The transferred one with the column filter should have 4
      df.schema.fieldNames should have size 4
    }
  }

  ignoreIf("run kafka-to-cos")(fileExists("src/main/resources/kafka-to-cos.conf") && portListening(9092)) {
    Transfer.main(Array("src/main/resources/kafka-to-cos.conf"))

    val conf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-cos.conf"))
    val cos = COSBuilder.buildTarget(conf).get.asInstanceOf[COS]
    withSparkSessionCOS(cos) { spark =>
      val df = spark.read.parquet(cosUrl(cos.bucket, cos.objectKey, COSSourceServiceName))

      // The original plants.parq file has 5 columns
      // The transferred one with the column filter should have 4
      df.schema.fieldNames should have size 4
    }
  }
}
