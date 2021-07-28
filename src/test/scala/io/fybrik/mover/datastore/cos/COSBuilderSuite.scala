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

import com.typesafe.config.ConfigFactory
import io.fybrik.mover.datastore.DataStoreBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
  * Tests for COS source building.
  */
class COSBuilderSuite extends AnyFunSuite with Matchers {
  test("if config parsing works for both 'cos' and 's3'") {
    val s =
      """
        |source {
        |  cos {
        |    endpoint = "eps"
        |    bucket = "bucket1"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |    accessKey = "ak"
        |    secretKey = "sk"
        |  }
        |}
        |destination {
        |  cos {
        |    endpoint = "ept"
        |    bucket = "bucket2"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |    access_key = "ak2"
        |    secret_key = "sk2"
        |  }
        |}""".stripMargin

    val config = ConfigFactory.parseString(s)
    val sourceDataStore = DataStoreBuilder.buildSource(config)
    val targetDataStore = DataStoreBuilder.buildTarget(config)
    sourceDataStore.get shouldBe a[COS]
    sourceDataStore.get.asInstanceOf[COS].bucket shouldBe "bucket1"
    val sourceAdditionalSparkConfig = sourceDataStore.get.additionalSparkConfig()
    sourceAdditionalSparkConfig.get("spark.hadoop.fs.cos.source.endpoint") shouldBe Some("eps")
    sourceAdditionalSparkConfig.get("spark.hadoop.fs.cos.source.access.key") shouldBe Some("ak")
    sourceAdditionalSparkConfig.get("spark.hadoop.fs.cos.source.secret.key") shouldBe Some("sk")
    targetDataStore.get shouldBe a[COS]
    targetDataStore.get.asInstanceOf[COS].bucket shouldBe "bucket2"
    val targetAdditionalSparkConfig = targetDataStore.get.additionalSparkConfig()
    targetAdditionalSparkConfig.get("spark.hadoop.fs.cos.target.endpoint") shouldBe Some("ept")
    targetAdditionalSparkConfig.get("spark.hadoop.fs.cos.target.access.key") shouldBe Some("ak2")
    targetAdditionalSparkConfig.get("spark.hadoop.fs.cos.target.secret.key") shouldBe Some("sk2")

    val s2 =
      """
        |source {
        |  s3 {
        |    endpoint = "eps"
        |    bucket = "bucket1"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}
        |destination {
        |  s3 {
        |    endpoint = "ept"
        |    bucket = "bucket2"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}""".stripMargin

    val config2 = ConfigFactory.parseString(s2)
    val sourceDataStore2 = DataStoreBuilder.buildSource(config2)
    val targetDataStore2 = DataStoreBuilder.buildTarget(config2)
    sourceDataStore2.get shouldBe a[COS]
    sourceDataStore2.get.asInstanceOf[COS].bucket shouldBe "bucket1"
    targetDataStore2.get shouldBe a[COS]
    targetDataStore2.get.asInstanceOf[COS].bucket shouldBe "bucket2"
  }
}
