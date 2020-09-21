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
package com.ibm.datamesh.mover.datastore.cos

import com.ibm.datamesh.mover.ExtendedFunSuite
import com.ibm.datamesh.mover.datastore.DataStoreBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers

/**
  * Tests for COS source building.
  */
class COSBuilderSuite extends ExtendedFunSuite with Matchers {
  test("if config parsing works for both 'cos' and 's3'") {
    val s =
      """
        |source {
        |  cos {
        |    endpoint = "eps"
        |    bucket = "bucket"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}
        |destination {
        |  cos {
        |    endpoint = "ept"
        |    bucket = "bucket"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}""".stripMargin

    val config = ConfigFactory.parseString(s)
    val sourceDataStore = DataStoreBuilder.buildSource(config)
    val targetDataStore = DataStoreBuilder.buildTarget(config)
    sourceDataStore.get shouldBe a[COS]
    sourceDataStore.get.asInstanceOf[COS].endpoint shouldBe "eps"
    targetDataStore.get shouldBe a[COS]
    targetDataStore.get.asInstanceOf[COS].endpoint shouldBe "ept"

    val s2 =
      """
        |source {
        |  s3 {
        |    endpoint = "eps"
        |    bucket = "bucket"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}
        |destination {
        |  s3 {
        |    endpoint = "ept"
        |    bucket = "bucket"
        |    objectKey = "ok"
        |    dataFormat = "parquet"
        |  }
        |}""".stripMargin

    val config2 = ConfigFactory.parseString(s)
    val sourceDataStore2 = DataStoreBuilder.buildSource(config)
    val targetDataStore2 = DataStoreBuilder.buildTarget(config)
    sourceDataStore2.get shouldBe a[COS]
    sourceDataStore2.get.asInstanceOf[COS].endpoint shouldBe "eps"
    targetDataStore2.get shouldBe a[COS]
    targetDataStore2.get.asInstanceOf[COS].endpoint shouldBe "ept"
  }
}
