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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Test scenarios for default values of [[TransferConfig]].
  */
class TransferConfigSuite extends AnyFlatSpec with Matchers {
  it should "parse empty config correctly" in {
    val config = ConfigFactory.empty()
    val tc = TransferConfig.apply(config)
    tc.dataFlowType shouldBe DataFlowType.Batch
    tc.sourceDataType shouldBe DataType.LogData
    tc.targetDataType shouldBe DataType.LogData
    tc.writeOperation shouldBe WriteOperation.Overwrite
    tc.trigger shouldBe Trigger.ProcessingTime("5 seconds")
  }

  it should "parse stream config correctly" in {
    val config = ConfigFactory.empty()
      .withValue("flowType", ConfigValueFactory.fromAnyRef("stream"))
    val tc = TransferConfig.apply(config)
    tc.dataFlowType shouldBe DataFlowType.Stream
    tc.sourceDataType shouldBe DataType.LogData
    tc.targetDataType shouldBe DataType.LogData
    tc.writeOperation shouldBe WriteOperation.Append
    tc.trigger shouldBe Trigger.ProcessingTime("5 seconds")
  }

  it should "parse trigger config correctly" in {
    val config = ConfigFactory.empty()
      .withValue("flowType", ConfigValueFactory.fromAnyRef("stream"))
      .withValue("triggerInterval", ConfigValueFactory.fromAnyRef("once"))
    val tc = TransferConfig.apply(config)
    tc.trigger shouldBe Trigger.Once()

    val config2 = ConfigFactory.empty()
      .withValue("flowType", ConfigValueFactory.fromAnyRef("stream"))
      .withValue("triggerInterval", ConfigValueFactory.fromAnyRef("2 seconds"))
    val tc2 = TransferConfig.apply(config2)
    tc2.trigger shouldBe Trigger.ProcessingTime("2 seconds")
  }
}
