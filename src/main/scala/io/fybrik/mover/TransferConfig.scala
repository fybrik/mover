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

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger

/**
  * Configuration class for the [[Transfer]] program.
  */
class TransferConfig(
    val dataFlowType: DataFlowType,
    val sourceDataType: DataType,
    val targetDataType: DataType,
    val writeOperation: WriteOperation,
    val trigger: Trigger
) {

}

object TransferConfig {
  def apply(config: Config): TransferConfig = {
    val dataFlowType = if (config.hasPath("flowType")) {
      DataFlowType.parse(config.getString("flowType"))
    } else {
      DataFlowType.Batch
    }

    val sourceDataType = if (config.hasPath("readDataType")) {
      DataType.parse(config.getString("readDataType"))
    } else {
      DataType.LogData
    }

    val targetDataType = if (config.hasPath("writeDataType")) {
      DataType.parse(config.getString("writeDataType"))
    } else {
      sourceDataType
    }

    val writeOperation = if (config.hasPath("writeOperation")) {
      WriteOperation.parse(config.getString("writeOperation"))
    } else {
      dataFlowType match {
        case DataFlowType.Batch  => WriteOperation.Overwrite
        case DataFlowType.Stream => WriteOperation.Append
      }
    }

    val trigger = if (config.hasPath("triggerInterval")) {
      val v = config.getString("triggerInterval")
      if (v.equals("once")) {
        Trigger.Once()
      } else {
        Trigger.ProcessingTime(config.getString("triggerInterval"))
      }
    } else {
      Trigger.ProcessingTime("5 seconds")
    }

    new TransferConfig(dataFlowType, sourceDataType, targetDataType, writeOperation, trigger)
  }
}
