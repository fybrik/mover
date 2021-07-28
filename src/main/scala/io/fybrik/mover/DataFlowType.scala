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

/**
  * Describes whether the data flow movement is of batch or streaming nature.
  * In this implementation it means whether to use Spark in batch or in streaming mode.
  */
trait DataFlowType
object DataFlowType {
  case object Batch extends DataFlowType
  case object Stream extends DataFlowType
  def parse(s: String): DataFlowType = {
    s.toLowerCase() match {
      case "batch"  => Batch
      case "stream" => Stream
      case _        => throw new IllegalArgumentException("Unknown dataflow type '" + s + "'. Expected values are 'batch' or 'stream'.")
    }
  }
}
