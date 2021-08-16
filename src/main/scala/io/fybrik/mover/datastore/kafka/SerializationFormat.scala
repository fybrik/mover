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
package io.fybrik.mover.datastore.kafka

/**
  * Serialization format that is used for the serialization of data within Kafka.
  * For now this only implements JSON and Avro data.
  * TODO maybe move the specific serialization code into the JSON and Avro classes below instead of having it in [[KafkaUtils]].
  */
trait SerializationFormat

object SerializationFormat {
  case object JSON extends SerializationFormat
  case object Avro extends SerializationFormat

  def parse(s: String): SerializationFormat = {
    s.toLowerCase() match {
      case "json" => JSON
      case "avro" => Avro
    }
  }
}
