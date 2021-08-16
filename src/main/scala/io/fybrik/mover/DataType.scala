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

import io.fybrik.mover.datastore.DataStore

/**
  * The data type describes what kind of data is read from a given [[DataStore]].
  */
trait DataType
object DataType {

  /**
    * LogData is plain data in form of columns.
    * E.g. in Spark it could be a data frame with columns a, b, c
    * In Kafka this is the pure KStream where the key part is null and not used
    * and the value part of kafka consists of the columns a, b, c.
    */
  case object LogData extends DataType

  /**
    * ChangeData is a form of a key-value data set that describes how the value of a key is changing
    * over time. In a database the key is normally a primary key and the value is the whole row.
    * In Kafka this refers to the notion of a KTable that has a value for the key field (e.g. a)
    * and the value stores the actual changes (e.g. a1, b1, c2)
    * In a Spark [[org.apache.spark.sql.DataFrame]] change data has a 'key' and a 'value' struct that contain
    * the actual data inside.
    */
  case object ChangeData extends DataType

  def parse(s: String): DataType = {
    s.toLowerCase() match {
      case "logdata" | "log"               => LogData
      case "changedata" | "change" | "cdc" => ChangeData
      case _                               => throw new IllegalArgumentException("Unknown data type '" + s + "'. Expected values are 'logdata' or 'changedata'.")
    }
  }
}
