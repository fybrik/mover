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
  * Write operation that should be performed.
  * Depending on the data flow type and the chosen source/target data store only certain
  * write operations may be allowed. The data store should determine if it supports the specified
  * write operation or not.
  */

trait WriteOperation
object WriteOperation {

  /**
    * Overwrites the data that may already be present at the target.
    * E.g. overwrite objects that are at the given object prefix in an object store.
    */
  case object Overwrite extends WriteOperation

  /**
    * Appends to the data that is already present at a target.
    * E.g. append new objects to a given object prefix in an object store.
    */
  case object Append extends WriteOperation

  /**
    * Update the data that is already present in a target.
    * This mode can mostly be used for change data and data stores that can interpret
    * this change data and offer INSERT/UPDATE/DELETE operations such as a relational database.
    */
  case object Update extends WriteOperation

  def parse(s: String): WriteOperation = {
    s.toLowerCase() match {
      case "overwrite" => Overwrite
      case "append"    => Append
      case "update"    => Update
      case _           => throw new IllegalArgumentException("Unknown write operation '" + s + "'. Expected values are 'overwrite', 'append' or 'update'.")
    }
  }
}
