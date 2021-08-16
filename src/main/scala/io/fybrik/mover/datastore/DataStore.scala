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
package io.fybrik.mover.datastore

import io.fybrik.mover.{DataFlowType, DataType, MetaData, WriteOperation}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

/**
  * A data store is the source and target component of the mover. It knows how to read data from
  * a specific data store as a Spark [[DataFrame]] and how to write a [[DataFrame]] back to that
  * data store.
  */

abstract class DataStore(iType: InputType) {
  /**
    * Extra configuration options that are needed to configure the [[org.apache.spark.SparkConf]].
    * @return
    */
  def additionalSparkConfig(): Map[String, String]

  /**
    * How to read meta data from a given store of a data set. (Currently not used)
    * @return
    */
  def sourceMetadata(): Option[MetaData]

  /**
    * Read a data frame from this data store.
    * Given the data flow type
    *
    * @param spark
    * @param dataFlowType
    * @param dataType
    * @return
    */
  def read(spark: SparkSession, dataFlowType: DataFlowType, dataType: DataType): DataFrame

  /**
    * Write the given data frame out as a one time batch.
    * This can happen in multiple different modes depending on what the data store supports.
    * If an operation is not supported an exception is thrown.
    *
    * @param df data frame to write
    * @param targetDataType target data type
    * @param writeOperation write operation that should be performed
    */
  def write(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): Unit

  /**
    * Write the given data frame out as stream.
    * (May not be supported by all data stores)
    * If an operation is not supported an exception is thrown.
    *
    * @param df data frame to write
    * @param targetDataType target data type
    * @param writeOperation write operation that should be performed
    * @return
    */
  def writeStream(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): DataStreamWriter[Row]

  /**
    * Delete the data that was created at the target.
    * CAUTION: For Append operations this could mean removing the whole data set!
    */
  def deleteTarget()
}
