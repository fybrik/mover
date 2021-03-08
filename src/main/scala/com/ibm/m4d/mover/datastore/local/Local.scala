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
package com.ibm.m4d.mover.datastore.local

import com.ibm.m4d.mover.datastore.cos.FileFormat
import com.ibm.m4d.mover.datastore.{DataStore, InputType}
import com.ibm.m4d.mover.{DataFlowType, DataType, MetaData, WriteOperation}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File

/**
  * [[DataStore]] that uses the local file system. This data store is mostly used for testing.
  */
class Local(
    val iType: InputType,
    val path: String,
    val fileFormat: FileFormat,
    val partitionBy: Seq[String] = Seq.empty[String]
) extends DataStore(iType) {
  override def additionalSparkConfig(): Map[String, String] = Map.empty[String, String]

  override def sourceMetadata(): Option[MetaData] = ???

  override def read(spark: SparkSession, dataFlowType: DataFlowType, dataType: DataType): DataFrame = {
    dataFlowType match {
      case DataFlowType.Batch =>
        fileFormat.read(spark, path)
      case DataFlowType.Stream =>
        fileFormat.readStream(spark, path)
    }
  }

  override def write(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): Unit = {
    fileFormat.write(df, path, writeOperation, partitionBy)
  }

  override def writeStream(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): DataStreamWriter[Row] = {
    fileFormat.writeStream(df, path, writeOperation, partitionBy)
  }

  override def deleteTarget(): Unit = {
    FileUtils.deleteDirectory(new File(path))
  }
}
