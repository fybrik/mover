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

import io.fybrik.mover.WriteOperation
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

/**
  * This trait describes file formats that are supported by the mover. It's meant to be extendable to multiple
  * different file formats by implementing this trait. Currently this trait is only used for the [[COS]] implementation
  * but is not limited to it as other storage systems that store files could also use it.
  * It distinguishes between reading/writing in the different batch/streaming modes.
  *
  * Implementations of file formats can decide which [[WriteOperation]] is supported.
  *
  * Other implementations of the [[FileFormat]] trait can be loaded by specifying the class name as a dataFormat.
  */
trait FileFormat {
  def read(spark: SparkSession, path: String): DataFrame
  def readStream(spark: SparkSession, path: String): DataFrame
  def write(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): Unit
  def writeStream(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): DataStreamWriter[Row]

  def setPartitions(dataFrameWriter: DataFrameWriter[Row], partitionBy: Seq[String]): DataFrameWriter[Row] = {
    if (partitionBy.isEmpty) {
      dataFrameWriter
    } else {
      dataFrameWriter.partitionBy(partitionBy: _*)
    }
  }

  def setPartitionsStream(dataStreamWriter: DataStreamWriter[Row], partitionBy: Seq[String]): DataStreamWriter[Row] = {
    if (partitionBy.isEmpty) {
      dataStreamWriter
    } else {
      dataStreamWriter.partitionBy(partitionBy: _*)
    }
  }
}

object FileFormat {
  case object Parquet extends FileFormat {
    override def read(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)

    override def readStream(spark: SparkSession, path: String): DataFrame = spark.readStream.parquet(path)

    override def write(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): Unit = {
      val saveMode = writeOperation match {
        case WriteOperation.Append    => SaveMode.Append
        case WriteOperation.Overwrite => SaveMode.Overwrite
        case WriteOperation.Update    => throw new IllegalArgumentException("Update operation not supported for batch!")
      }
      setPartitions(df.write, partitionBy).mode(saveMode).parquet(path)
    }

    override def writeStream(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): DataStreamWriter[Row] = {
      val outputMode = writeOperation match {
        case WriteOperation.Append    => OutputMode.Append()
        case WriteOperation.Update    => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream into parquet!")
        case WriteOperation.Overwrite => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream!")
      }
      setPartitionsStream(df.writeStream, partitionBy)
        .outputMode(outputMode)
        .format("parquet")
        .option("path", path)
    }
  }

  case object JSON extends FileFormat {
    override def read(spark: SparkSession, path: String): DataFrame = spark.read.json(path)

    override def readStream(spark: SparkSession, path: String): DataFrame = spark.readStream.json(path)

    override def write(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): Unit = {
      val saveMode = writeOperation match {
        case WriteOperation.Append    => SaveMode.Append
        case WriteOperation.Overwrite => SaveMode.Overwrite
        case WriteOperation.Update    => throw new IllegalArgumentException("Update operation not supported for batch!")
      }
      setPartitions(df.write, partitionBy).mode(saveMode).json(path)
    }

    override def writeStream(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): DataStreamWriter[Row] = {
      val outputMode = writeOperation match {
        case WriteOperation.Append    => OutputMode.Append()
        case WriteOperation.Update    => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream into JSON!")
        case WriteOperation.Overwrite => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream!")
      }
      setPartitionsStream(df.writeStream, partitionBy)
        .outputMode(outputMode)
        .format("json")
        .option("path", path)
    }
  }

  case object CSV extends FileFormat {
    override def read(spark: SparkSession, path: String): DataFrame = {
      spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }

    override def readStream(spark: SparkSession, path: String): DataFrame = {
      spark.readStream.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }

    override def write(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): Unit = {
      val saveMode = writeOperation match {
        case WriteOperation.Append    => SaveMode.Append
        case WriteOperation.Overwrite => SaveMode.Overwrite
        case WriteOperation.Update    => throw new IllegalArgumentException("Update operation not supported for batch!")
      }
      setPartitions(df.write, partitionBy).mode(saveMode)
        .format("csv")
        .option("header", "true")
        .save(path)
    }

    override def writeStream(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): DataStreamWriter[Row] = {
      val outputMode = writeOperation match {
        case WriteOperation.Append    => OutputMode.Append()
        case WriteOperation.Update    => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream into CSV!")
        case WriteOperation.Overwrite => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream!")
      }
      setPartitionsStream(df.writeStream, partitionBy)
        .outputMode(outputMode)
        .format("csv")
        .option("path", path)
        .option("header", "true")
    }
  }

  case object ORC extends FileFormat {
    override def read(spark: SparkSession, path: String): DataFrame = spark.read.orc(path)

    override def readStream(spark: SparkSession, path: String): DataFrame = spark.readStream.orc(path)

    override def write(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): Unit = {
      val saveMode = writeOperation match {
        case WriteOperation.Append    => SaveMode.Append
        case WriteOperation.Overwrite => SaveMode.Overwrite
        case WriteOperation.Update    => throw new IllegalArgumentException("Update operation not supported for batch!")
      }
      setPartitions(df.write, partitionBy).mode(saveMode).orc(path)
    }

    override def writeStream(df: DataFrame, path: String, writeOperation: WriteOperation, partitionBy: Seq[String]): DataStreamWriter[Row] = {
      val outputMode = writeOperation match {
        case WriteOperation.Append    => OutputMode.Append()
        case WriteOperation.Update    => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream into ORC!")
        case WriteOperation.Overwrite => throw new IllegalArgumentException("Write operation overwrite is not supported for a stream!")
      }
      setPartitionsStream(df.writeStream, partitionBy)
        .outputMode(outputMode)
        .format("orc")
        .option("path", path)
    }
  }

  def parse(s: String): FileFormat = {
    s.toLowerCase() match {
      case "parquet" => Parquet
      case "json"    => JSON
      case "csv"     => CSV
      case "orc"     => ORC
      case _ =>
        // If file format not found try to load it as a class. (Assume a custom format)
        Class.forName(s).getDeclaredConstructor().newInstance().asInstanceOf[FileFormat]
    }
  }
}
