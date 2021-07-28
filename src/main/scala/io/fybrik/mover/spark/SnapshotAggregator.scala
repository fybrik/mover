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
package io.fybrik.mover.spark

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * This aggregator compares values of the same key and returns only the one with the largest offset value.
  */
class SnapshotAggregator(schema: StructType) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType(Array(schema.apply("offset"), schema.apply("value")))
  }

  override def bufferSchema: StructType = {
    new StructType(Array(schema.apply("offset"), schema.apply("value")))
  }

  override def dataType: DataType = schema.apply("value").dataType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, -1L)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputOffset = input.getLong(0)
    if (buffer.getLong(0) < inputOffset) {
      buffer.update(0, input.getLong(0))
      buffer.update(1, input.get(1))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getLong(0) < buffer2.getLong(0)) {
      buffer1.update(0, buffer2.getLong(0))
      buffer1.update(1, buffer2.get(1))
    }
  }

  override def evaluate(buffer: Row): Any = buffer.get(1)
}

object SnapshotAggregator {

  /**
    * This method does a "snapshot" on a typical Kafka dataframe that contains a key(binary), a value(extracted) and an offset.
    *
    * It returns the value with the highest offset for a key.
    * If the value with the largest offset for a key is null it is filtered out.
    *
    * @param df dataframe with Kafka columns
    * @return dataframe of snapshotted values
    */
  def createSnapshot(df: DataFrame): DataFrame = {
    val snapshotAggregatorFunction = new SnapshotAggregator(df.schema)

    val aggregatedDF =
      df.groupBy("key")
        .agg(snapshotAggregatorFunction(df("offset"), df("value")).as("value"))

    val keyValueDF = aggregatedDF.filter(aggregatedDF("value").isNotNull)
    keyValueDF.select("value.*")
  }

  /**
    * This method does a "snapshot" on a typical Kafka dataframe that contains a key(binary), a value(binary) and an offset.
    *
    * It returns the value with the highest offset for a key.
    * If the value with the largest offset for a key is null it is filtered out.
    *
    * @param df dataframe with Kafka binary value
    * @return dataframe of snapshotted values
    */
  def createSnapshotOnBinary(df: DataFrame): DataFrame = {
    val snapshotAggregatorFunction = new SnapshotAggregator(df.schema)

    val aggregatedDF =
      df.groupBy("key")
        .agg(snapshotAggregatorFunction(df("offset"), df("value")).as("value"))

    val keyValueDF = aggregatedDF.filter(aggregatedDF("value").isNotNull)
    keyValueDF.select("value")
  }
}
