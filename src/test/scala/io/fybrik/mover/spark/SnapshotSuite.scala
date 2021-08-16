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

import java.sql.Timestamp

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Tests the behavior of the [[SnapshotAggregator]].
  */
class SnapshotSuite extends AnyFlatSpec with Matchers with SparkTest {
  behavior of "SnapshotAggregator"
  it should "build a correct snapshot from values" in {
    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataFrame(Seq(
        KafkaDummyRecord(Array(1), DummyValue(1, "a"), SnapshotTester.Topic, 0, 0L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(2), DummyValue(2, "b"), SnapshotTester.Topic, 0, 1L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(3), DummyValue(3, "c"), SnapshotTester.Topic, 0, 2L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(2), DummyValue(2, "f"), SnapshotTester.Topic, 0, 3L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(1), DummyValue(1, "z"), SnapshotTester.Topic, 0, 4L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(3), null, SnapshotTester.Topic, 0, 5L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(1), DummyValue(1, "d"), SnapshotTester.Topic, 0, 6L, new Timestamp(0L), 0),
        KafkaDummyRecord(Array(4), DummyValue(4, "z4"), SnapshotTester.Topic, 0, 7L, new Timestamp(0L), 0)
      ))

      val snapshot = SnapshotAggregator.createSnapshot(df)

      val rows = snapshot.as[DummyValue].collect()

      rows should contain theSameElementsAs Seq(
        DummyValue(1, "d"),
        DummyValue(2, "f"),
        DummyValue(4, "z4")
      )
    }
  }

  it should "build a correct snapshot from binary" in {
    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataFrame(Seq(
        BinaryKafkaDummyRecord(Array(1), Array(1, 2, 3), SnapshotTester.Topic, 0, 0L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(2), Array(4, 5, 6), SnapshotTester.Topic, 0, 1L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(3), Array(7, 8, 9), SnapshotTester.Topic, 0, 2L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(2), Array(5, 4, 6), SnapshotTester.Topic, 0, 3L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(1), Array(2, 1, 3), SnapshotTester.Topic, 0, 4L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(3), null, SnapshotTester.Topic, 0, 5L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(1), Array(3, 2, 1), SnapshotTester.Topic, 0, 6L, new Timestamp(0L), 0),
        BinaryKafkaDummyRecord(Array(4), Array(10, 11, 12), SnapshotTester.Topic, 0, 7L, new Timestamp(0L), 0)
      ))

      val snapshot = SnapshotAggregator.createSnapshotOnBinary(df)

      val rows = snapshot.as[Array[Byte]].collect()

      rows should contain theSameElementsAs Seq(
        Array(3, 2, 1),
        Array(5, 4, 6),
        Array(10, 11, 12)
      )
    }
  }
}

object SnapshotTester {
  val Topic = "dummyTopic"
}

case class KafkaDummyRecord(key: Array[Byte], value: DummyValue, topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)
case class BinaryKafkaDummyRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)

case class DummyValue(a: Int, b: String)
case class DummyKey(a: Int)
