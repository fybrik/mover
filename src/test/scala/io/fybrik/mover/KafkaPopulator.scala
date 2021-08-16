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

import java.io.File
import io.fybrik.mover.datastore.kafka.{Kafka, KafkaBuilder, KafkaUtils}
import io.fybrik.mover.spark.{SparkTest, _}
import com.typesafe.config.ConfigFactory

/**
  * This simple utitlity program populates a Kafka source with a couple of test records.
  */
object KafkaPopulator extends SparkTest {
  def main(args: Array[String]): Unit = {
    withSparkSession { spark =>
      import spark.implicits._

      val conf = ConfigFactory.parseFile(new File("src/test/resources/local-to-kafka.conf"))

      val kafka = KafkaBuilder.buildTarget(conf).get.asInstanceOf[Kafka]

      val data = Seq(
        TestClass(1, "1", "a", 1.0),
        TestClass(2, "2", "b", 2.0),
        TestClass(3, "4", "c", 4.0)
      )

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafka)

      println("Wrotes records to topic")
    }
  }
}
