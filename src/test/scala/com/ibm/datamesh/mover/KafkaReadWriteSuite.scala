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
package com.ibm.datamesh.mover

import com.ibm.datamesh.mover.datastore.Source
import com.ibm.datamesh.mover.datastore.kafka.SerializationFormat.Avro
import com.ibm.datamesh.mover.datastore.kafka.{Kafka, KafkaUtils}
import com.ibm.datamesh.mover.spark.{SparkTest, _}
import org.scalatest.matchers.should.Matchers

/**
  * Tests writing and reading to and from Kafka using the [[KafkaUtils]].
  * This is kind of an integration test.
  * It is ignored by default. If the system property -DkafkaPwd=...  is passed then it will run.
  */
class KafkaReadWriteSuite extends ExtendedFunSuite with SparkTest with Matchers {
  testIf("it should write to Kafka and read from Kafka localhost")(portListening(9092)) {
    withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        TestClass(1, "1", "a", 1.0),
        TestClass(2, "2", "b", 2.0),
        TestClass(3, "4", "c", 4.0)
      )

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val assetName = "DHUBDEMO"
      val topic = assetName + ".demo.small.test"

      val kafkaConfig = new Kafka(
        Source,
        "localhost:9092",
        "",
        "",
        topic,
        Some("http://localhost:8081"),
        None,
        None,
        false,
        false,
        Avro,
        "SASL_SSL",
        "SCRAM-SHA-512",
        None,
        None,
        None,
      )

      val beforeSchema = df.schema

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafkaConfig)

      // Reading topic again
      val readKafkaDF = KafkaUtils.readFromKafka(spark, kafkaConfig)

      val afterSchema = readKafkaDF.schema

      val readArray = readKafkaDF.as[TestClass].collect()

      readArray should contain theSameElementsAs data
      beforeSchema shouldBe afterSchema
      println("end")
    }
  }
}

case class TestClass(i: Integer, s: String, s2: String, d: Double)
