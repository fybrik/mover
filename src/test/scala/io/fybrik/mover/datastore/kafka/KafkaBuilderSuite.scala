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

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
  */
class KafkaBuilderSuite extends AnyFlatSpec with Matchers {
  it should "build a Kafka config" in {
    val s =
      """
    |source {
    |  kafka {
    |    kafkaBrokers = "eps"
    |    user = "user"
    |    password = "pw"
    |    kafkaTopic = "parquet"
    |    schemaRegistryURL = "http://localhost"
    |    keySchema = "myschema"
    |    valueSchema = "myschema"
    |    createSnapshot = "true"
    |    securityProtocol = "SASL_SSL"
    |    saslMechanism = "SCRAM-SHA-512"
    |    keyDeserializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
    |    valueDeserializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
    |  }
    |}
    |destination {
    |  kafka {
    |    kafkaBrokers = "ept"
    |    kafkaTopic = "bucket"
    |    keyDeserializer = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"
    |    valueDeserializer = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"
    |    sslTruststore = "SGVsbG8gV29ybGQK"
    |    sslTruststoreLocation = "truststore.jks"
    |  }
    |}""".stripMargin

    FileUtils.forceDeleteOnExit(new File("truststore.jks"))
    val config = ConfigFactory.parseString(s)
    val sourceStore = KafkaBuilder.buildSource(config).map(_.asInstanceOf[Kafka])
    val targetStore = KafkaBuilder.buildTarget(config).map(_.asInstanceOf[Kafka])
    val properties = sourceStore.map(_.getCommonProps("kafka."))
    properties.isSuccess shouldBe true
    properties.get("kafka.sasl.jaas.config") shouldBe "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pw\";"
    targetStore.isSuccess shouldBe true
  }

  it should "build a Kafka config with dataformat" in {
    val s =
      """
        |source {
        |  kafka {
        |    kafkaBrokers = "eps"
        |    user = "user"
        |    password = "pw"
        |    kafkaTopic = "parquet"
        |    schemaRegistryURL = "http://localhost"
        |    keySchema = "myschema"
        |    valueSchema = "myschema"
        |    createSnapshot = "true"
        |    securityProtocol = "SASL_SSL"
        |    saslMechanism = "SCRAM-SHA-512"
        |    dataFormat = "avro"
        |  }
        |}
        |destination {
        |  kafka {
        |    kafkaBrokers = "ept"
        |    kafkaTopic = "bucket"
        |    serializationFormat = "json"
        |    sslTruststore = "SGVsbG8gV29ybGQK"
        |    sslTruststoreLocation = "truststore.jks"
        |  }
        |}""".stripMargin

    FileUtils.forceDeleteOnExit(new File("truststore.jks"))
    val config = ConfigFactory.parseString(s)
    val sourceStore = KafkaBuilder.buildSource(config).map(_.asInstanceOf[Kafka])
    val targetStore = KafkaBuilder.buildTarget(config).map(_.asInstanceOf[Kafka])
    val properties = sourceStore.map(_.getCommonProps("kafka."))
    sourceStore.isSuccess shouldBe true
    sourceStore.get.serializationFormat shouldBe SerializationFormat.Avro
    properties.isSuccess shouldBe true
    properties.get("kafka.sasl.jaas.config") shouldBe "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pw\";"
    targetStore.isSuccess shouldBe true
    targetStore.get.serializationFormat shouldBe SerializationFormat.JSON
  }

  behavior of "illegal configurations"
  it should "fail different serializers for key and value" in {
    val s =
      """
        |source {
        |  kafka {
        |    kafkaBrokers = "eps"
        |    kafkaTopic = "parquet"
        |    keyDeserializer = "serde1"
        |    valueDeserializer = "serde2"
        |  }
        |}""".stripMargin

    val config = ConfigFactory.parseString(s)
    val sourceStore = KafkaBuilder.buildSource(config)
    sourceStore.isSuccess shouldBe false
  }

  it should "fail for wrong truststore configuration" in {
    val s =
      """
        |source {
        |  kafka {
        |    kafkaBrokers = "eps"
        |    kafkaTopic = "parquet"
        |    sslTruststore = "trust"
        |  }
        |}""".stripMargin

    val config = ConfigFactory.parseString(s)
    val sourceStore = KafkaBuilder.buildSource(config)
    sourceStore.isSuccess shouldBe false
  }
}
