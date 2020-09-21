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
package com.ibm.datamesh.mover.datastore.kafka

import java.io.File
import java.nio.file.Files
import java.util.Base64

import com.ibm.datamesh.mover.ConfigUtils
import com.ibm.datamesh.mover.datastore.{DataStore, DataStoreBuilder, InputType, Source, Target}
import com.typesafe.config.Config

import scala.util.Try

/**
  * This builds a [[DataStore]] for Kafka. It checks for configuration options and returns a
  * failure object if the configuration is incoherent.
  */
case object KafkaBuilder extends DataStoreBuilder {
  private def createKafkaDataStore(iType: InputType, config: Config): Try[DataStore] = {
    val keyDeserializer = ConfigUtils.opt(config, "keyDeserializer").getOrElse("io.confluent.kafka.serializers.KafkaAvroDeserializer")
    val valueDeserializer = ConfigUtils.opt(config, "valueDeserializer").getOrElse("io.confluent.kafka.serializers.KafkaAvroDeserializer")
    if (!(keyDeserializer.equals(valueDeserializer))) {
      throw new IllegalArgumentException("Currently keyDeserializer and valueDeserializer have to be the same!")
    }
    val dataFormat = if (config.hasPath("serializationFormat")) {
      SerializationFormat.parse(config.getString("serializationFormat"))
    } else if (keyDeserializer.equals("io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer")) {
      SerializationFormat.JSON
    } else SerializationFormat.Avro

    val truststore = ConfigUtils.opt(config, "sslTruststore")
    val truststoreLocation = ConfigUtils.opt(config, "sslTruststoreLocation")
    (truststore, truststoreLocation) match {
      case (Some(tr), Some(loc)) =>
        if (!(new File(loc).exists())) {
          // If truststore file does not exists write the contents into a new file
          val truststoreContent = Base64.getDecoder.decode(tr)
          Files.write(new File(loc).toPath, truststoreContent)
        }
      case (None, Some(_)) =>
      case (None, None)    =>
      case (_, _)          => throw new IllegalArgumentException("Not supported!")
    }

    Try(Kafka(
      iType,
      config.getString("kafkaBrokers"),
      config.getString("user"),
      config.getString("password"),
      config.getString("kafkaTopic"),
      ConfigUtils.opt(config, "schemaRegistryURL"),
      ConfigUtils.opt(config, "keySchema"),
      ConfigUtils.opt(config, "valueSchema"),
      if (config.hasPath("createSnapshot")) config.getBoolean("createSnapshot") else false,
      if (config.hasPath("raw")) config.getBoolean("raw") else false,
      dataFormat,
      if (config.hasPath("securityProtocol")) config.getString("securityProtocol") else "SASL_SSL",
      if (config.hasPath("saslMechanism")) config.getString("saslMechanism") else "SCRAM-SHA-512",
      truststore,
      truststoreLocation,
      ConfigUtils.opt(config, "sslTruststorePassword")
    ))
  }

  override def buildSource(config: Config): Try[DataStore] = {
    createKafkaDataStore(Source, config.getConfig("source").getConfig("kafka"))
  }

  override def buildTarget(config: Config): Try[DataStore] = {
    createKafkaDataStore(Target, config.getConfig("destination").getConfig("kafka"))
  }
}
