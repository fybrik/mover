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

import java.util.Properties
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import io.fybrik.mover.{DataFlowType, DataType, MetaData, WriteOperation}
import io.fybrik.mover.datastore.{DataStore, InputType}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * [[DataStore]] implementation of a Kafka streaming system.
  * This supports multiple data flow modes and serialization formats.
  */
class Kafka(
    iType: InputType,
    val kafkaBrokers: String,
    user: String,
    password: String,
    val kafkaTopic: String,
    val schemaRegistryURL: Option[String] = None,
    val keySchema: Option[String] = None,
    val valueSchema: Option[String] = None,
    createSnapshot: Boolean = false,
    val raw: Boolean = false,
    val serializationFormat: SerializationFormat = SerializationFormat.Avro,
    securityProtocol: String = "SASL_SSL",
    saslMechanism: String = "SCRAM-SHA-512",
    sslTruststoreLocation: Option[String] = None,
    sslTruststorePassword: Option[String] = None
) extends DataStore(iType) {
  private val logger = LoggerFactory.getLogger(getClass)

  override def additionalSparkConfig(): Map[String, String] = Map.empty[String, String]

  override def sourceMetadata(): Option[MetaData] = ???

  override def read(spark: SparkSession, dataFlowType: DataFlowType, dataType: DataType): DataFrame = {
    dataFlowType match {
      case DataFlowType.Batch  => KafkaUtils.readFromKafka(spark, this)
      case DataFlowType.Stream => KafkaUtils.readKafkaStream(spark, this)
    }
  }

  override def write(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): Unit = {
    KafkaUtils.writeToKafka(df, this)
  }

  override def writeStream(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): DataStreamWriter[Row] = {
    KafkaUtils.writeToKafkaStream(df, this, kafkaTopic)
  }

  def getCredentialProperties(prefix: String = ""): Map[String, String] = {
    if (user.nonEmpty) {
      val jaasClass = saslMechanism.toLowerCase() match {
        case "plain"         => "org.apache.kafka.common.security.plain.PlainLoginModule"
        case "scram-sha-512" => "org.apache.kafka.common.security.scram.ScramLoginModule"
        case _               => throw new IllegalArgumentException(s"Unsupported SASL mechanism! '$saslMechanism'")
      }
      Map(
        prefix + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> securityProtocol.toUpperCase(),
        prefix + SaslConfigs.SASL_MECHANISM -> saslMechanism.toUpperCase(),
        (prefix + SaslConfigs.SASL_JAAS_CONFIG -> (jaasClass + " required username=\"" + user + "\" password=\"" + password + "\";"))
      ) ++ sslTruststoreLocation.map(s => prefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> s) ++ sslTruststorePassword.map(s => prefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> s)
    } else {
      Map.empty[String, String]
    }
  }

  /**
    * @return Kafka properties that are common to both producers and consumers.
    */
  def getCommonProps(prefix: String = ""): Map[String, String] = {
    getCredentialProperties(prefix) ++ Map(
      prefix + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
      // compression is transparent to the consumer
      prefix + "compression.type" -> "gzip",
      // need larger batch size than the default 16 KB for compression to be efficient.
      prefix + "batch.size" -> "1048576",
      // set linger such that Kafka waits up to 1 second to fill the batch size.
      // this has a bit of effect on the latency but throughput seems more important.
      prefix + "linger.ms" -> "1000",
      // below options are put to these insane values in order to prevent
      // org.apache.common.errors.UnknownTopicOrPartitionException
      // when automatic topic creation is used in Kafka.
      prefix + "session.timeout.ms" -> "300000",
      prefix + "fetch.max.wait.ms" -> "300000",
      prefix + "request.timeout.ms" -> "800000",
      prefix + "max.request.size" -> "16000000",
      prefix + "retry.backoff.ms" -> "1000",
      prefix + "retries" -> "300",
      prefix + "max.in.flight.requests.per.connection" -> "1"
    ) ++ schemaRegistryURL.map(s => prefix + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> s)
  }

  override def deleteTarget(): Unit = {
    if (KafkaUtils.deleteTopic(this, kafkaTopic)) {
      logger.info(s"Kafka topic ${kafkaTopic} was deleted!")
    } else {
      logger.warn(s"Kafka topic ${kafkaTopic} could not be deleted!")
    }
  }

  def inferSchema(): Boolean = {
    serializationFormat.equals(SerializationFormat.JSON) && (keySchema.isEmpty || valueSchema.isEmpty)
  }
}
