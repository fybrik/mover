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
package com.ibm.m4d.mover.datastore.kafka

import java.util.Properties

import com.ibm.m4d.mover.datastore.{DataStore, InputType}
import com.ibm.m4d.mover.{DataFlowType, DataType, MetaData, WriteOperation}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
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
case class Kafka(
    iType: InputType,
    kafkaBrokers: String,
    user: String,
    password: String,
    kafkaTopic: String,
    schemaRegistryURL: Option[String] = None,
    keySchema: Option[String] = None,
    valueSchema: Option[String] = None,
    createSnapshot: Boolean = false,
    raw: Boolean = false,
    serializationFormat: SerializationFormat = SerializationFormat.Avro,
    securityProtocol: String = "SASL_SSL",
    saslMechanism: String = "SCRAM-SHA-512",
    sslTruststore: Option[String] = None,
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
    val jaasClass = saslMechanism.toLowerCase() match {
      case "plain"         => "org.apache.kafka.common.security.plain.PlainLoginModule"
      case "scram-sha-512" => "org.apache.kafka.common.security.scram.ScramLoginModule"
      case _               => throw new IllegalArgumentException(s"Unsupported SASL mechanism! '$saslMechanism'")
    }
    Map(
      prefix + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> securityProtocol,
      prefix + SaslConfigs.SASL_MECHANISM -> saslMechanism,
      (prefix + SaslConfigs.SASL_JAAS_CONFIG -> (jaasClass + " required username=\"" + user + "\" password=\"" + password + "\";"))
    ) ++ sslTruststoreLocation.map(s => prefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> s) ++ sslTruststorePassword.map(s => prefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> s)
  }

  /**
    * @return Kafka properties that are common to both producers and consumers.
    */
  private def getCommonProps = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
    getCredentialProperties().foreach { case (k, v) => props.put(k, v) }
    props
  }

  /**
    * @return The Kafka producer properties that are needed to write into the control topic.
    */
  def getProducerProperties: Properties = {
    val props = getCommonProps
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props
  }

  /**
    * @return Using the given groupId, returns the Kafka consumer properties.
    */
  def getConsumerProperties(groupId: String, metadataRefresh: Option[Long]): Properties = { // consumer properties
    val props = this.getCommonProps
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    // messages are not committed. This makes sure that we get all.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    // confluent specific configuration required by the avro binary de-serializer
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false")
    // set the frequency by which metadata, e.g. wildcard topic subscriptions, are refreshed.
    metadataRefresh.foreach(l => props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, l.toString))
    props
  }

  /**
    * @return The Kafka consumer properties with a default group Id
    */
  def getConsumerProperties: Properties = this.getConsumerProperties("ctrl-msg-consumer", None)

  override def deleteTarget(): Unit = {
    KafkaUtils.deleteTopic(this, kafkaTopic)
    logger.info(s"Kafka topic ${kafkaTopic} was deleted!")
  }
}
