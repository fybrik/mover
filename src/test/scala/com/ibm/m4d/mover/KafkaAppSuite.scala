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
package com.ibm.m4d.mover

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer, KafkaContainer, MultipleContainers}
import com.ibm.m4d.mover.datastore.kafka.{Kafka, KafkaBuilder, KafkaUtils}
import com.ibm.m4d.mover.spark.{SparkTest, _}
import com.ibm.m4d.mover.transformation.{MyClass, MyClassKV, MyClassKey}
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.collection.JavaConverters._

/**
  * This suite is testing the [[Transfer]] program in an integration test manner with test containers.
  */
class KafkaAppSuite extends AnyFlatSpec with ForAllTestContainer with SparkTest with Matchers with BeforeAndAfterEach {

  val kafkaContainer: KafkaContainer = new KafkaContainer()
  val registryContainer: GenericContainer = new GenericContainer("apicurio/apicurio-registry-mem:latest", Seq(8080))
  override val container: MultipleContainers = MultipleContainers(kafkaContainer, registryContainer)

  override protected def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File("test.parq"))
  }

  val data = Seq(
    TestClass(1, "1", "a", 1.0),
    TestClass(2, "2", "b", 2.0),
    TestClass(3, "4", "c", 4.0),
    TestClass(2, "2", "c", 4.0)
  )

  val cdcData = Seq(
    MyClassKV(MyClassKey(3), MyClass(3, "c1", 3.0)),
    MyClassKV(MyClassKey(1), MyClass(1, "a1", 1.0)),
    MyClassKV(MyClassKey(2), MyClass(2, "b1", 2.0)),
    MyClassKV(MyClassKey(1), MyClass(1, "a2", 1.1)),
    MyClassKV(MyClassKey(2), MyClass(2, "b2", 2.1)),
    MyClassKV(MyClassKey(1), MyClass(1, "a3", 1.2)),
    MyClassKV(MyClassKey(3), null),
    MyClassKV(MyClassKey(4), MyClass(4, "d1", 4.0)),
  )

  behavior of "Batch"
  it should "Read Kafka log in different formats and write to local" in {
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("batch-log-" + format),
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "overwrite",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("batch-log-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafka)
    }

    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    withSparkSession { spark =>
      import spark.implicits._
      val rdf = spark.read.parquet("test.parq")
      rdf.schema.names should contain theSameElementsAs Seq("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
      val rData = rdf.select("value.*").as[TestClass].collect()
      rData should contain theSameElementsAs data
    }
  }

  it should "Read Kafka cdc in different formats and write to local (Snapshot)" in {
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("batch-cdc-" + format),
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "transformations.0.action" -> "digest",
      "transformations.0.columns.0" -> "s",
      "readDataType" -> "cdc",
      "writeDataType" -> "logdata",
      "writeOperation" -> "overwrite",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("batch-cdc-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSessionExtra(Map("spark.sql.shuffle.partitions" -> "1")) { spark =>
      import spark.implicits._

      val kafkaDF = spark.createDataset(cdcData).coalesce(1).toDF()
        .setNullableStateOfColumn("key.i", nullable = false)
        .setNullableStateOfColumn("value.i", nullable = false)
        .setNullableStateOfColumn("value.d", nullable = false)
        .setNullableStateOfColumn("key", nullable = false)

      KafkaUtils.writeToKafka(kafkaDF, kafka)
    }

    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    withSparkSession { spark =>
      import spark.implicits._
      val rdf = spark.read.parquet("test.parq")
      val expected = Seq(
        MyClass(1, "a3", 1.2),
        MyClass(2, "b2", 2.1),
        MyClass(4, "d1", 4.0),
      )
      val rData = rdf.as[MyClass].collect()
      rData should contain theSameElementsAs expected
    }
  }

  it should "Read Kafka avro debugging info" in {
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("batch-log-" + format),
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "source.kafka.raw" -> "true",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "overwrite",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("batch-log-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSession { spark =>
      import spark.implicits._
      import com.ibm.m4d.mover.spark._

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      kafka.write(kafkaDF, DataType.LogData, WriteOperation.Append)

      val rawDebug = KafkaUtils.readFromKafka(spark, kafka)
      rawDebug.schema.names should contain theSameElementsAs Seq(
        "key", "value", "topic", "partition", "offset", "timestamp",
        "timestampType", "data_key", "data_value", "key_schema_id", "value_schema_id"
      )

      kafka.deleteTarget()
    }
  }

  behavior of "failures"
  it should "fail if topic that is deleted does not exist" in {
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("batch-log-non-existant"),
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "source.kafka.raw" -> "true",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "overwrite",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("batch-log-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    kafka.deleteTarget()
  }

  behavior of "streams"
  it should "append avro data to a file" in {
    FileUtils.deleteDirectory(new File("test.parq"))
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("batch-log-non-existant"),
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "flowType" -> "stream",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "append",
      "triggerInterval" -> "once",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("stream-log-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafka)
    }

    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    withSparkSession { spark =>
      import spark.implicits._
      val rdf = spark.read.parquet("test.parq")
      rdf.schema.names should contain theSameElementsAs Seq("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
      val rData = rdf.select("value.*").as[TestClass].collect()
      rData should contain theSameElementsAs data
    }

    FileUtils.deleteDirectory(new File("test.parq"))
    kafka.deleteTarget()
  }

  it should "append json data to a file" in {
    FileUtils.deleteDirectory(new File("test.parq"))
    System.setProperty("IS_TEST", "true")
    val format = "json"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-local.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> ("stream-json"),
      "source.kafka.dataFormat" -> format,
      "flowType" -> "stream",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "append",
      "triggerInterval" -> "once",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("stream-json-" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafka)
    }

    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    withSparkSession { spark =>
      import spark.implicits._
      val rdf = spark.read.parquet("test.parq")
      rdf.schema.names should contain theSameElementsAs Seq("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
      val rData = rdf.select("value.*")
        .select(col("i").cast(IntegerType), col("s"), col("s2"), col("d"))
        .as[TestClass]
        .collect()
      rData should contain theSameElementsAs data
    }

    FileUtils.deleteDirectory(new File("test.parq"))
    kafka.deleteTarget()
  }

  it should "write data out to Kafka again" in {
    System.setProperty("IS_TEST", "true")
    val format = "avro"
    val registryUrl = "http://" + registryContainer.containerIpAddress + ":" + registryContainer.mappedPort(8080) + "/api/ccompat"
    val baseConf = ConfigFactory.parseFile(new File("src/main/resources/kafka-to-kafka.conf"))
    val newConf = ConfigFactory.parseMap(Map(
      "source.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "source.kafka.kafkaTopic" -> "stream-log-src",
      "source.kafka.schemaRegistryURL" -> registryUrl,
      "source.kafka.serializationFormat" -> format,
      "destination.kafka.kafkaBrokers" -> kafkaContainer.bootstrapServers,
      "destination.kafka.kafkaTopic" -> "stream-log-target",
      "destination.kafka.schemaRegistryURL" -> registryUrl,
      "destination.kafka.serializationFormat" -> format,
      "flowType" -> "stream",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "append",
      "triggerInterval" -> "once",
    ).asJava).withFallback(baseConf)

    val tempConf = Files.createTempFile("stream-log-src" + format, ".json")
    Files.write(tempConf, newConf.root().render(ConfigRenderOptions.concise()).getBytes())

    val kafka = KafkaBuilder.buildSource(newConf).get.asInstanceOf[Kafka]

    withSparkSession { spark =>
      import spark.implicits._

      val df = spark.createDataset(data).toDF()
        .setNullableStateOfColumn("i", nullable = false)
        .setNullableStateOfColumn("d", nullable = false)

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(df, Seq(df("i")))

      KafkaUtils.writeToKafka(kafkaDF, kafka)
    }

    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    withSparkSession { spark =>
      import spark.implicits._
      val kafkaTarget = KafkaBuilder.buildTarget(newConf).get.asInstanceOf[Kafka]
      val kafkaDF = KafkaUtils.readFromKafka(spark, kafkaTarget)
      kafkaDF.schema.names should contain theSameElementsAs Seq("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
      val rData = kafkaDF.select("value.*").as[TestClass].collect()
      rData should contain theSameElementsAs data
    }

    kafka.deleteTarget()
  }
}
