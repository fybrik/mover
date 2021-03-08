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

import com.google.gson.{JsonObject, JsonParser}
import com.ibm.m4d.mover.datastore.Source
import com.ibm.m4d.mover.datastore.kafka.SerializationFormat.{Avro, JSON}
import com.ibm.m4d.mover.spark.SparkTest
import com.ibm.m4d.mover.transformation.MyClass
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.{Schema, SchemaCompatibility}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.language.reflectiveCalls

/**
  * Tests serialization/deserialization of Kafka streams.
  */
class KafkaUtilsSuite extends AnyFunSuite with SparkTest with Matchers {
  case class SchemaVersion(subject: String, version: Int, id: Int, schema: String) {
    def toJSONString: String = {
      val obj = new JsonObject()
      obj.addProperty("subject", subject)
      obj.addProperty("version", version)
      obj.addProperty("id", id)
      obj.addProperty("schema", schema)
      obj.toString
    }

    def JSONSchema: String = {
      val obj = new JsonObject()
      obj.addProperty("schema", schema)
      obj.toString
    }

    def asAvroSchema: Schema = {
      new Schema.Parser().parse(schema)
    }
  }

  private val dispatcher = new Dispatcher() {
    private val subjectVersion = "/subjects/([0-9a-zA-Z-]+)/versions/([0-9a-zA-Z-]+)".r
    private val subject = "/subjects/([0-9a-zA-Z-]+)/versions".r
    private val schemas = "/schemas/ids/([0-9a-zA-Z-]+)".r
    private val compatibility = "/compatibility/subjects/([0-9a-zA-Z-]+)/versions/([0-9a-zA-Z-]+)".r
    val cacheBySubject = new mutable.HashMap[String, mutable.HashMap[Int, SchemaVersion]]
    def maxId: Int = {
      if (cacheBySubject.isEmpty) {
        0
      } else {
        cacheBySubject.map(_._2.values.map(_.id).max).max
      }
    }
    def maxVersion(subject: String): Option[Int] = {
      cacheBySubject.get(subject).map(_.values.map(_.version).max)
    }

    def latest(subject: String): Option[SchemaVersion] = {
      cacheBySubject.get(subject).map(_.values.maxBy(_.version))
    }

    def find(subject: String, version: Int): Option[SchemaVersion] = {
      cacheBySubject.get(subject).flatMap(_.values.find(_.version == version))
    }

    def notFoundResponse(subject: String): MockResponse = {
      val notFoundText = s"""{\"message\": \"No artifact with ID '$subject' was found.\",  \"error_code\": 404}"""
      new MockResponse().setResponseCode(404).setBody(notFoundText).setHeader("Content-Type", "application/json")
    }

    override def dispatch(recordedRequest: RecordedRequest): MockResponse = {
      HttpUrl.parse(recordedRequest.getPath)
      recordedRequest.getRequestUrl.encodedPath() match {
        case subjectVersion(subject, version) =>
          if (version.equals("latest")) {
            latest(subject) match {
              case None => notFoundResponse(subject)
              case Some(schema) =>
                new MockResponse().setResponseCode(200)
                  .setBody(schema.toJSONString)
                  .setHeader("Content-Type", "application/json")
            }
          } else {
            find(subject, version.toInt) match {
              case None =>
                notFoundResponse(subject)
              case Some(schema) =>
                new MockResponse().setResponseCode(200)
                  .setBody(schema.toJSONString)
                  .setHeader("Content-Type", "application/json")
            }
          }
        case subject(subject) =>
          recordedRequest.getMethod match {
            case "GET" =>
              if (cacheBySubject.contains(subject)) {
                val versions = cacheBySubject.get(subject).map(_.values.map(_.version).mkString("[", ",", "]")).getOrElse("[]")
                new MockResponse().setResponseCode(200).setBody(versions).setHeader("Content-Type", "application/json")
              } else {
                notFoundResponse(subject)
              }
            case "POST" =>
              val jsonElement = new JsonParser().parse(recordedRequest.getBody.readUtf8())
              val schema = jsonElement.getAsJsonObject.get("schema").getAsString
              cacheBySubject.synchronized {
                if (cacheBySubject.contains(subject)) {
                  // Check if object already exists in registry cache
                  val existingObject = cacheBySubject(subject).values.find(_.schema.equals(schema))
                  existingObject match {
                    case Some(obj) =>
                      new MockResponse().setResponseCode(200).setBody("{\"id\": " + obj.id + "}").setHeader("Content-Type", "application/json")
                    case None =>
                      val nextVersion = maxVersion(subject).getOrElse(0) + 1
                      val id = maxId + 1
                      cacheBySubject(subject) += (nextVersion -> SchemaVersion(subject, nextVersion, id, schema))
                      new MockResponse().setResponseCode(200).setBody("{\"id\": " + id + "}").setHeader("Content-Type", "application/json")
                  }
                } else {
                  val newMap = new mutable.HashMap[Int, SchemaVersion]()
                  val nextVersion = 1
                  val id = maxId + 1
                  newMap += (nextVersion -> SchemaVersion(subject, nextVersion, id, schema))
                  cacheBySubject += (subject -> newMap)
                  new MockResponse().setResponseCode(200).setBody("{\"id\": " + id + "}").setHeader("Content-Type", "application/json")
                }
              }
          }
        case schemas(id) =>
          val maybeEntry = cacheBySubject.flatMap(_._2.find(_._2.id == id.toInt).map(_._2)).headOption

          maybeEntry match {
            case Some(schema) =>
              new MockResponse().setResponseCode(200)
                .setBody(schema.JSONSchema)
                .setHeader("Content-Type", "application/json")
            case None =>
              notFoundResponse(id)
          }
        case compatibility(subject, version) =>
          if (version.equals("latest")) {
            latest(subject) match {
              case None => notFoundResponse(subject)
              case Some(schema) =>
                val jsonElement = new JsonParser().parse(recordedRequest.getBody.readUtf8())
                val rSchema = jsonElement.getAsJsonObject.get("schema").getAsString
                val readerSchema = new Schema.Parser().parse(rSchema)
                val compatibility = SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, schema.asAvroSchema)
                if (compatibility.getType.equals(SchemaCompatibilityType.COMPATIBLE)) {
                  new MockResponse().setResponseCode(200)
                    .setBody("{\"is_compatible\": true}")
                    .setHeader("Content-Type", "application/json")
                } else {
                  new MockResponse().setResponseCode(200)
                    .setBody("{\"is_compatible\": false}")
                    .setHeader("Content-Type", "application/json")
                }
            }
          } else {
            notFoundResponse(version)
          }
        case _ => new MockResponse().setResponseCode(404)
      }
    }
  }

  test("test serde with confluent avro and registry") {
    val server = new MockWebServer
    dispatcher.cacheBySubject.clear()
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/")

    withSparkSession { spark =>
      import spark.implicits._

      val records = Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )

      val refDF = spark.createDataFrame(records)

      val kafkaConfig = new Kafka(
        Source,
        "localhost:9091",
        "",
        "",
        "topic",
        Some(url.toString),
        None,
        None,
        false,
        false,
        Avro
      )

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(refDF, Seq(refDF("i")))
      val kafkaSerializedDF = KafkaUtils.catalystToKafka(kafkaDF, kafkaConfig)

      //simulate write
      val rows = kafkaSerializedDF.collect()

      val kafkaDeserializedDF = KafkaUtils.kafkaBytesToCatalyst(kafkaSerializedDF, kafkaConfig)

      val deserializedRows = kafkaDeserializedDF.collect()

      deserializedRows should have size 3

      val deserializedValues = KafkaUtils.mapToValue(kafkaDeserializedDF).as[MyClass].collect()

      deserializedValues should contain theSameElementsAs records
      dispatcher.cacheBySubject.keySet should contain theSameElementsAs Seq("topic-key", "topic-value")
      dispatcher.maxId shouldBe 2
      dispatcher.cacheBySubject.values.map(_.size).sum shouldBe 2
    }
    server.shutdown()
  }

  test("test serde with avro schema") {
    val keySchema = "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"topic\",\"fields\":[{\"name\":\"i\",\"type\":[\"int\",\"null\"]}]}"
    val valueSchema = "{\"type\":\"record\",\"name\":\"Value\",\"namespace\":\"topic\",\"fields\":[{\"name\":\"i\",\"type\":[\"int\",\"null\"]},{\"name\":\"s\",\"type\":[\"string\",\"null\"]},{\"name\":\"d\",\"type\":\"double\"}]}"

    withSparkSession { spark =>
      import spark.implicits._

      val records = Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )

      val refDF = spark.createDataFrame(records)

      val kafkaConfig = new Kafka(
        Source,
        "localhost:9091",
        "",
        "",
        "topic",
        None,
        Some(keySchema),
        Some(valueSchema),
        false,
        false,
        Avro
      )

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(refDF, Seq(refDF("i")))
      val kafkaSerializedDF = KafkaUtils.catalystToKafka(kafkaDF, kafkaConfig)

      //simulate write
      val rows = kafkaSerializedDF.collect()

      val kafkaDeserializedDF = KafkaUtils.kafkaBytesToCatalyst(kafkaSerializedDF, kafkaConfig)

      val deserializedRows = kafkaDeserializedDF.collect()

      deserializedRows should have size 3

      val deserializedValues = KafkaUtils.mapToValue(kafkaDeserializedDF).as[MyClass].collect()

      deserializedValues should contain theSameElementsAs records
    }
  }

  test("test serde with json schema") {

    withSparkSession { spark =>
      import spark.implicits._
      val records = Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )

      val refDF = spark.createDataFrame(records)

      val keySchema = refDF.select("i").schema.json
      val valueSchema = refDF.schema.json

      val kafkaConfig = new Kafka(
        Source,
        "localhost:9091",
        "",
        "",
        "topic",
        None,
        Some(keySchema),
        Some(valueSchema),
        false,
        false,
        JSON
      )

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(refDF, Seq(refDF("i")))
      val kafkaSerializedDF = KafkaUtils.catalystToKafka(kafkaDF, kafkaConfig)

      //simulate write
      val rows = kafkaSerializedDF.collect()

      val kafkaDeserializedDF = KafkaUtils.kafkaBytesToCatalyst(kafkaSerializedDF, kafkaConfig)

      val deserializedRows = kafkaDeserializedDF.collect()

      deserializedRows should have size 3

      val deserializedValues = KafkaUtils.mapToValue(kafkaDeserializedDF).as[MyClass].collect()

      deserializedValues should contain theSameElementsAs records
    }
  }

  test("test serde with json and no schema (automatic inference)") {

    withSparkSession { spark =>
      import spark.implicits._
      val records = Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )

      val refDF = spark.createDataFrame(records)

      val keySchema = refDF.select("i").schema.json
      val valueSchema = refDF.schema.json

      val kafkaConfig = new Kafka(
        Source,
        "localhost:9091",
        "",
        "",
        "topic",
        None,
        None,
        None,
        false,
        false,
        JSON
      )

      val kafkaDF = KafkaUtils.toKafkaWriteableDF(refDF, Seq(refDF("i")))
      val kafkaSerializedDF = KafkaUtils.catalystToKafka(kafkaDF, kafkaConfig)

      //simulate write
      val rows = kafkaSerializedDF.collect()

      val kafkaDeserializedDF = KafkaUtils.kafkaBytesToCatalyst(kafkaSerializedDF, kafkaConfig)

      val deserializedRows = kafkaDeserializedDF.collect()

      deserializedRows should have size 3

      // Cast to integer first as JSON automatic inference infers integer numbers as long
      val deserializedValues = KafkaUtils.mapToValue(kafkaDeserializedDF).select(col("i").cast(IntegerType), col("s"), col("d"))
        .as[MyClass]
        .collect()

      deserializedValues should contain theSameElementsAs records
    }
  }
}
