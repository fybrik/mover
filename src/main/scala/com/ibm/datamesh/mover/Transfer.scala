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

import java.io.File
import java.nio.charset.Charset
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import com.ibm.datamesh.mover.conf.CredentialSubstitutor
import com.ibm.datamesh.mover.datastore.DataStoreBuilder
import com.ibm.datamesh.mover.spark.{SnapshotAggregator, SparkConfig, SparkOutputCounter, SparkUtils}
import com.ibm.datamesh.mover.transformation.Transformation
import com.typesafe.config.ConfigFactory
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClientException}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * This is the main application logic of a transfer.
  * Currently the separation between a batch and a stream application is only done for the writing part.
  * About the general workflow:
  * 1. A [[org.apache.spark.sql.DataFrame]] is loaded from a [[com.ibm.datamesh.mover.datastore.DataStore]]
  * 2. Optionally a snapshot is calculated for change data.
  * 3. Transformations are applied
  * 4. Data is written out to a target [[com.ibm.datamesh.mover.datastore.DataStore]].
  */
object Transfer {
  private val logger = LoggerFactory.getLogger(Transfer.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting streaming application...")
    if (args.length != 1) {
      throw new IllegalArgumentException(
        "Please specify the path to the config file as only parameter!"
      )
    }
    val config = CredentialSubstitutor.substituteCredentials(ConfigFactory.parseFile(new File(args(0))))

    val dataFlowType = if (config.hasPath("flowType")) {
      DataFlowType.parse(config.getString("flowType"))
    } else {
      DataFlowType.Batch
    }

    val sourceDataType = if (config.hasPath("readDataType")) {
      DataType.parse(config.getString("readDataType"))
    } else {
      DataType.LogData
    }

    val targetDataType = if (config.hasPath("writeDataType")) {
      DataType.parse(config.getString("writeDataType"))
    } else {
      sourceDataType
    }

    val writeOperation = if (config.hasPath("writeOperation")) {
      WriteOperation.parse(config.getString("writeOperation"))
    } else {
      dataFlowType match {
        case DataFlowType.Batch  => WriteOperation.Overwrite
        case DataFlowType.Stream => WriteOperation.Append
      }
    }

    val source = DataStoreBuilder.buildSource(config).recover { case NonFatal(e) => throw e }.get
    val target = DataStoreBuilder.buildTarget(config).recover { case NonFatal(e) => throw e }.get

    val transformations = Transformation.loadTransformations(config)

    // Collect all additional spark configurations from source/target (e.g. COS or Kafka specific configuration)
    // and additional spark configuration from transformations (e.g. PME)
    val additionalSparkConfig = source.additionalSparkConfig() ++
      target.additionalSparkConfig() ++
      transformations.foldLeft(Map.empty[String, String])((m, t) => m ++ t.additionalSparkConfig())

    val spark = SparkUtils.sparkSession("transfer", debug = false, local = false, sparkConfig = SparkConfig.default, additionalOptions = additionalSparkConfig)

    try {

      val sourceDF = source.read(spark, dataFlowType, sourceDataType)

      val performSnapshot = sourceDataType == DataType.ChangeData && targetDataType == DataType.LogData && dataFlowType == DataFlowType.Batch

      val df = if (performSnapshot) {
        logger.info("Performing a snapshot of the source...")
        SnapshotAggregator.createSnapshot(sourceDF)
      } else {
        sourceDF
      }

      val transformedDF = try {
        transformations
          // as they were already applied to the Spark configuration
          .foldLeft(df) { case (df, transformation) =>
            sourceDataType match {
              case DataType.LogData                          => transformation.transformLogData(df)
              case DataType.ChangeData if performSnapshot    => transformation.transformLogData(df) // If a snapshot has already been performed treat data as log data
              case DataType.ChangeData if !performSnapshot   => transformation.transformChangeData(df)
            }
          }
      } catch {
        case NonFatal(e) =>
          val msg = "Configuring the transformations failed with message: " + e.getMessage
          writeTerminationMessage(msg)
          throw e
      }

      dataFlowType match {
        case DataFlowType.Batch =>

          val counter = SparkOutputCounter.createAndRegister(spark)
          target.write(transformedDF, targetDataType, writeOperation)

          logger.info(s"Transfer done!")
          val numRecords = counter.lastNumOfOutputRows match {
            case Some(count) =>
              logger.info(s"Wrote $count records!")
              count
            case None =>
              logger.warn("Could not find any counts!")
              -1L
          }

          val msg = s"Ingested $numRecords records!"
          writeK8sEvent(msg, "Success")

          writeTerminationMessage(msg)

        case DataFlowType.Stream =>
          val stream = target.writeStream(transformedDF, targetDataType, writeOperation)
          val s = stream
            .trigger(Trigger.ProcessingTime(config.getString("triggerInterval")))
            .start()

          s.awaitTermination()
      }

    } catch {
      case NonFatal(e) =>
        val msg = "Moving data failed when writing with message: " + e.getMessage
        writeTerminationMessage(msg)
        throw e
    } finally {
      spark.stop()
    }
  }

  def writeTerminationMessage(msg: String): Unit = {
    try {
      FileUtils.writeStringToFile(new File("/dev/termination-log"), msg, Charset.defaultCharset())
    } catch {
      case NonFatal(e) => logger.warn("Could not write termination message " + msg)
    }
  }

  private def writeK8sEvent(message: String, reason: String): Unit = {
    val randStr = RandomStringUtils.randomAlphabetic(16)

    try {
      if (sys.env.contains("NAMESPACE") && sys.env.contains("OWNER_NAME") && sys.env.contains("OWNER_KIND") && sys.env.contains("OWNER_UID")) {
        val client = new DefaultKubernetesClient
        val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT)
        client.events().createNew()
          .withType("Normal")
          .withMessage(message)
          .withReason(reason)
          .withFirstTimestamp(timestamp)
          .withLastTimestamp(timestamp)
          .withNewMetadata()
          .withName("mover." + randStr)
          .withNamespace(sys.env("NAMESPACE"))
          .endMetadata()
          .withNewInvolvedObject()
          .withNamespace(sys.env("NAMESPACE"))
          .withName(sys.env("OWNER_NAME"))
          .withKind(sys.env("OWNER_KIND"))
          .withUid(sys.env("OWNER_UID"))
          .endInvolvedObject()
          .done()
      } else {
        logger.warn("Could not send event to Kubernetes as some needed environment variables are not set.")
      }
    } catch {
      case e: KubernetesClientException =>
        logger.warn("Could not send finished event to Kubernetes!", e)
    }
  }

}
