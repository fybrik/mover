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

import com.ibm.m4d.mover.conf.CredentialSubstitutor
import com.ibm.m4d.mover.datastore.kafka.{Kafka, KafkaUtils}
import com.ibm.m4d.mover.datastore.{DataStore, DataStoreBuilder}
import com.ibm.m4d.mover.spark.{SnapshotAggregator, SparkConfig, SparkOutputCounter, SparkUtils}
import com.ibm.m4d.mover.transformation.Transformation
import com.typesafe.config.ConfigFactory
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClientException}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.Charset
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import scala.util.control.NonFatal

/**
  * This is the main application logic of a transfer.
  * Currently the separation between a batch and a stream application is only done for the writing part.
  * About the general workflow:
  * 1. A [[org.apache.spark.sql.DataFrame]] is loaded from a [[DataStore]]
  * 2. Optionally a snapshot is calculated for change data.
  * 3. Transformations are applied
  * 4. Data is written out to a target [[DataStore]].
  */
object Transfer {
  private val logger = LoggerFactory.getLogger(Transfer.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting transfer application...")
    if (args.length != 1) {
      throw new IllegalArgumentException(
        "Please specify the path to the config file as only parameter!"
      )
    }
    val config = CredentialSubstitutor.substituteCredentials(ConfigFactory.parseFile(new File(args(0))).resolve())

    val transferConfig = TransferConfig.apply(config)

    // Load source and target data stores
    val source = DataStoreBuilder.buildSource(config).recover { case NonFatal(e) => throw e }.get
    val target = DataStoreBuilder.buildTarget(config).recover { case NonFatal(e) => throw e }.get

    val transformations = Transformation.loadTransformations(config)

    // Collect all additional spark configurations from source/target (e.g. COS or Kafka specific configuration)
    // and additional spark configuration from transformations (e.g. PME)
    val additionalSparkConfig = source.additionalSparkConfig() ++
      target.additionalSparkConfig() ++
      transformations.foldLeft(Map.empty[String, String])((m, t) => m ++ t.additionalSparkConfig())

    val sparkConf = if (config.hasPath("spark")) SparkConfig.parse(config.getConfig("spark")) else SparkConfig.default
    val spark = SparkUtils.sparkSession("transfer", debug = false, local = false, sparkConfig = sparkConf, additionalOptions = additionalSparkConfig)

    try {
      sparkProcessing(spark, transferConfig, source, target, transformations)
    } catch {
      case NonFatal(e) =>
        val msg = "Moving data failed when writing with message: " + e.getMessage
        writeTerminationMessage(msg)
        throw e
    } finally {
      spark.stop()
    }
    if (!sys.props.get("IS_TEST").contains("true")) {
      sys.exit(0) // Somehow Spark3 does not stop by itself
    }
  }

  def sparkProcessing(
      spark: SparkSession,
      transferConfig: TransferConfig,
      source: DataStore,
      target: DataStore,
      transformations: Seq[Transformation]
  ): Unit = {
    // Read data frame given the data flow type and source data type
    // More information about data flows and data types can be found in [[DataFlowType]], [[DataType]]
    // or in the Mover-matrix.md description.
    val sourceDF = source.read(spark, transferConfig.dataFlowType, transferConfig.sourceDataType)

    // If the source data type is change data and the target data type is log data a snapshot has to be performed for a batch
    // process in order to only filter out the last valid value for a key.
    // If the source data type is log data no snapshot has to be performed as log data is already a snapshot.
    val performSnapshot = transferConfig.sourceDataType == DataType.ChangeData &&
      transferConfig.targetDataType == DataType.LogData &&
      transferConfig.dataFlowType == DataFlowType.Batch

    val df = if (performSnapshot) {
      logger.info("Performing a snapshot of the source...")
      SnapshotAggregator.createSnapshot(sourceDF)
    } else {
      if (transferConfig.dataFlowType == DataFlowType.Stream &&
        transferConfig.sourceDataType == DataType.ChangeData) {
        if (transferConfig.targetDataType == DataType.LogData) {
          logger.warn("WARNING: Source data type is change data and target data type is log data" +
            " in a stream scenario. A proper snapshot cannot be performed in this scenario " +
            "so the data is just mapped to the value. This may lead to duplicate values and loss of nullable information!")
          sourceDF.select("value.*")
        } else {
          // If source and target data type is ChangeData then only
          // keep key and value and continue... TODO maybe Kafka needs to keep partition information?
          sourceDF.select("key", "value")
        }
      } else {
        sourceDF
      }
    }

    transferConfig.dataFlowType match {
      case DataFlowType.Batch =>

        // Apply transformations to the data frame. Depending if it's log data or change data different
        // transform methods have to be called. Transform methods usually return a newly transformed
        // data frame.
        val transformedDF = try {
          transformations
            .foldLeft(df) { case (df, transformation) =>
              transferConfig.sourceDataType match {
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

        val counter = SparkOutputCounter.createAndRegister(spark)
        target.write(transformedDF, transferConfig.targetDataType, transferConfig.writeOperation)

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

        def processMicroBatch(mDF: DataFrame, id: Long): Unit = {

          val newMicroBatch = if (source.isInstanceOf[Kafka] && source.asInstanceOf[Kafka].inferSchema()) {
            mDF.persist()
            // Hack for json Kafka that still needs schema detection
            KafkaUtils.inferJsonSchemaAndConvert(mDF)
          } else {
            mDF
          }

          // Apply transformations to the data frame. Depending if it's log data or change data different
          // transform methods have to be called. Transform methods usually return a newly transformed
          // data frame.
          val transformedDF = try {
            transformations
              .foldLeft(newMicroBatch) { case (df, transformation) =>
                transferConfig.sourceDataType match {
                  case DataType.LogData                          => transformation.transformLogData(df)
                  case DataType.ChangeData if !performSnapshot   => transformation.transformChangeData(df)
                }
              }
          } catch {
            case NonFatal(e) =>
              val msg = "Configuring the transformations failed with message: " + e.getMessage
              writeTerminationMessage(msg)
              throw e
          }

          target.write(transformedDF, transferConfig.targetDataType, transferConfig.writeOperation)

          df.unpersist()
        }

        val stream = df.writeStream.foreachBatch(processMicroBatch _)
          .trigger(transferConfig.trigger)
          .start()

        stream.awaitTermination()
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
