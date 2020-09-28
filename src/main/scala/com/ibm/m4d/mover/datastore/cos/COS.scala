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
package com.ibm.m4d.mover.datastore.cos

import com.ibm.cloud.objectstorage.ClientConfiguration
import com.ibm.cloud.objectstorage.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary
import com.ibm.cloud.objectstorage.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ibm.m4d.mover.datastore.{DataStore, InputType}
import com.ibm.m4d.mover.spark.{COSTargetServiceName, cosUrl}
import com.ibm.m4d.mover.{DataFlowType, DataType, MetaData, WriteOperation}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * [[DataStore]] class for a Cloud object store. This is configuring the Stocator library underneath
  * and is compatible to other S3 systems.
  */
case class COS(
    iType: InputType,
    endpoint: String,
    bucket: String,
    objectKey: String,
    fileFormat: FileFormat,
    region: Option[String] = None,
    apiKey: Option[String] = None,
    serviceInstance: Option[String] = None,
    accessKey: Option[String] = None,
    secretKey: Option[String] = None,
    partitionBy: Seq[String] = Seq.empty[String],
    serviceName: String = COSTargetServiceName
) extends DataStore(iType) {
  private val logger = LoggerFactory.getLogger(getClass)

  override def additionalSparkConfig(): Map[String, String] = {
    Map(
      "spark.hadoop.fs.stocator.scheme.list" -> "cos",
      "spark.hadoop.fs.cos.impl" -> "com.ibm.stocator.fs.ObjectStoreFileSystem",
      "spark.hadoop.fs.stocator.cos.impl" -> "com.ibm.stocator.fs.cos.COSAPIClient",
      "spark.hadoop.fs.stocator.cos.scheme" -> "cos",
      "spark.hadoop.fs.cos." + serviceName + ".endpoint" -> endpoint,
      "spark.hadoop.fs.cos.threads.max" -> "30",
      "spark.hadoop.fs.cos.threads.keepalivetime" -> "360",
      "spark.hadoop.fs.cos.connection.maximum" -> "20000",
      "spark.hadoop.fs.cos.client.execution.timeout" -> Int.MaxValue.toString,
      "spark.hadoop.fs.cos.connection.timeout" -> Int.MaxValue.toString,
      "spark.hadoop.fs.cos.client.request.timeout" -> Int.MaxValue.toString,
      "spark.hadoop.fs.cos.multipart.threshold" -> "262144000",
      "spark.hadoop.fs.cos.fast.upload" -> "true"
    ) ++
      accessKey.map(ak => "spark.hadoop.fs.cos." + serviceName + ".access.key" -> ak) ++
      secretKey.map(sk => "spark.hadoop.fs.cos." + serviceName + ".secret.key" -> sk)
  }

  override def sourceMetadata(): Option[MetaData] = ???

  override def read(spark: SparkSession, dataFlowType: DataFlowType, dataType: DataType): DataFrame = {
    val path = cosUrl(bucket, objectKey, serviceName)
    dataFlowType match {
      case DataFlowType.Batch =>
        fileFormat.read(spark, path)
      case DataFlowType.Stream =>
        fileFormat.readStream(spark, path)
    }
  }

  override def write(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): Unit = {
    val path = cosUrl(bucket, objectKey, serviceName)
    fileFormat.write(df, path, writeOperation, partitionBy)
  }

  override def writeStream(df: DataFrame, targetDataType: DataType, writeOperation: WriteOperation): DataStreamWriter[Row] = {
    val path = cosUrl(bucket, objectKey, serviceName)
    fileFormat.writeStream(df, path, writeOperation, partitionBy)
  }

  private def cosClient(): AmazonS3 = {

    val clientConfig = new ClientConfiguration()
      .withRequestTimeout(5000)

    clientConfig.setUseTcpKeepAlive(true)

    val credentials = (accessKey, secretKey, apiKey, serviceInstance) match {
      case (None, None, Some(ak), Some(si)) =>
        new BasicIBMOAuthCredentials(ak, si)
      case (Some(ak), Some(sk), None, None) =>
        new BasicAWSCredentials(ak, sk)
      case (_, _, _, _) => throw new IllegalArgumentException("Either 'accessKey' and 'secretKey' or" +
        " 'apiKey' and 'serviceInstance' have to be provided!")
    }

    AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region.orNull))
      .withPathStyleAccessEnabled(true)
      .withClientConfiguration(clientConfig)
      .build()
  }

  override def deleteTarget(): Unit = {
    val client = cosClient()

    val objects = try {
      client.listObjects(bucket, objectKey).getObjectSummaries.asScala
    } catch {
      case NonFatal(e) =>
        logger.error("Could not list objects!", e)
        Seq.empty[S3ObjectSummary]
    }

    objects.foreach { obj =>
      logger.info(s"Deleting object ${obj.getKey}...")
      try {
        client.deleteObject(bucket, obj.getKey)
      } catch {
        case NonFatal(e) => logger.error(s"Could not delete object ${obj.getKey}!", e)
      }
    }
  }
}
