package io.fybrik.mover

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
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.ibm.cloud.objectstorage.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.fybrik.mover.datastore.cos.{COS, COSBuilder}
import io.fybrik.mover.spark._
import io.fybrik.mover.transformation.MyClass
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.nio.file.Files
import scala.collection.JavaConverters._

/**
  * Tests for the S3 interface
  */
class COSAppSuiteS3Mock extends AnyFlatSpec with ForAllTestContainer with TableDrivenPropertyChecks with Matchers with SparkTest {

  override val container: GenericContainer = new GenericContainer(
    "adobe/s3mock:latest",
    Seq(9090, 9191),
    Map(
      "validKmsKeys" -> "ak/sk",
      "initialBuckets" -> "bucket1,bucket2"
    )
  )

  val data = Seq(
    MyClass(1, "a", 1.0),
    MyClass(2, "b", 2.0),
    MyClass(3, "c", 3.0)
  )

  it should "write and read to S3" in {
    System.setProperty("IS_TEST", "true")
    val endpoint = "http://localhost:" + container.mappedPort(9090)

    val fileName = "myfile.parq"
    val config = ConfigFactory.parseMap(Map(
      "source.cos.endpoint" -> endpoint,
      "source.cos.bucket" -> "bucket1",
      "source.cos.objectKey" -> "myfile.parq",
      "source.cos.dataFormat" -> "parquet",
      "source.cos.accessKey" -> "ak",
      "source.cos.secretKey" -> "sk",
      "destination.cos.endpoint" -> endpoint,
      "destination.cos.bucket" -> "bucket2",
      "destination.cos.objectKey" -> fileName,
      "destination.cos.dataFormat" -> "parquet",
      "destination.cos.accessKey" -> "ak",
      "destination.cos.secretKey" -> "sk",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "overwrite",
    ).asJava)

    val tempConf = Files.createTempFile("batch-log-parquet", ".json")
    Files.write(tempConf, config.root().render(ConfigRenderOptions.concise()).getBytes())

    // populate S3 with a file in bucket1
    val sourceCOS = COSBuilder.buildSource(config).get.asInstanceOf[COS]
    withSparkSessionExtra(sourceCOS.additionalSparkConfig()) { spark =>
      val df = spark.createDataFrame(data)
      df.write.parquet(cosUrl("bucket1", fileName))
    }

    // copy the file to bucket2
    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    val targetCOS = COSBuilder.buildTarget(config).get.asInstanceOf[COS]
    withSparkSessionExtra(targetCOS.additionalSparkConfig()) { spark =>
      // Read file from bucket2 and ensure it's the same
      val readDF = spark.read.parquet(cosUrl("bucket2", fileName, COSTargetServiceName))
      import spark.implicits._

      val readData = readDF.as[MyClass].collect()

      readData should contain theSameElementsAs data
    }
    val client = AmazonS3ClientBuilder
      .standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, null))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("ak", "sk")))
      .build

    client.listObjects("bucket2", fileName).getObjectSummaries.size() shouldBe 3

    targetCOS.deleteTarget()

    client.listObjects("bucket2", fileName).getObjectSummaries.size() shouldBe 0
  }

  it should "write and read to S3 as stream" in {
    System.setProperty("IS_TEST", "true")
    val endpoint = "http://localhost:" + container.mappedPort(9090)

    val fileName = "myfile-stream.parq"
    val config = ConfigFactory.parseMap(Map(
      "source.cos.endpoint" -> endpoint,
      "source.cos.bucket" -> "bucket1",
      "source.cos.objectKey" -> "myfile.parq",
      "source.cos.dataFormat" -> "parquet",
      "source.cos.accessKey" -> "ak",
      "source.cos.secretKey" -> "sk",
      "destination.cos.endpoint" -> endpoint,
      "destination.cos.bucket" -> "bucket2",
      "destination.cos.objectKey" -> fileName,
      "destination.cos.dataFormat" -> "parquet",
      "destination.cos.accessKey" -> "ak",
      "destination.cos.secretKey" -> "sk",
      "flowType" -> "stream",
      "readDataType" -> "logdata",
      "writeDataType" -> "logdata",
      "writeOperation" -> "append",
      "triggerInterval" -> "once",
    ).asJava)

    val tempConf = Files.createTempFile("stream-log-parq", ".json")
    Files.write(tempConf, config.root().render(ConfigRenderOptions.concise()).getBytes())

    // populate S3 with a file in bucket1
    val sourceCOS = COSBuilder.buildSource(config).get.asInstanceOf[COS]
    withSparkSessionExtra(sourceCOS.additionalSparkConfig()) { spark =>
      val df = spark.createDataFrame(data)
      df.write.parquet(cosUrl("bucket1", fileName))
    }

    // copy the file to bucket2
    Transfer.main(Array(tempConf.toAbsolutePath.toString))

    val targetCOS = COSBuilder.buildTarget(config).get.asInstanceOf[COS]
    withSparkSessionExtra(targetCOS.additionalSparkConfig()) { spark =>
      // Read file from bucket2 and ensure it's the same
      val readDF = spark.read.parquet(cosUrl("bucket2", fileName, COSTargetServiceName))
      import spark.implicits._

      val readData = readDF.as[MyClass].collect()

      readData should contain theSameElementsAs data
    }
    val client = AmazonS3ClientBuilder
      .standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, null))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("ak", "sk")))
      .build

    client.listObjects("bucket2", fileName).getObjectSummaries.size() shouldBe 3

    targetCOS.deleteTarget()

    client.listObjects("bucket2", fileName).getObjectSummaries.size() shouldBe 0
  }
}
