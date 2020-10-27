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

import com.ibm.m4d.mover.ConfigUtils
import com.ibm.m4d.mover.datastore._
import com.ibm.m4d.mover.spark.{COSSourceServiceName, COSTargetServiceName}
import com.typesafe.config.Config

import scala.util.Try
import scala.collection.JavaConverters._

/**
  * A builder that knows how to instatiate a [[COS]] object or will return failure objects
  * if exceptions have occurred such as an invalid configuration.
  */
case object COSBuilder extends DataStoreBuilder {
  private def createCOSDataStore(iType: InputType, config: Config): Try[COS] = {
    Try {
      val cosConfig = if (config.hasPath("s3")) {
        config.getConfig("s3")
      } else {
        config.getConfig("cos")
      }

      val serviceName = iType match {
        case Source => COSSourceServiceName
        case Target => COSTargetServiceName
      }

      val fileFormat = FileFormat.parse(cosConfig.getString("dataFormat"))
      val endpoint = cosConfig.getString("endpoint")
      val objectKey = cosConfig.getString("objectKey")
      val bucket = cosConfig.getString("bucket")
      val region = ConfigUtils.opt(cosConfig, "region")
      val apiKey = ConfigUtils.opt(cosConfig, "apiKey")
      val serviceInstance = ConfigUtils.opt(cosConfig, "serviceInstance")
      val accessKey = ConfigUtils.opt(cosConfig, "accessKey").orElse(ConfigUtils.opt(cosConfig, "access_key"))
      val secretKey = ConfigUtils.opt(cosConfig, "secretKey").orElse(ConfigUtils.opt(cosConfig, "secret_key"))
      val partitionBy = if (cosConfig.hasPath("partitionBy")) cosConfig.getStringList("partitionBy").asScala else Seq.empty[String]
      cos.COS(iType, endpoint, bucket, objectKey, fileFormat, region, apiKey, serviceInstance, accessKey, secretKey, partitionBy, serviceName)
    }
  }

  override def buildSource(config: Config): Try[DataStore] = {
    createCOSDataStore(Source, config.getConfig("source"))
  }

  override def buildTarget(config: Config): Try[DataStore] = {
    createCOSDataStore(Target, config.getConfig("destination"))
  }
}
