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
package com.ibm.m4d.mover.datastore.local

import com.ibm.m4d.mover.datastore.cos.FileFormat
import com.ibm.m4d.mover.datastore.{DataStore, DataStoreBuilder, Source}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * A builder that knows how to instatiate a [[Local]] object or will return failure objects
  * if exceptions have occurred such as an invalid configuration.
  */
class LocalBuilder extends DataStoreBuilder {
  override def buildSource(config: Config): Try[DataStore] = {
    if (config.hasPath("source.local")) {
      val localConfig = config.getConfig("source.local")
      val fileFormat = FileFormat.parse(localConfig.getString("dataFormat"))
      val path = localConfig.getString("path")
      val partitionBy = if (localConfig.hasPath("partitionBy")) localConfig.getStringList("partitionBy").asScala else Seq.empty[String]
      Success(new Local(Source, path, fileFormat, partitionBy))
    } else {
      Failure(new IllegalArgumentException("Could not find local datastore configuration!"))
    }
  }

  override def buildTarget(config: Config): Try[DataStore] = {
    if (config.hasPath("destination.local")) {
      val localConfig = config.getConfig("destination.local")
      val fileFormat = FileFormat.parse(localConfig.getString("dataFormat"))
      val path = localConfig.getString("path")
      val partitionBy = if (localConfig.hasPath("partitionBy")) localConfig.getStringList("partitionBy").asScala else Seq.empty[String]
      Success(new Local(Source, path, fileFormat, partitionBy))
    } else {
      Failure(new IllegalArgumentException("Could not find local datastore configuration!"))
    }
  }
}
