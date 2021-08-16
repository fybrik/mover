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
package io.fybrik.mover.datastore

import com.typesafe.config.Config
import io.fybrik.mover.datastore.cos.COSBuilder
import io.fybrik.mover.datastore.kafka.KafkaBuilder

import scala.collection.mutable
import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

/**
  * The idea is that a [[DataStore]] and it's [[DataStoreBuilder]] remain abstract so that
  * external sources can be added and tested within a flow in an easy way by implementing these traits/abstract classes.
  */
trait DataStoreBuilder {
  def buildSource(config: Config): Try[DataStore]
  def buildTarget(config: Config): Try[DataStore]
}

object DataStoreBuilder {
  private val registeredDataStoreBuilders = new mutable.HashMap[String, DataStoreBuilder]()
  registeredDataStoreBuilders.put("cos", COSBuilder)
  registeredDataStoreBuilders.put("s3", COSBuilder)
  registeredDataStoreBuilders.put("kafka", KafkaBuilder)

  def buildSource(config: Config): Try[DataStore] = {
    val sourceConfig = config.getConfig("source")
    val datastoreConfig = sourceConfig.root.keySet().asScala.find(key => registeredDataStoreBuilders.contains(key))
    val maybeClass = if (sourceConfig.hasPath("class")) Some(sourceConfig.getString("class")) else None
    // If a builder is given by class name it's looked up by this.
    // This way new datastores can be supported by loading additional jars.

    (datastoreConfig, maybeClass) match {
      case (Some(entry), _) => registeredDataStoreBuilders(entry).buildSource(config)
      case (None, Some(cl)) =>
        Class.forName(cl)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataStoreBuilder]
          .buildSource(config)
      case (None, None) => Failure(new IllegalArgumentException("Could not find any builder!"))
    }
  }

  def buildTarget(config: Config): Try[DataStore] = {
    val targetConfig = config.getConfig("destination")
    val datastoreConfig = targetConfig.root.keySet().asScala.find(key => registeredDataStoreBuilders.contains(key))
    val maybeClass = if (targetConfig.hasPath("class")) Some(targetConfig.getString("class")) else None
    // If a builder is given by class name it's looked up by this.
    // This way new datastores can be supported by loading additional jars.

    (datastoreConfig, maybeClass) match {
      case (Some(entry), _) => registeredDataStoreBuilders(entry).buildTarget(config)
      // TODO Maybe pick [[DataStoreBuilder]] by class name?
      case (None, Some(cl)) =>
        Class.forName(cl)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataStoreBuilder]
          .buildTarget(config)
      case (None, None) => Failure(new IllegalArgumentException("Could not find any builder!"))
    }
  }

  def registerNewBuilder(shortName: String, builder: DataStoreBuilder): Unit = {
    registeredDataStoreBuilders.put(shortName, builder)
  }

  def unregisterNewBuilder(shortName: String): Unit = {
    registeredDataStoreBuilders.remove(shortName)
  }
}
