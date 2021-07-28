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
package io.fybrik.mover.spark

import com.typesafe.config.Config
import io.fybrik.mover.ConfigUtils

import scala.util.Try

/**
  * Configuration object that stores simple configuration that is given to the mover in a struct.
  * TODO maybe replace this with something else
  */
class SparkConfig(
    val appName: String = "transfer",
    val driverCores: Option[Int] = None,
    val numExecutors: Int = 0,
    val executorCores: Int = 1,
    val executorMemory: String = "4g",
    val image: Option[String] = None,
    val imagePullPolicy: Option[String] = None,
    val shufflePartitions: Int = 10,
    val additionalOptions: Map[String, String] = Map.empty[String, String]
)

object SparkConfig {
  def parse(config: Config): SparkConfig = {
    new SparkConfig(
      if (config.hasPath("appName")) config.getString("appName") else "transfer",
      ConfigUtils.opt(config, "driverCores").map(s => Try(s.toInt).getOrElse(1)),
      if (config.hasPath("numExecutors")) config.getInt("numExecutors") else 0,
      if (config.hasPath("executorCores")) config.getInt("executorCores") else 1,
      if (config.hasPath("executorMemory")) config.getString("executorMemory") else "4g",
      ConfigUtils.opt(config, "image"),
      ConfigUtils.opt(config, "imagePullPolicy"),
      if (config.hasPath("shufflePartitions")) config.getInt("shufflePartitions") else 10,
      if (config.hasPath("additionalOptions")) ConfigUtils.parseMap(config.getConfig("additionalOptions")) else Map.empty[String, String]
    )
  }

  def default: SparkConfig = new SparkConfig()
}
