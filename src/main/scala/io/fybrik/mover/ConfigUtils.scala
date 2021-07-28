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
package io.fybrik.mover

import com.typesafe.config._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * This is a configuration utility that helps to read configuration files in typesafe [[Config]] format
  * and is able to offer some secret substitution mechanisms that look up credentials in other secrets
  * and injects them into the confiuration.
  */
object ConfigUtils {
  val SecretSubstitutionKey = "vaultPath"
  val SecretImportKey = "secretImport"

  private val logger = LoggerFactory.getLogger(ConfigUtils.getClass)

  def opt(config: Config, key: String): Option[String] = {
    if (config.hasPath(key)) {
      Some(config.getString(key))
    } else {
      None
    }
  }

  def parseMap(config: Config): Map[String, String] = {
    config.entrySet().asScala.map(e => (e.getKey, e.getValue.unwrapped().toString)).toMap
  }
}

case class ConfigException(m: String) extends Exception(m)
