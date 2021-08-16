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
package io.fybrik.mover.conf

import java.io.File
import java.nio.file.Files

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * This substitutor retrieves credentials from mounted secrets. As secrets in K8s are
  * basically key-value pairs these are loaded in as substituted credentials.
  * The secrets have to be specified with the key 'secretImport'.
  */
class SecretImportSubstitutor extends CredentialSubstitutor {
  import SecretImportSubstitutor._

  override def substitutionKey: String = SecretImportKey

  override def substituteConfig(config: Config): Config = {
    val secretImport = config.getString(substitutionKey)
    val split = secretImport.split(";")
    val secretName = split(0)
    val mappings = if (split.size > 1) { // do mappings
      split.tail.foldLeft(Map.empty[String, String]) { (mapping, newMappingstr) =>
        val mappingSplit = newMappingstr.split("->").map(_.trim)
        if (mappingSplit.length == 2) {
          mapping + (mappingSplit(0) -> mappingSplit(1))
        } else {
          mapping
        }
      }
    } else {
      Map.empty[String, String]
    }
    val dir = new File(secretName)
    val newConf = if (dir.isDirectory) {
      // This secret importer loads files as key/values into the configuration.
      // The key is the name of the file
      // The value is the content of the file
      // This is how secrets are loaded from K8s secrets when mounted to a path in a container
      dir.listFiles().foldLeft(ConfigFactory.empty()) { (conf, file) =>
        val value = new String(Files.readAllBytes(file.toPath))
        val entryName = mappings.getOrElse(file.getName, file.getName)
        conf.withValue(entryName, ConfigValueFactory.fromAnyRef(value))
      }
    } else {
      ConfigFactory.empty()
    }
    val newKeys = newConf.entrySet().asScala.map(_.getKey)
    if (newKeys.nonEmpty) {
      logger.info("Found credentials with keys: " + newKeys.mkString(","))
    } else {
      logger.warn("No entries found in " + config.getString(substitutionKey))
    }
    for (newKey <- newKeys) {
      if (config.hasPath(newKey)) {
        logger.warn(s"Key '$newKey' already exists and will not be substituted!")
      }
    }

    newConf
  }
}

object SecretImportSubstitutor {
  val SecretImportKey = "secretImport"
  private val logger = LoggerFactory.getLogger(SecretImportKey.getClass)
}
