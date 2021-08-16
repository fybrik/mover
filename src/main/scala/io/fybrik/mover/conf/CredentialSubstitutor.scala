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

import com.typesafe.config.{Config, ConfigList, ConfigObject, ConfigValueFactory}
import scala.collection.JavaConverters._

/**
  * This is an interface to implement a credential substitution mechanism for a given configuration.
  * The substitute method recursively goes through the configuration in search for a given substitutionKey.
  * Once such a key is found the substituteConfig method is called that has to be implemented by classes
  * that implement this trait. This is supposed to fetch the new configuration that is to be inserted in place.
  */
trait CredentialSubstitutor {
  def substitutionKey: String

  def substituteConfig(config: Config): Config

  /**
    * Recursively substitutes the SecretSubstitutionKey in startConfig with a value that is
    * retrieved from the security provider.
    *
    * @param startConfig config with templates to substitute
    * @return
    */
  def substitute(startConfig: Config): Config = {
    startConfig.root().keySet().asScala.foldLeft(startConfig) { (config, key) =>
      val configObject = config.getValue(key)

      configObject match {
        case obj: ConfigObject if key.equals(substitutionKey) => // Struct key equals to substitution key
          val newConf = substituteConfig(config)

          config.withoutPath(substitutionKey)
            .withFallback(newConf)
        case obj: ConfigObject => // If the key contains a struct recurse one level deeper
          val subConf = substitute(obj.toConfig)
          config.withValue(key, subConf.root())
        case list: ConfigList =>
          val newConfigValues = list.asScala.map {
            case obj: ConfigObject =>
              substitute(obj.toConfig).root()
            case cv =>
              cv
          }

          config.withValue(key, ConfigValueFactory.fromIterable(newConfigValues.asJava))
        case _ if key.equals(substitutionKey) => // Field key equals to substitution key
          val newConf = substituteConfig(config)

          config.withoutPath(substitutionKey)
            .withFallback(newConf)
        case _ => config
      }
    }
  }
}

object CredentialSubstitutor {
  def substituteCredentials(config: Config): Config = {
    val substitutors = Seq(
      VaultSecretSubstitutor,
      new SecretProviderSubstitutor(config),
      new SecretImportSubstitutor
    )

    val substitutedConfig = substitutors.foldLeft(config)(
      (nConf, substitutor) => substitutor.substitute(nConf)
    )

    substitutedConfig
      .withoutPath(SecretProviderSubstitutor.SecretProviderUrlKey)
      .withoutPath(SecretProviderSubstitutor.SecretProviderRoleKey)
  }
}
