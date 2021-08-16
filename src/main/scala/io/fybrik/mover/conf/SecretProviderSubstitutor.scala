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

import com.typesafe.config.{Config, ConfigFactory}
import okhttp3.{HttpUrl, OkHttpClient, Request, Response}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Substitutor for values retrieved from the secret provider component of the fybrik.
  * This substitutor uses the key 'vaultPath'.
  * Either a secret provider URL and Role have to be specified or the vaultPath
  * value has to contain a full valid URL to the secret of a secret provider
  */
class SecretProviderSubstitutor(config: Config) extends CredentialSubstitutor {
  import SecretProviderSubstitutor._

  private val client = if ((config.hasPath(SecretProviderUrlKey)) && (config.hasPath(SecretProviderRoleKey))) {
    val secretProviderUrl = config.getString(SecretProviderUrlKey)
    val secretProviderRole = config.getString(SecretProviderRoleKey)
    Some(new SecretProviderClient(secretProviderUrl, secretProviderRole))
  } else {
    None
  }

  override def substitutionKey: String = SecretSubstitutionKey

  override def substituteConfig(config: Config): Config = {
    client match {
      case Some(cl) =>
        val newConf = cl.retrieveConfig(config.getString(SecretSubstitutionKey))
        val newKeys = newConf.entrySet().asScala.map(_.getKey)
        if (newKeys.nonEmpty) {
          logger.info("Found credentials with keys: " + newKeys.mkString(","))
        } else {
          logger.warn("No entries found in " + config.getString(SecretSubstitutionKey))
        }
        for (newKey <- newKeys) {
          if (config.hasPath(newKey)) {
            logger.warn(s"Key '$newKey' already exists and will not be substituted!")
          }
        }

        config.withoutPath(SecretSubstitutionKey)
          .withFallback(newConf)
      case None =>
        val substitutionStr = config.getString(substitutionKey)
        if (substitutionStr.startsWith("http")) {
          SecretProviderClient.retrieveConfig(substitutionStr)
        } else {
          logger.warn("Could not find any secret provider credentials and substitution value is not a URL! Skipping configuration substitution!")
          config
        }
    }
  }
}

object SecretProviderSubstitutor {
  val SecretSubstitutionKey = "vaultPath"
  val SecretProviderUrlKey = "secretProviderURL"
  val SecretProviderRoleKey = "secretProviderRole"
  private val logger = LoggerFactory.getLogger(SecretProviderSubstitutor.getClass)
}

class SecretProviderClient(secretProviderUrl: String, secretProviderRole: String) {
  def retrieveConfig(name: String): Config = {
    val url = if (name.startsWith("http")) {
      name
    } else {
      HttpUrl.parse(secretProviderUrl).newBuilder()
        .addQueryParameter("role", secretProviderRole)
        .addQueryParameter("secret_name", name)
        .build()
        .toString
    }

    SecretProviderClient.retrieveConfig(url)
  }
}

object SecretProviderClient {
  private val logger = LoggerFactory.getLogger("SecretProviderClient")
  private val client = new OkHttpClient()

  def retrieveConfig(urlStr: String): Config = {
    val url = HttpUrl.parse(urlStr)
    logger.info("Requesting secrets from " + url)
    val request = new Request.Builder()
      .url(url)
      .get()
      .build()
    logger.info("Request: " + request.toString)
    var response: Response = null
    try {
      response = client.newCall(request).execute()
      if (response.isSuccessful) {
        val conf = ConfigFactory.parseString(response.body().string())
        if (conf.hasPath("data")) {
          conf.getConfig("data")
        } else {
          conf
        }
      } else {
        logger.warn(s"Received: ${response.code()} ${response.message()} Body: ${response.body().string()}")
        throw new IllegalArgumentException("Could not retrieve url!")
      }
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }
}
