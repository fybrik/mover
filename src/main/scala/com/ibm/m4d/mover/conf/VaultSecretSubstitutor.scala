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
package com.ibm.m4d.mover.conf

import com.ibm.m4d.mover.ConfigException
import com.typesafe.config.{Config, ConfigFactory}
import okhttp3._
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.Charset
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * This substitutor retrieves credentials from Vault using a JWT token as authentication.
  */
class VaultSecretSubstitutor extends CredentialSubstitutor {
  import VaultSecretSubstitutor._

  override def substitutionKey: String = VaultSecretSubstitutor.VaultConfigKey

  override def substituteConfig(config: Config): Config = {
    val vaultConfig = config.getConfig(substitutionKey)

    val newConf = VaultClient.readData(vaultConfig)

    val newKeys = newConf.entrySet().asScala.map(_.getKey).toSeq
    if (newKeys.nonEmpty) {
      logger.info("Found credentials with keys: " + newKeys.mkString(","))
    } else {
      logger.warn("No entries found in Vault!")
    }
    for (newKey <- newKeys) {
      if (config.hasPath(newKey)) {
        logger.warn(s"Key '$newKey' already exists and will not be substituted!")
      }
    }

    // Remove key `vault` from configuration and add new key-value sets as
    // fallback so they won't overwrite already existing values.
    config.withoutPath(VaultSecretSubstitutor.VaultConfigKey)
      .withFallback(newConf)
  }
}

object VaultSecretSubstitutor {
  val VaultConfigKey = "vault"
  private val logger = LoggerFactory.getLogger(VaultSecretSubstitutor.getClass)
}

object VaultClient {
  private val logger = LoggerFactory.getLogger(VaultClient.getClass)

  private val VaultTokenHeaderKey = "X-Vault-Token"
  private val VaultDataField = "data"
  private val VaultAddress = "address"
  private val VaultRole = "role"
  private val VaultAuthPath = "authPath"
  private val DefaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

  private val client = new OkHttpClient()

  /**
    * Given a configuration that contains parameters such as address, role, authPath and secretPath
    * this method will return a configuration of values that are found at the given secretPath in the
    * given Vault instance.
    * @param vaultConfig Vault configuration
    * @return
    */
  def readData(vaultConfig: Config): Config = {
    val address = vaultConfig.getString(VaultAddress)
    val role = vaultConfig.getString(VaultRole)
    val authPath = vaultConfig.getString(VaultAuthPath)
    val jwt = if (vaultConfig.hasPath("jwt")) {
      vaultConfig.getString("jwt")
    } else {
      FileUtils.readFileToString(new File(DefaultServiceAccountTokenPath), Charset.defaultCharset)
    }
    val secretPath = vaultConfig.getString("secretPath")

    login(address, authPath, role, jwt) match {
      case Success(token) =>
        read(address, token, secretPath)
      case Failure(e) =>
        logger.error("Could not retrieve login token!", e)
        ConfigFactory.empty()
    }
  }

  private def login(address: String, authPath: String, role: String, jwt: String): Try[String] = {
    val url = HttpUrl.parse(address + authPath)
    val json = "{\"role\":\"" + role + "\",\"jwt\":\"" + jwt + "\"}"

    val body = RequestBody.create(MediaType.parse("application/json"), json)
    logger.info("Requesting secrets from " + url)
    val request = new Request.Builder()
      .url(url)
      .post(body)
      .build()
    logger.info("Request: " + request.toString)
    var response: Response = null
    try {
      response = client.newCall(request).execute()
      if (response.isSuccessful) {
        val conf = ConfigFactory.parseString(response.body().string())
        if (conf.hasPath("auth.client_token")) {
          Success(conf.getString("auth.client_token"))
        } else {
          Failure(ConfigException("Client token not found in data response!"))
        }
      } else {
        logger.warn(s"Received: ${response.code()} ${response.message()} Body: ${response.body().string()}")
        Failure(ConfigException("Could not retrieve login token!"))
      }
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }

  private def read(address: String, token: String, path: String): Config = {
    val url = HttpUrl.parse(address + path)
    logger.info("Requesting secrets from " + url)
    val request = new Request.Builder()
      .url(url)
      .header(VaultTokenHeaderKey, token)
      .get()
      .build()
    logger.info("Request: " + request.toString)
    var response: Response = null
    try {
      response = client.newCall(request).execute()
      if (response.isSuccessful) {
        val conf = ConfigFactory.parseString(response.body().string())
        if (conf.hasPath(VaultDataField)) {
          conf.getConfig(VaultDataField)
        } else {
          ConfigFactory.empty()
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
