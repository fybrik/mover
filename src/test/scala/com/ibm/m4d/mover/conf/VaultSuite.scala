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

import com.google.gson.JsonParser
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileNotFoundException
import scala.collection.JavaConverters._

/**
  * Unit test for the [[VaultClient]] and the [[com.ibm.m4d.mover.conf.VaultSecretSubstitutor]].
  */
class VaultSuite extends AnyFlatSpec with Matchers {
  val FakeLogin = "{\"auth\": {\"client_token\":\"mytoken\"}}"
  val WrongLogin = "{\"auth\": {\"no_token\":\"mytoken\"}}"
  val FakeData = "{\"data\": {\"foo\":\"bar\"}}"
  val LoginPath = "/v1/auth/kubernetes/login"
  val SecretPath = "/v1/secret/foo"
  val Jwt = "fakejwt"

  private val vaultDispatcher = new Dispatcher() {
    override def dispatch(recordedRequest: RecordedRequest): MockResponse = {
      HttpUrl.parse(recordedRequest.getPath)
      recordedRequest.getRequestUrl.encodedPath() match {
        case LoginPath =>
          val obj = new JsonParser().parse(recordedRequest.getBody.readUtf8()).getAsJsonObject
          if (obj.getAsJsonPrimitive("jwt").getAsString.equals(Jwt)) {
            new MockResponse().setBody(FakeLogin)
          } else {
            new MockResponse().setResponseCode(401)
          }
        case "/v1/auth/kubernetes/noToken" =>
          new MockResponse().setBody(WrongLogin)
        case SecretPath if recordedRequest.getHeader("X-Vault-Token").equals("mytoken") => new MockResponse().setBody(FakeData)
        case SecretPath => new MockResponse().setResponseCode(403)
        case _ => new MockResponse().setResponseCode(404)
      }
    }
  }

  "VaultClient" should "be able to retrieve some data" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val conf = ConfigFactory.empty()
      .withValue("role", ConfigValueFactory.fromAnyRef(role))
      .withValue("address", ConfigValueFactory.fromAnyRef(addressStr))
      .withValue("authPath", ConfigValueFactory.fromAnyRef(LoginPath))
      .withValue("jwt", ConfigValueFactory.fromAnyRef(jwt))
      .withValue("secretPath", ConfigValueFactory.fromAnyRef(SecretPath))

    val data = VaultClient.readData(conf)

    data.getString("foo") shouldBe "bar"

    server.shutdown()
  }

  "VaultSecretSubstitutor" should "replace a substitute a config correctly" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> jwt,
      "secretPath" -> SecretPath
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = VaultSecretSubstitutor.substitute(config)

    substitutedConfig.getString("foo") shouldBe "bar"
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "replace a substitute a config correctly" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> jwt,
      "secretPath" -> SecretPath
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("foo") shouldBe "bar"
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "not retieve credential if login is wrong" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> "wrongjwt",
      "secretPath" -> SecretPath
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.hasPath("foo") shouldBe false
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "not retrieve credential if auth path corrupt" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> "/v1/auth/kubernetes/noToken",
      "jwt" -> jwt,
      "secretPath" -> SecretPath
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.hasPath("foo") shouldBe false
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "not retrieve credential if secret path is wrong" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> jwt,
      "secretPath" -> "/v1/secret/foo2"
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.hasPath("foo") shouldBe false
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "does not substitute already existing key" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> jwt,
      "secretPath" -> "/v1/secret/foo"
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("foo", ConfigValueFactory.fromAnyRef("original"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("foo") shouldBe "original"
    substitutedConfig.getString("a") shouldBe "b"
    substitutedConfig.hasPath("vault") shouldBe false

    server.shutdown()
  }

  "CredentialSubstitutor" should "try to read token from file if not specified" in {
    val jwt = "fakejwt"
    val role = "test"
    val server = new MockWebServer
    server.setDispatcher(vaultDispatcher)
    server.start()
    val address = server.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    val vaultMap = Map(
      "role" -> role,
      "address" -> "http://localhost:3243",
      "authPath" -> LoginPath,
      "secretPath" -> "/v1/secret/foo"
    )

    val config = ConfigFactory.empty()
      .withValue("a", ConfigValueFactory.fromAnyRef("b"))
      .withValue("foo", ConfigValueFactory.fromAnyRef("original"))
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))

    val substitutedConfig = intercept[FileNotFoundException](CredentialSubstitutor.substituteCredentials(config))

    server.shutdown()
  }
}
