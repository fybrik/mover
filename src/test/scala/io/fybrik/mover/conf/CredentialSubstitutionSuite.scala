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

import com.google.gson.JsonParser
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.JavaConverters._

/**
  * Check the order of the credential substitution
  */
class CredentialSubstitutionSuite extends AnyFlatSpec with Matchers {
  val FakeLogin = "{\"auth\": {\"client_token\":\"mytoken\"}}"
  val FakeData = "{\"data\": {\"foo\":\"vaultbar\"}}"
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
        case SecretPath if recordedRequest.getHeader("X-Vault-Token").equals("mytoken") => new MockResponse().setBody(FakeData)
        case SecretPath => new MockResponse().setResponseCode(403)
        case _ => new MockResponse().setResponseCode(404)
      }
    }
  }

  private val Secret1 = "{\"data\": " +
    "{" +
    "\"foo\": \"spbar\"" +
    "}}"

  private val secretProvider = new Dispatcher() {
    override def dispatch(recordedRequest: RecordedRequest): MockResponse = {
      HttpUrl.parse(recordedRequest.getPath)
      recordedRequest.getRequestUrl.encodedPath() match {
        case "/get-secret" =>
          new MockResponse().setResponseCode(200)
          if (recordedRequest.getRequestUrl.queryParameterNames().size() == 2) {
            val secretPath = recordedRequest.getRequestUrl.queryParameterValues("secret_name").get(0)
            secretPath match {
              case "/v1/hmac/bucket1" => new MockResponse().setBody(Secret1)
              case _                  => new MockResponse().setResponseCode(404)
            }
          } else {
            new MockResponse().setResponseCode(500)
          }
        case _ => new MockResponse().setResponseCode(404)
      }
    }
  }

  it should "substitute credentials of vault first" in {
    // Mock dispatcher of a secret provider
    val spServer = new MockWebServer
    spServer.setDispatcher(secretProvider)
    spServer.start()
    val spURL = spServer.url("/get-secret")

    // Mock vault server
    val vaultServer = new MockWebServer
    vaultServer.setDispatcher(vaultDispatcher)
    vaultServer.start()
    val address = vaultServer.url("")
    val addressStr = address.toString.substring(0, address.toString.length - 1) // Get address without tailing "/"

    // Configure config with both services
    val vaultMap = Map(
      "role" -> "test",
      "address" -> addressStr,
      "authPath" -> LoginPath,
      "jwt" -> Jwt,
      "secretPath" -> SecretPath
    )

    val vaultPath = spURL.toString + "?role=myrole&secret_name=%2Fv1%2Fhmac%2Fbucket1"

    val config = ConfigFactory.empty()
      .withValue("vault", ConfigValueFactory.fromMap(vaultMap.asJava))
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef(vaultPath))

    // Substitute credentials
    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("foo") shouldBe "vaultbar"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false
    substitutedConfig.hasPath("vault") shouldBe false
    substitutedConfig.entrySet() should have size 1

    spServer.shutdown()
    vaultServer.shutdown()
  }
}
