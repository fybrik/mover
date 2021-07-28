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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
  * This test suite tests the fetching and substitution of credentials
  * in a configuration.
  */
class SecretProviderSuite extends AnyFlatSpec with Matchers {
  private val Secret1 = "{\"data\": " +
    "{" +
    "\"accessKey\": \"myak\", " +
    "\"secretKey\": \"mysk\"" +
    "}, " +
    "\"metadata\": {\"" +
    "created_time\": \"2020-04-07T11:07:05.905968973Z\", " +
    "\"deletion_time\": \"\", " +
    "\"destroyed\": false," +
    " \"version\": 1}" +
    "}"

  private val Secret2 = "{\"data\": " +
    "{" +
    "\"password\": \"mypwd\"" +
    "}, " +
    "\"metadata\": {\"" +
    "created_time\": \"2020-04-07T11:07:05.905968973Z\", " +
    "\"deletion_time\": \"\", " +
    "\"destroyed\": false," +
    " \"version\": 1}" +
    "}"

  private val Secret3 = "{\"data\": " +
    "{" +
    "\"key\": \"DEADBEEF\"" +
    "}, " +
    "\"metadata\": {\"" +
    "created_time\": \"2020-04-07T11:07:05.905968973Z\", " +
    "\"deletion_time\": \"\", " +
    "\"destroyed\": false," +
    " \"version\": 1}" +
    "}"

  private val Secret4 = "{" +
    "\"key\": \"DEADBEEF\"" +
    "}"

  private val dispatcher = new Dispatcher() {
    override def dispatch(recordedRequest: RecordedRequest): MockResponse = {
      HttpUrl.parse(recordedRequest.getPath)
      recordedRequest.getRequestUrl.encodedPath() match {
        case "/get-secret" =>
          new MockResponse().setResponseCode(200)
          if (recordedRequest.getRequestUrl.queryParameterNames().size() == 2) {
            val secretPath = recordedRequest.getRequestUrl.queryParameterValues("secret_name").get(0)
            secretPath match {
              case "/v1/hmac/bucket1"    => new MockResponse().setBody(Secret1)
              case "/v1/db2/test2"       => new MockResponse().setBody(Secret2)
              case "/v1/parquet/test"    => new MockResponse().setBody(Secret3)
              case "/v1/parquet/newTest" => new MockResponse().setBody(Secret4)
              case _                     => new MockResponse().setResponseCode(404)
            }
          } else {
            new MockResponse().setResponseCode(500)
          }
        case _ => new MockResponse().setResponseCode(404)
      }
    }
  }

  it should "substitute a configuration" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")
    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "vaultPath" -> "/v1/db2/test2"
    )
    val config = ConfigFactory.empty()
      .withValue("secretProviderURL", ConfigValueFactory.fromAnyRef(url.toString))
      .withValue("secretProviderRole", ConfigValueFactory.fromAnyRef("myrole"))
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef("/v1/hmac/bucket1"))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    substitutedConfig.getString("sub.password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false

    server.getRequestCount shouldBe 2
    val request1 = server.takeRequest
    request1.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fdb2%2Ftest2"
    val request2 = server.takeRequest
    request2.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fhmac%2Fbucket1"

    server.shutdown()
  }

  it should "substitute a configuration (set passwords take priority)" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")
    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "vaultPath" -> "/v1/db2/test2",
      "password" -> "existingPwd"
    )
    val config = ConfigFactory.empty()
      .withValue("secretProviderURL", ConfigValueFactory.fromAnyRef(url.toString))
      .withValue("secretProviderRole", ConfigValueFactory.fromAnyRef("myrole"))
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef("/v1/hmac/bucket1"))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    // Check that existing password will not be overwritten
    substitutedConfig.getString("sub.password") shouldBe "existingPwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false

    server.getRequestCount shouldBe 2
    val request1 = server.takeRequest
    request1.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fdb2%2Ftest2"
    val request2 = server.takeRequest
    request2.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fhmac%2Fbucket1"

    server.shutdown()
  }

  it should "substitute a configuration with new api" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")
    val config = ConfigFactory.empty()
      .withValue("secretProviderURL", ConfigValueFactory.fromAnyRef(url.toString))
      .withValue("secretProviderRole", ConfigValueFactory.fromAnyRef("myrole"))
      .withValue("otherConf", ConfigValueFactory.fromAnyRef("otherValue"))
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef("/v1/parquet/newTest"))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("key") shouldBe "DEADBEEF"
    substitutedConfig.getString("otherConf") shouldBe "otherValue"

    server.getRequestCount shouldBe 1
    val request1 = server.takeRequest
    request1.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fparquet%2FnewTest"

    server.shutdown()
  }

  it should "do stuff" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")

    val s = "{\"source\":{\"dataAsset\":\"NQD60833.SMALL\"," +
      "\"database\":{\"db2URL\":\"jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50001/BLUDB:sslConnection=true;\"," +
      "\"user\":\"\",\"vaultPath\":\"/v1/db2/test2\"}},\"destination\":{\"dataAsset\":\"small.encrypted.parq\"," +
      "\"s3\":{\"endpoint\":\"s3.eu-gb.cloud-object-storage.appdomain.cloud\"," +
      "\"region\":\"eu-gb\",\"bucket\":\"test-bucket\",\"objectKey\":\"small.encrypted.parq\"," +
      "\"dataFormat\":\"parquet\",\"vaultPath\":\"/v1/hmac/bucket1\"}}," +
      "\"transformation\":[{\"name\":\"Encrypt blood group\"," +
      "\"action\":\"Encrypt\",\"columns\":[\"BLOOD_GROUP\"],\"options\":{\"vaultPath\":\"/v1/parquet/test\"}}," +
      "{\"name\":\"Remove columns\",\"action\":\"RemoveColumn\",\"columns\":[\"JOB\",\"SSN\"]}]," +
      "\"image\":\"image-registry.openshift-image-registry.svc:5000/mover/datamover:1.0-SNAPSHOT\"," +
      "\"imagePullPolicy\":\"Always\"," +
      "\"secretProviderURL\":\"" + url.toString + "\"," +
      "\"secretProviderRole\":\"demo\"," +
      "\"successfulJobHistoryLimit\":5," +
      "\"failedJobHistoryLimit\":5}"
    val config = ConfigFactory.parseString(s)

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getConfigList("transformation").get(0).getConfig("options").getString("key") shouldBe "DEADBEEF"
    val sourceConf = substitutedConfig.getConfig("source").getConfig("database")
    val targetConf = substitutedConfig.getConfig("destination").getConfig("s3")
    targetConf.getString("accessKey") shouldBe "myak"
    targetConf.getString("secretKey") shouldBe "mysk"
    // Check that existing password will not be overwritten
    sourceConf.getString("password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false

    server.getRequestCount shouldBe 3
    val request1 = server.takeRequest
    request1.getPath shouldBe "/get-secret?role=demo&secret_name=%2Fv1%2Fhmac%2Fbucket1"
    val request2 = server.takeRequest
    request2.getPath shouldBe "/get-secret?role=demo&secret_name=%2Fv1%2Fdb2%2Ftest2"
    val request3 = server.takeRequest
    request3.getPath shouldBe "/get-secret?role=demo&secret_name=%2Fv1%2Fparquet%2Ftest"

    server.shutdown()
  }

  it should "substitute a configuration by just using the url" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")
    val fullURL1 = HttpUrl.parse(url.toString).newBuilder()
      .addQueryParameter("role", "myrole")
      .addQueryParameter("secret_name", "/v1/hmac/bucket1")
      .build()
    val fullURL2 = HttpUrl.parse(url.toString).newBuilder()
      .addQueryParameter("role", "myrole")
      .addQueryParameter("secret_name", "/v1/db2/test2")
      .build()
    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "vaultPath" -> fullURL2.toString
    )
    val config = ConfigFactory.empty()
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef(fullURL1.toString))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    substitutedConfig.getString("sub.password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false

    server.getRequestCount shouldBe 2
    val request1 = server.takeRequest
    request1.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fhmac%2Fbucket1"
    val request2 = server.takeRequest
    request2.getPath shouldBe "/get-secret?role=myrole&secret_name=%2Fv1%2Fdb2%2Ftest2"

    server.shutdown()
  }

  it should "substitute a configuration by just using the url (configured provider)" in {
    // Mock dispatcher of a secret provider
    val server = new MockWebServer
    server.setDispatcher(dispatcher)
    server.start()
    val url = server.url("/get-secret")
    val fullURL1 = HttpUrl.parse(url.toString).newBuilder()
      .addQueryParameter("role", "myrole")
      .addQueryParameter("secret_name", "/v1/hmac/bucket1")
      .build()
    val fullURL2 = HttpUrl.parse(url.toString).newBuilder()
      .addQueryParameter("role", "myrole")
      .addQueryParameter("secret_name", "/v1/db2/test2")
      .build()
    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "vaultPath" -> fullURL2.toString
    )
    val config = ConfigFactory.empty()
      .withValue("secretProviderURL", ConfigValueFactory.fromAnyRef(url.toString))
      .withValue("secretProviderRole", ConfigValueFactory.fromAnyRef("myrole"))
      .withValue("vaultPath", ConfigValueFactory.fromAnyRef(fullURL1.toString))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val substitutedConfig = CredentialSubstitutor.substituteCredentials(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    substitutedConfig.getString("sub.password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false

    server.getRequestCount shouldBe 2
    val expectedRequestPaths = Seq(
      "/get-secret?role=myrole&secret_name=%2Fv1%2Fhmac%2Fbucket1",
      "/get-secret?role=myrole&secret_name=%2Fv1%2Fdb2%2Ftest2"
    )

    val request1 = server.takeRequest
    Seq(request1.getPath) should contain oneElementOf (expectedRequestPaths)
    val request2 = server.takeRequest
    Seq(request2.getPath) should contain oneElementOf (expectedRequestPaths)

    server.shutdown()
  }
}
