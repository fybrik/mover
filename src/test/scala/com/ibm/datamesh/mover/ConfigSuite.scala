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
package com.ibm.datamesh.mover

import java.nio.file.{Files, Paths}

import com.ibm.datamesh.mover.conf.SecretImportSubstitutor
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
  * Tests the secret substitution done in the [[ConfigUtils]].
  */
class ConfigSuite extends AnyFlatSpec with Matchers {
  it should "substitute config correctly for secretImport" in {
    val testFolder = Files.createTempDirectory("sub-test")
    testFolder.toFile.deleteOnExit()
    Files.write(Paths.get(testFolder.toAbsolutePath.toString, "password"), "mypwd".getBytes)

    val testFolder2 = Files.createTempDirectory("sub-test2")
    testFolder2.toFile.deleteOnExit()
    Files.write(Paths.get(testFolder2.toAbsolutePath.toString, "accessKey"), "myak".getBytes)
    Files.write(Paths.get(testFolder2.toAbsolutePath.toString, "secretKey"), "mysk".getBytes)

    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "secretImport" -> testFolder.toString
    )
    val config = ConfigFactory.empty()
      .withValue("secretImport", ConfigValueFactory.fromAnyRef(testFolder2.toString))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val secretImportSubstitutor = new SecretImportSubstitutor
    val substitutedConfig = secretImportSubstitutor.substitute(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    substitutedConfig.getString("sub.password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false
  }

  it should "substitute config correctly for secretImport with mapping" in {
    val testFolder = Files.createTempDirectory("sub-test")
    testFolder.toFile.deleteOnExit()
    Files.write(Paths.get(testFolder.toAbsolutePath.toString, "password"), "mypwd".getBytes)

    val testFolder2 = Files.createTempDirectory("sub-test2")
    testFolder2.toFile.deleteOnExit()
    Files.write(Paths.get(testFolder2.toAbsolutePath.toString, "ak"), "myak".getBytes)
    Files.write(Paths.get(testFolder2.toAbsolutePath.toString, "sk"), "mysk".getBytes)

    val subConfig = Map(
      "a" -> "1",
      "b" -> "2",
      "secretImport" -> testFolder.toString
    )
    val config = ConfigFactory.empty()
      .withValue("secretImport", ConfigValueFactory.fromAnyRef(testFolder2.toString + ";ak -> accessKey;sk -> secretKey"))
      .withValue("sub", ConfigValueFactory.fromMap(subConfig.asJava))

    val secretImportSubstitutor = new SecretImportSubstitutor
    val substitutedConfig = secretImportSubstitutor.substitute(config)

    substitutedConfig.getString("accessKey") shouldBe "myak"
    substitutedConfig.getString("secretKey") shouldBe "mysk"
    substitutedConfig.getString("sub.password") shouldBe "mypwd"
    substitutedConfig.hasPath("secretProviderURL") shouldBe false
    substitutedConfig.hasPath("secretProviderRole") shouldBe false
  }
}
