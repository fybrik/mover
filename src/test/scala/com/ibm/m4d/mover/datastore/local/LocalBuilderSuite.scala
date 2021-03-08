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
package com.ibm.m4d.mover.datastore.local

import com.ibm.m4d.mover.datastore.DataStoreBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * @author ffr@zurich.ibm.com (Florian Froese)
  */
class LocalBuilderSuite extends AnyFlatSpec with Matchers {
  it should "succeed building reader and writer data source" in {
    val config = ConfigFactory.parseResources("local-to-local.conf")
    val sourceStore = new LocalBuilder().buildSource(config)
    val targetStore = new LocalBuilder().buildTarget(config)
    sourceStore should be a 'success
    targetStore should be a 'success
  }

  it should "fail when being called with wrong config" in {
    val config = ConfigFactory.parseResources("local-to-local.conf")
      .withoutPath("source.local")
      .withoutPath("destination.local")
    val sourceStore = new LocalBuilder().buildSource(config)
    val targetStore = new LocalBuilder().buildTarget(config)
    sourceStore should be a 'failure
    targetStore should be a 'failure
  }

  it should "succeed when configuring via DataStoreBuilder" in {
    val config = ConfigFactory.parseResources("local-to-local.conf")
    DataStoreBuilder.registerNewBuilder("local", new LocalBuilder())
    val sourceStore = DataStoreBuilder.buildSource(config)
    val targetStore = DataStoreBuilder.buildTarget(config)
    sourceStore should be a 'success
    targetStore should be a 'success
  }

  it should "fail when DataStore is being called with wrong config" in {
    val config = ConfigFactory.parseResources("local-to-local.conf")
      .withoutPath("source.class")
      .withoutPath("destination.class")
    DataStoreBuilder.unregisterNewBuilder("local")
    val sourceStore = DataStoreBuilder.buildSource(config)
    val targetStore = DataStoreBuilder.buildTarget(config)
    sourceStore should be a 'failure
    targetStore should be a 'failure
  }
}
