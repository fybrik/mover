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

import java.io.File
import com.typesafe.config.ConfigFactory
import io.fybrik.mover.datastore.{DataStore, DataStoreBuilder}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * This finalizer program is called when a BatchTransfer or StreamTransfer object is deleted.
  * As many targets are supported these are handled by calling the deleteTarget method of the [[DataStore]].
  */
object Finalizer {
  private val logger = LoggerFactory.getLogger("Finalizer")

  def main(args: Array[String]): Unit = {
    logger.info("Finalizer is starting...")
    if (args.length != 1) {
      throw new IllegalArgumentException(
        "Please specify the path to the config file as only parameter!"
      )
    }
    val config = ConfigFactory.parseFile(new File(args(0)))

    DataStoreBuilder.buildTarget(config) match {
      case Success(datastore) => datastore.deleteTarget()
      case Failure(e)         => throw e
    }
    logger.info("Finalizer finished")
  }
}
