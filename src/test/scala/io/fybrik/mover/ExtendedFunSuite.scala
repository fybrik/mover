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

import java.io.{File, IOException}
import java.net.Socket

import org.scalactic.source
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

/**
  * This [[AnyFunSuite]] is able to ignore tests based on a condition.
  * e.g. Run this test only if a certain environment variable is set.
  */
class ExtendedFunSuite extends AnyFunSuite {
  protected def testIf(testName: String, testTags: Tag*)(predicate: => Boolean)(testFun: => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
    if (predicate) {
      test(testName, testTags: _*)(testFun)(pos)
    } else {
      ignore(testName, testTags: _*)(testFun)(pos)
    }
  }

  /**
    * In contrary to the name this will ALWAYS ignore the test!!
    * It's meant as method next to testIfExists so that developers can
    * easily always ignore a test without changing the method signature and parameters too much.
    */
  protected def ignoreIf(testName: String, testTags: Tag*)(predicate: => Boolean)(testFun: => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
    ignore(testName, testTags: _*)(testFun)(pos)
  }

  protected def testIfExists(testName: String, testTags: Tag*)(file: String)(testFun: => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
    if (fileExists(file)) {
      test(testName, testTags: _*)(testFun)(pos)
    } else {
      ignore(testName, testTags: _*)(testFun)(pos)
    }
  }

  /**
    * In contrary to the name this will ALWAYS ignore the test!!
    * It's meant as method next to testIfExists so that developers can
    * easily always ignore a test without changing the method signature and parameters too much.
    */
  protected def ignoreIfExists(testName: String, testTags: Tag*)(file: String)(testFun: => Any /* Assertion */ )(implicit pos: source.Position): Unit = {
    ignore(testName, testTags: _*)(testFun)(pos)
  }

  protected def fileExists(file: String): Boolean = {
    new File(file).exists()
  }

  protected def portListening(port: Int): Boolean = try {
    val socket = new Socket("localhost", port)
    socket.close()
    true
  } catch {
    case e: IOException =>
      false
  }
}
