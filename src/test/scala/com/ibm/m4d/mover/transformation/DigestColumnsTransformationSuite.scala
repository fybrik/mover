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
package com.ibm.m4d.mover.transformation

import com.ibm.m4d.mover.spark.SparkTest
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * Tests for [[DigestColumnsTransformation]]
  */
class DigestColumnsTransformationSuite extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks with SparkTest {
  val table = Table(
    "hashAlgorithm",
    "md5",
    "sha1",
    "sha2", // 512 bits
    "crc32",
    "murmur3"
  )

  val expectedResults = Map(
    "md5" -> Seq(
      MyClass(1, "c3fcd3d76192e4007dfb496cca67e13b", 1.0),
      MyClass(2, "07694ef19cf359bfd74556dc0cc7956d", 2.0),
      MyClass(3, "8dda2bba265b7478676bf9526e79c91c", 3.0)
    ),
    "sha1" -> Seq(
      MyClass(1, "32d10c7b8cf96570ca04ce37f2a19d84240d3a89", 1.0),
      MyClass(2, "03d630bd344c60bca6b4e1e96237f371a52fc462", 2.0),
      MyClass(3, "29216e59de907aab9a587c3424f393e6912baed2", 3.0)
    ),
    "sha2" -> Seq(
      MyClass(1, "4dbff86cc2ca1bae1e16468a05cb9881c97f1753bce3619034898faa1aabe429955a1bf8ec483d7421fe3c1646613a59ed5441fb0f321389f77f48a879c7b1f1", 1.0),
      MyClass(2, "6cf15b5b147ed859119df308a3e22a3958ecf1056b9cab135a1ce722ec57f1b65a03983a183141db9cb68817d57fab964be3068fe05eac8ff3d5f24ca34c6524", 2.0),
      MyClass(3, "5d63cd2920fdbf1f67d2a55a7d5b792331f9e21cc9965419170176e98a221d3a68080225f0e781734304c1ef6f162dade36acf463b137e6767416c1c53fa845d", 3.0)
    ),
    "crc32" -> Seq(
      MyClass(1, "1277644989", 1.0),
      MyClass(2, "2558221405", 2.0),
      MyClass(3, "305700432", 3.0)
    ),
    "murmur3" -> Seq(
      MyClass(1, "-1990933474", 1.0),
      MyClass(2, "108557723", 2.0),
      MyClass(3, "2056360226", 3.0)
    ),
  )

  it should "test all digest algorithms for batch transformations" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "abcdefghijklmnopqrstuvwxyz", 1.0),
        MyClass(2, "bcdefghijklmnopqrstuvwxyza", 2.0),
        MyClass(3, "cdefghijklmnopqrstuvwxyzab", 3.0)
      ))

      forAll(table) { algo =>
        val config = ConfigFactory.empty()
          .withValue("algo", ConfigValueFactory.fromAnyRef(algo))

        val transformation = new DigestColumnsTransformation("n", Seq("s"), config, ConfigFactory.empty())

        val transformedDF = transformation.transformLogData(df)

        import spark.implicits._

        val collected = transformedDF.as[MyClass].collect()

        collected should contain theSameElementsAs expectedResults(algo)
      }
    }
  }

  it should "test all digest algorithms for cdc transformations" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClassKV(MyClassKey(1), MyClass(1, "abcdefghijklmnopqrstuvwxyz", 1.0)),
        MyClassKV(MyClassKey(2), MyClass(2, "bcdefghijklmnopqrstuvwxyza", 2.0)),
        MyClassKV(MyClassKey(3), MyClass(3, "cdefghijklmnopqrstuvwxyzab", 3.0))
      ))

      forAll(table) { algo =>
        val config = ConfigFactory.empty()
          .withValue("algo", ConfigValueFactory.fromAnyRef(algo))

        val transformation = new DigestColumnsTransformation("n", Seq("s"), config, ConfigFactory.empty())

        val transformedDF = transformation.transformChangeData(df)

        import spark.implicits._

        val collected = transformedDF.as[MyClassKV].collect()

        val values = collected.map(_.value)

        values should contain theSameElementsAs expectedResults(algo)
      }
    }
  }

  it should "fail for an unknown algorithm" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClassKV(MyClassKey(1), MyClass(1, "abcdefghijklmnopqrstuvwxyz", 1.0)),
        MyClassKV(MyClassKey(2), MyClass(2, "bcdefghijklmnopqrstuvwxyza", 2.0)),
        MyClassKV(MyClassKey(3), MyClass(3, "cdefghijklmnopqrstuvwxyzab", 3.0))
      ))

      val algo = "md5000"
      val config = ConfigFactory.empty()
        .withValue("algo", ConfigValueFactory.fromAnyRef(algo))

      val transformation = new DigestColumnsTransformation("n", Seq("s"), config, ConfigFactory.empty())

      val transformedDF = intercept[IllegalArgumentException](transformation.transformChangeData(df))
    }
  }
}
