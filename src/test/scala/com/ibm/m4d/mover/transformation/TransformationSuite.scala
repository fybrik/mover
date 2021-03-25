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

import com.ibm.m4d.mover.datastore.kafka.KafkaUtils
import com.ibm.m4d.mover.spark.{SparkTest, _}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Tests the different behavior of transformation operations on log data and change data.
  */
class TransformationSuite extends AnyFlatSpec with Matchers with SparkTest {
  it should "remove a column in log data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val transformation = new RemoveColumnTransformation("n", Seq("i"), ConfigFactory.empty(), ConfigFactory.empty())

      val transformedDF = transformation.transformLogData(df)

      transformedDF.schema.fieldNames shouldBe Array("s", "d")
      transformedDF.count() shouldBe 3
    }
  }

  it should "remove a column in a change data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClassKV(MyClassKey(1), MyClass(1, "a", 1.0)),
        MyClassKV(MyClassKey(2), MyClass(2, "b", 2.0)),
        MyClassKV(MyClassKey(3), MyClass(3, "c", 3.0))
      ))

      val transformation = new RemoveColumnTransformation("n", Seq("s"), ConfigFactory.empty(), ConfigFactory.empty())

      val transformedDF = transformation.transformChangeData(df)

      transformedDF.schema.fieldNames should have size 2
      transformedDF.schema.fieldNames should contain theSameElementsAs Seq("key", "value")
      val keyStruct = transformedDF.schema.fieldAsStructType("key")
      val valueStruct = transformedDF.schema.fieldAsStructType("value")
      keyStruct.fieldNames shouldBe Array("i")
      valueStruct.fieldNames shouldBe Array("i", "d")
      transformedDF.count() shouldBe 3
    }
  }

  it should "filter rows in log data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val s =
        """
          |transformation = [
          |{
          |  name = "n"
          |  action = "filterrows"
          |  options.clause = "i == 1"
          |}
         ]""".stripMargin

      val config = ConfigFactory.parseString(s)

      val transformation = Transformation.loadTransformations(config).head

      val transformedDF = transformation.transformLogData(df)

      transformedDF.count() shouldBe 1
    }
  }

  it should "fail filter rows in log data if no clause is specified" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val s =
        """
          |transformation = [
          |{
          |  name = "n"
          |  action = "filterrows"
          |}
         ]""".stripMargin

      val config = ConfigFactory.parseString(s)

      val transformation = intercept[IllegalArgumentException](Transformation.loadTransformations(config).head)
    }
  }

  it should "hash column in md5 in log data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val s =
        """
          |transformation = [
          |{
          |  name = "n"
          |  action = "digestcolumns"
          |  options.algo = "md5"
          |  columns = ["s"]
          |}
         ]""".stripMargin

      val config = ConfigFactory.parseString(s)
      val transformation = Transformation.loadTransformations(config).head

      val transformedDF = transformation.transformLogData(df)

      import spark.implicits._

      transformedDF.as[MyClass].collect() shouldBe Array(
        MyClass(1, DigestUtils.md5Hex("a"), 1.0),
        MyClass(2, DigestUtils.md5Hex("b"), 2.0),
        MyClass(3, DigestUtils.md5Hex("c"), 3.0)
      )
    }
  }

  it should "redact a column in log data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val transformation = new RedactColumnsTransformation("n", Seq("s"), ConfigFactory.empty(), ConfigFactory.empty())

      val transformedDF = transformation.transformLogData(df)

      import spark.implicits._

      transformedDF.as[MyClass].collect() shouldBe Array(
        MyClass(1, "XXXXXXXXXX", 1.0),
        MyClass(2, "XXXXXXXXXX", 2.0),
        MyClass(3, "XXXXXXXXXX", 3.0)
      )
    }
  }

  it should "redact a column in a change data" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClassKV(MyClassKey(1), MyClass(1, "a", 1.0)),
        MyClassKV(MyClassKey(2), MyClass(2, "b", 2.0)),
        MyClassKV(MyClassKey(3), MyClass(3, "c", 3.0))
      ))

      val transformation = new RedactColumnsTransformation("n", Seq("s"), ConfigFactory.empty(), ConfigFactory.empty())

      val transformedDF = transformation.transformChangeData(df)
      import spark.implicits._

      transformedDF.schema.fieldNames should have size 2
      transformedDF.schema.fieldNames should contain theSameElementsAs Seq("key", "value")
      val keyStruct = transformedDF.schema.fieldAsStructType("key")
      val valueStruct = transformedDF.schema.fieldAsStructType("value")
      keyStruct.fieldNames shouldBe Array("i")
      valueStruct.fieldNames shouldBe Array("i", "s", "d")
      transformedDF.count() shouldBe 3
      KafkaUtils.mapToValue(transformedDF).as[MyClass].collect() shouldBe Array(
        MyClass(1, "XXXXXXXXXX", 1.0),
        MyClass(2, "XXXXXXXXXX", 2.0),
        MyClass(3, "XXXXXXXXXX", 3.0)
      )
    }
  }

  it should "load a third party transformation for log data (NoopTransformation)" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      ))

      val s =
        """
          |transformation = [
          |{
          |  name = "noop"
          |  action = "class"
          |  class = "com.ibm.m4d.mover.transformation.NoopTransformation"
          |  options.a = "true"
          |  columns = ["s"]
          |}
         ]""".stripMargin

      val config = ConfigFactory.parseString(s)

      val transformation = Transformation.loadTransformations(config).head

      transformation.additionalSparkConfig() should have size 0

      val transformedDF = transformation.transformLogData(df)
      import spark.implicits._
      transformedDF.as[MyClass].collect() shouldBe Array(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )
    }
  }

  it should "load a third party transformation for change data (NoopTransformation)" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClassKV(MyClassKey(1), MyClass(1, "a", 1.0)),
        MyClassKV(MyClassKey(2), MyClass(2, "b", 2.0)),
        MyClassKV(MyClassKey(3), MyClass(3, "c", 3.0))
      ))

      val s =
        """
          |transformation = [
          |{
          |  name = "noop"
          |  action = "class"
          |  class = "com.ibm.m4d.mover.transformation.NoopTransformation"
          |  options.a = "true"
          |  columns = ["s"]
          |}
         ]""".stripMargin

      val config = ConfigFactory.parseString(s)

      val transformation = Transformation.loadTransformations(config).head

      val transformedDF = transformation.transformChangeData(df)
      import spark.implicits._
      KafkaUtils.mapToValue(transformedDF).as[MyClass].collect() shouldBe Array(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0)
      )
    }
  }

  // This test is flaky and thus ignored.
  ignore should "sample a fraction of the dataset" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        MyClass(1, "a", 1.0),
        MyClass(2, "b", 2.0),
        MyClass(3, "c", 3.0),
        MyClass(4, "d", 4.0),
        MyClass(5, "e", 5.0),
        MyClass(6, "f", 6.0),
        MyClass(7, "g", 7.0),
        MyClass(8, "h", 8.0),
        MyClass(9, "i", 9.0),
        MyClass(10, "j", 10.0)
      ))

      val transformation = new SampleRowsTransformation("n", ConfigFactory.empty().withValue("fraction", ConfigValueFactory.fromAnyRef(0.2)), ConfigFactory.empty())

      val transformedDF = transformation.transformLogData(df)

      val n = transformedDF.count()
      (n >= 0 && n < 5) shouldBe true // Spark sample is not always accurate. Give test some room to succeed.
    }
  }

  it should "merge transformations correctly" in {
    val s =
      """
        |transformation = [
        |{
        |  name = "n"
        |  action = "RemoveColumns"
        |  columns = ["c1"]
        |},
        |{
        |  name = "n2"
        |  action = "RemoveColumns"
        |  columns = ["c2"]
        |}
        |,
        |{
        |  name = "n"
        |  action = "RedactColumns"
        |  columns = ["c3"]
        |}]""".stripMargin

    val c = ConfigFactory.parseString(s)
    val ts = Transformation.loadTransformations(c)

    ts should have size 2
    ts(0) shouldBe a[RemoveColumnTransformation]
    ts(1) shouldBe a[RedactColumnsTransformation]

    ts(0).asInstanceOf[RemoveColumnTransformation].columns should contain theSameElementsAs Seq("c1", "c2")

    val seq = Seq(
      new RemoveColumnTransformation("n", Seq("c1"), ConfigFactory.empty(), ConfigFactory.empty()),
      new RemoveColumnTransformation("n", Seq("c2"), ConfigFactory.empty(), ConfigFactory.empty())
    )
    val ts2 = Transformation.merge(seq)

    ts2 should have size 1
  }

  it should "load empty transformations" in {
    val c = ConfigFactory.empty()
    val ts = Transformation.loadTransformations(c)
    ts should have size 0
  }

  it should "fail when loading an unknown transformation" in {
    val s =
      """
        |transformation = [
        |{
        |  name = "n"
        |  action = "RandomAction"
        |  columns = ["c1"]
        |}]""".stripMargin

    val c = ConfigFactory.parseString(s)
    intercept[IllegalArgumentException](Transformation.loadTransformations(c))
  }
}

case class MyClass(i: Integer, s: String, d: Double)
case class MyClassKey(i: Integer)
case class MyClassKV(key: MyClassKey, value: MyClass)
