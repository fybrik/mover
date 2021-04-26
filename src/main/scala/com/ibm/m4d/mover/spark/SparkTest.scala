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
package com.ibm.m4d.mover.spark

import java.util.TimeZone

import com.ibm.m4d.mover.datastore.cos.COS
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * This trait can be mixed into scala tests in order to retrieve a [[SparkSession]].
  */
trait SparkTest {
  private def conf(tz: String = "UTC") = {
    TimeZone.setDefault(TimeZone.getTimeZone(tz))
    new SparkConf()
      .setAppName("testing")
      .setMaster("local[2]")
      .set("spark.driver.bindAddress", "localhost")
      .set("spark.ui.enabled", "false")
      .set("spark.eventLog.enabled", "false")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.sql.parquet.writeLegacyFormat", "true")
      .set("spark.sql.session.timeZone", tz)
      .set("spark.sql.streaming.schemaInference", "true")
      .set("spark.sql.streaming.checkpointLocation", "/tmp/datamover")
  }

  /**
    * Creates a [[org.apache.spark.sql.SparkSession]] and starts it.
    * Afterwards it runs given method and stops the spark context afterwards.
    *
    * @param f test method
    */
  def withSparkSession(f: SparkSession => Unit) {
    val spark = SparkSession.builder().config(conf()).getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  /**
    * Creates a [[org.apache.spark.sql.SparkSession]] and starts it.
    * Afterwards it runs given method and stops the spark context afterwards.
    *
    * @param f test method
    */
  def withSparkSessionExtra(additional: Map[String, String])(f: SparkSession => Unit) {
    val sparkConf = additional.foldLeft(conf())((conf, tup) => conf.set(tup._1, tup._2))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  /**
    * Creates a [[org.apache.spark.sql.SparkSession]] and starts it.
    * Afterwards it runs given method and stops the spark context afterwards.
    *
    * @param f test method
    */
  def withSparkSessionWithTZ(tz: String = "UTC")(f: SparkSession => Unit) {
    val spark = SparkSession.builder().config(conf(tz)).getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }
}
