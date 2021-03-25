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

import com.ibm.m4d.mover.spark.SparkUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{Metadata, StructField}
import com.ibm.m4d.mover.spark._

import scala.collection.JavaConverters._

/**
  * A transformation applies some methods on data frames.
  * This is an abstract class that forces the developer to implement methods separately for log data
  * and change data as the dataframes of these are structured differently.
  */
abstract class Transformation(name: String, columns: Seq[String], options: Config, allConfig: Config) {
  val transAction = "TransformationAction"
  val transName = "TransformationName"

  protected def getOptionOrElse(name: String, default: String): String = {
    if (options.hasPath(name)) {
      options.getString(name)
    } else {
      default
    }
  }

  protected def getOption(name: String): Option[String] = {
    if (options.hasPath(name)) {
      Some(options.getString(name))
    } else {
      None
    }
  }

  def getTransformMeta(
      df: DataFrame,
      columnName: String,
      actionType: String,
      actionName: String,
      struct: Option[String]
  ): Metadata = {
    val dfField = field(df, columnName, struct)
    val avroSchema = SparkUtils.getAvroFromColumn(dfField)
    avroSchema.addProp(transAction, actionType)
    avroSchema.addProp(transName, actionName)
    Metadata.fromJson(avroSchema.toString)
  }

  def columnSet(df: DataFrame, struct: Option[String]): Set[String] = {
    struct match {
      case Some(struct) => df.schema.fieldAsStructType(struct).fieldNames.toSet
      case None         => df.schema.fieldNames.toSet
    }
  }

  def colName(columnName: String, struct: Option[String]): String = {
    struct.map(s => s"$s.$columnName").getOrElse(columnName)
  }

  def field(df: DataFrame, columnName: String, struct: Option[String]): StructField = {
    struct match {
      case Some(struct) => df.schema.fieldAsStructType(struct).field(columnName)
      case None         => df.schema.field(columnName)
    }
  }

  def additionalSparkConfig(): Map[String, String] = Map.empty[String, String]

  /**
    * Transforms a dataframe from a batch source that has the columns
    * at the root level of the dataframe.
    * @param df
    * @return
    */
  def transformLogData(df: DataFrame): DataFrame

  /**
    * Transforms a dataframe from a stream source (like Kafka) that has columns
    * in a 'value' struct and a possible 'key' struct that are at the root level.
    * @param df
    * @return
    */
  def transformChangeData(df: DataFrame): DataFrame

  /**
    * Method that can merge other transformations with this one. The goal is to be able
    * to merge with similar transformations if a transformation only supports one instance
    * of its kind. e.g. if a common configuration has to be done (via SparkConfig) it may be
    * necessary to merge all instances of a transformation to make sure that an aggregated
    * configuration can be build.
    *
    * The default merge is to just return the transformations as they are.
    *
    * @param others all other transformations
    * @return
    */
  def merge(others: Seq[Transformation]): Seq[Transformation] = {
    Seq(this) ++ others
  }
}

object Transformation {
  private val ConstructorArgs = Array(classOf[String], classOf[Seq[String]], classOf[Config], classOf[Config])
  // loads all transformation name to class mappings into a config object
  // class mappings have to be specified in files called 'transformations.conf'.
  private val loadedTransformations = getClass.getClassLoader.getResources("transformations.conf").asScala
    .toSeq.foldLeft(ConfigFactory.empty()) { (conf, url) =>
      val newConf = ConfigFactory.parseURL(url)
      newConf.withFallback(conf)
    }
  def loadTransformations(config: Config): Seq[Transformation] = {
    val transformations = if (config.hasPath("transformation")) {
      val transformations = config.getConfigList("transformation").asScala

      transformations.map { conf =>
        val name = conf.getString("name")
        val columns = conf.getStringList("columns").asScala.toSeq
        val options = if (conf.hasPath("options")) {
          conf.getConfig("options")
        } else {
          ConfigFactory.empty()
        }
        val action = conf.getString("action")
        action.toLowerCase() match {
          // TODO allow for custom transformations via class loading
          case "delete" | "removecolumn" | "removecolumns" => new RemoveColumnTransformation(name, columns, options, config)
          case "digest" | "digestcolumn" | "digestcolumns" => new DigestColumnsTransformation(name, columns, options, config)
          case "redact" | "redactcolumn" | "redactcolumns" => new RedactColumnsTransformation(name, columns, options, config)
          case "filter" | "filterrows"                     => new FilterRowsTransformation(name, options, config)
          case "sample" | "samplerows"                     => new SampleRowsTransformation(name, options, config)
          case "class" =>
            Class.forName(conf.getString("class"))
              .getDeclaredConstructor(ConstructorArgs: _*)
              .newInstance(name, columns, options, config)
              .asInstanceOf[Transformation]
          case _ =>
            if (loadedTransformations.hasPath(action.toLowerCase())) {
              val cl = loadedTransformations.getString(action.toLowerCase())
              Class.forName(cl)
                .getDeclaredConstructor(ConstructorArgs: _*)
                .newInstance(name, columns, options, config)
                .asInstanceOf[Transformation]
            } else {
              throw new IllegalArgumentException(s"No transformation found with name '${action.toLowerCase()}'!")
            }
        }
      }
    } else {
      Seq.empty[Transformation]
    }

    merge(transformations)
  }

  /**
    * This calls the merge function of a transformation on the other transformations to offer
    * merge functionality to transformations that need it.
    * This is a recursive method that calls the merge function of the first transformation on the tail
    * and recursively continues on this.
    *
    * @param transformations transformations to merge
    * @return
    */
  def merge(transformations: Seq[Transformation]): Seq[Transformation] = {
    if (transformations.isEmpty || transformations.size == 1) {
      transformations
    } else {
      val merged = transformations.head.merge(transformations.tail)
      Seq(merged.head) ++ merge(merged.tail)
    }
  }
}
