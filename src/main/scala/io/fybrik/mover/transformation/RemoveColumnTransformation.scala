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
package io.fybrik.mover.transformation

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
  * This transformations removes a column from an existing data frame.
  */
class RemoveColumnTransformation(val name: String, val columns: Seq[String], val options: Config, val allConfig: Config) extends Transformation(name, columns, options, allConfig) {
  private val logger = LoggerFactory.getLogger(getClass)
  override def additionalSparkConfig(): Map[String, String] = Map.empty[String, String]

  private def removeColumn(df: DataFrame, struct: Option[String]): DataFrame = {
    import io.fybrik.mover.spark._
    val columnsSet = struct match {
      case Some(struct) => df.schema.fieldAsStructType(struct).fieldNames.toSet
      case None         => df.schema.fieldNames.toSet
    }

    columns.diff(columnsSet.toSeq)
      .foreach(c => logger.warn(s"Column $c in transformation ${name} is not in source! Ignoring..."))

    // Build new columns
    val newColumns = columnsSet.flatMap { columnName =>
      if (columns.contains(columnName)) {
        logger.info(s"Removing column $columnName from dataset!")
        None
      } else {
        Some(df(colName(columnName, struct)))
      }
    }.toSeq

    struct match {
      case Some(s) => df.withColumn(s, org.apache.spark.sql.functions.struct(newColumns: _*))
      case None    => df.select(newColumns: _*)
    }
  }

  /**
    * Transforms a dataframe from a batch source that has the columns
    * at the root level of the dataframe.
    *
    * @param df
    * @return
    */
  override def transformLogData(df: DataFrame): DataFrame = {
    removeColumn(df, None)
  }

  /**
    * Transforms a dataframe from a stream source (like Kafka) that has columns
    * in a 'value' struct and a possible 'key' struct that are at the root level.
    *
    * @param df
    * @return
    */
  override def transformChangeData(df: DataFrame): DataFrame = {
    val dfKeyTransformed = removeColumn(df, Some("key"))
    removeColumn(dfKeyTransformed, Some("value"))
  }

  override def merge(others: Seq[Transformation]): Seq[Transformation] = {
    val (sameOperations, otherOperations) = others.partition(_.isInstanceOf[RemoveColumnTransformation])
    val newTransformation = sameOperations
      .map(_.asInstanceOf[RemoveColumnTransformation])
      .foldLeft(this)((op, t) => new RemoveColumnTransformation(op.name, op.columns ++ t.columns, op.options, op.allConfig))

    Seq(newTransformation) ++ otherOperations
  }
}
