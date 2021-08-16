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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.Metadata
import org.slf4j.LoggerFactory

/**
  * This transformation redacts a value of given columns. This replaces the values of a column with a
  * given redactValue. The default is to replace the content of columns with some XXXX.
  */
class RedactColumnsTransformation(name: String, columns: Seq[String], options: Config, allConfig: Config) extends Transformation(name, columns, options, allConfig) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val redactValue = getOptionOrElse("redactValue", "XXXXXXXXXX")

  override def additionalSparkConfig(): Map[String, String] = Map.empty[String, String]

  private def redactColumns(df: DataFrame, struct: Option[String]): DataFrame = {
    val columnsSet = columnSet(df, struct)

    val newColumns = columnsSet.map { columnName =>
      if (columns.contains(columnName)) {
        logger.info(s"Redacting column $columnName in dataset!")

        val meta: Metadata = getTransformMeta(df, columnName, "Redact", name, struct)
        lit(redactValue).as(columnName, meta)
      } else {
        df(colName(columnName, struct))
      }
    }.toSeq

    struct match {
      case Some(s) => df.withColumn(s, org.apache.spark.sql.functions.struct(newColumns: _*))
      case None    => df.select(newColumns: _*)
    }
  }

  override def transformLogData(df: DataFrame): DataFrame = {
    redactColumns(df, None)
  }

  override def transformChangeData(df: DataFrame): DataFrame = {
    val dfKeyTransformed = redactColumns(df, Some("key"))
    redactColumns(dfKeyTransformed, Some("value"))
  }
}
