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
import org.apache.spark.sql.functions.{crc32, hash, md5, sha1, sha2}
import org.slf4j.LoggerFactory

/**
  * This transformations builds the hash of given columns using the specified algorithm
  * and stores it back to the column name.
  */
class DigestColumnsTransformation(name: String, columns: Seq[String], options: Config, allConfig: Config) extends Transformation(name, columns, options, allConfig) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val algo = getOptionOrElse("algo", "md5")

  private def digest(df: DataFrame, struct: Option[String]): DataFrame = {
    val columnsSet = columnSet(df, struct)

    columns.diff(columnsSet.toSeq)
      .foreach(c => logger.warn(s"Column $c in transformation ${name} is not in source! Ignoring..."))

    val newColumns = columnsSet.map { columnName =>
      if (columns.contains(columnName)) {
        logger.info(s"Computing digest of column $columnName in dataset!")

        val fqColumnName = colName(columnName, struct)
        val meta = getTransformMeta(df, columnName, "Digest " + algo.toLowerCase, name, struct)
        algo.toLowerCase() match {
          case "md5"     => md5(df(fqColumnName)).as(columnName, meta)
          case "sha1"    => sha1(df(fqColumnName)).as(columnName, meta)
          case "sha2"    => sha2(df(fqColumnName), 512).as(columnName, meta)
          case "crc32"   => crc32(df(fqColumnName)).as(columnName, meta)
          case "murmur3" => hash(df(fqColumnName)).as(columnName, meta)
          case _         => throw new IllegalArgumentException(s"Algorithm '$algo' is not supported!")
        }

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
    digest(df, None)
  }

  override def transformChangeData(df: DataFrame): DataFrame = {
    val dfKeyTransformed = digest(df, Some("key"))
    digest(dfKeyTransformed, Some("value"))
  }
}
