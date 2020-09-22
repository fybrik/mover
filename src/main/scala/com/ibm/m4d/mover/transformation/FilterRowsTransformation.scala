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

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * This transformations filters rows according to the given filter clause that has to be valid Spark SQL code.
  */
case class FilterRowsTransformation(name: String, options: Config, allConfig: Config) extends Transformation(name, Seq.empty[String], options, allConfig) {
  private val logger = LoggerFactory.getLogger(FilterRowsTransformation.getClass)
  private val clauseOption = getOption("clause")

  override def transformLogData(df: DataFrame): DataFrame = {
    // Apply filter clause on dataframe
    clauseOption.map(clause => df.filter(clause)).getOrElse(df)
  }

  override def transformChangeData(df: DataFrame): DataFrame = {
    clauseOption.map(clause => df.filter(clause)).getOrElse(df)
  }
}
