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

/**
  * This transformation samples a subset of rows using the specified fraction number.
  * The sampling is handled by Spark.
  */
class SampleRowsTransformation(name: String, options: Config, allConfig: Config) extends Transformation(name, Seq.empty[String], options, allConfig) {
  private val fraction = getOptionOrElse("fraction", "0.25").toDouble

  override def transformLogData(df: DataFrame): DataFrame = {
    df.sample(fraction)
  }

  override def transformChangeData(df: DataFrame): DataFrame = {
    df.sample(fraction)
  }
}
