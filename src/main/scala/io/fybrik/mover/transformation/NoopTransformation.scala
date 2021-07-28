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
  * Ths is an example transformation that does nothing and is there for reference and testing.
  */
class NoopTransformation(name: String, columns: Seq[String], options: Config, allConfig: Config) extends Transformation(name, columns, options, allConfig) {
  /**
    * Transforms a dataframe from a batch source that has the columns
    * at the root level of the dataframe.
    *
    * @param df
    * @return
    */
  override def transformLogData(df: DataFrame): DataFrame = df

  /**
    * Transforms a dataframe from a stream source (like Kafka) that has columns
    * in a 'value' struct and a possible 'key' struct that are at the root level.
    *
    * @param df
    * @return
    */
  override def transformChangeData(df: DataFrame): DataFrame = df
}
