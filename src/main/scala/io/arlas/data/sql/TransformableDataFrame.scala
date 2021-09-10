/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.sql

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasTransformer}
import io.arlas.data.transform.features._
import io.arlas.data.transform.tools.DataFrameFormatter
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Reformat the input dataframe to make it processable:
  *  - Transform columns name to lower case without special characters
  *  - Drop duplicated observation (same identifier and timestamp)
  *  - Make sure basic columns are available (id, lat, lon, timestamp)
  *  - Store double type column in the correct type
  *  - Create unix timestamp from datetime
  *  - Create arlas partition, used to store data in parquet format
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param doubleColumns Vector containing the names of column containing double type field
  */
class TransformableDataFrame(df: DataFrame) {

  def asArlasFormattedData(dataModel: DataModel, doubleColumns: Vector[String] = Vector()): DataFrame = {
    df.process(
      new DataFrameFormatter(dataModel, doubleColumns),
      new WithArlasTimestamp(dataModel),
      new WithArlasPartition(arlasTimestampColumn)
    )
  }

  def process(transformers: ArlasTransformer*): DataFrame = {
    doPipelineTransform(df, transformers: _*)
  }

  def doPipelineTransform(df: DataFrame, transformers: ArlasTransformer*): DataFrame = {
    val pipeline = new Pipeline()
    pipeline.setStages(transformers.toArray)
    pipeline.fit(df).transform(df)
  }

  def withEmptyCol(colName: String, colType: DataType = StringType) =
    df.withColumn(colName, lit(null).cast(colType))

}
