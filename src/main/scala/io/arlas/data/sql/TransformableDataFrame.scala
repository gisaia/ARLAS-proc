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
import org.apache.spark.sql.functions.{col, date_format, lit, to_date}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

class TransformableDataFrame(df: DataFrame) {

  def asArlasFormattedData(dataModel: DataModel, doubleColumns: Vector[String] = Vector()): DataFrame = {
    df.enrichWithArlas(new DataFrameFormatter(dataModel, doubleColumns), new WithArlasTimestamp(dataModel))
      .withColumn(arlasPartitionColumn,
                  date_format(to_date(col(dataModel.timestampColumn), dataModel.timeFormat), arlasPartitionFormat).cast(IntegerType))
  }

  def enrichWithArlas(transformers: ArlasTransformer*): DataFrame = {
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
