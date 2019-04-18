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
import io.arlas.data.transform._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class TransformableDataFrame(df: DataFrame) {

  def asArlasCleanedData(dataModel: DataModel): DataFrame = {
    doPipelineTransform(df,
                        new DataFrameValidator(dataModel),
                        new WithArlasTimestamp(dataModel),
                        new WithArlasPartition(dataModel))
  }

  def asArlasTimeSeries(dataModel: DataModel): DataFrame = {
    doPipelineTransform(df,
                        new WithEmptyArlasTimeSerieId(dataModel),
                        new WithArlasVisibilityState(dataModel),
                        new ArlasTimeSerieIdFiller(dataModel))
  }

  def asArlasMotions(dataModel: DataModel): DataFrame = {
    doPipelineTransform(df,
                        new WithArlasMovingState(dataModel),
                        new ArlasStillSimplifier(dataModel),
                        new WithArlasMotionId(dataModel),
                        new WithArlasMoveSimplifier(dataModel))
  }

  def asArlasResampledMotions(dataModel: DataModel, spark: SparkSession): DataFrame = {
    doPipelineTransform(df, new ArlasResampler(dataModel, arlasMotionIdColumn, spark))
  }

  def enrichWithArlas(transformers: ArlasTransformer*): DataFrame = {
    doPipelineTransform(df, transformers: _*)
  }

  def doPipelineTransform(df: DataFrame, transformers: ArlasTransformer*): DataFrame = {
    val pipeline = new Pipeline()
    pipeline.setStages(transformers.toArray)
    pipeline.fit(df).transform(df)
  }
}

// Classes below do not transform input data
// Consider them as interfaces to describe how code may be organized
// TODO implement following ArlasTransformers

class WithArlasVisibilityState(dataModel: DataModel) extends ArlasTransformer(dataModel) {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF
}
class WithArlasMovingState(dataModel: DataModel) extends ArlasTransformer(dataModel) {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF
}
class ArlasStillSimplifier(dataModel: DataModel) extends ArlasTransformer(dataModel) {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF
}
class WithArlasMotionId(dataModel: DataModel) extends ArlasTransformer(dataModel) {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF
}
class WithArlasMoveSimplifier(dataModel: DataModel) extends ArlasTransformer(dataModel) {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF
}
