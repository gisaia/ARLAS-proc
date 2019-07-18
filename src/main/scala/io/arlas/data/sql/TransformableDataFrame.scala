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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class TransformableDataFrame(df: DataFrame) {

  def asArlasCleanedData(dataModel: DataModel): DataFrame = {
    doPipelineTransform(df,
                        new DataFrameValidator(dataModel),
                        new WithArlasTimestamp(dataModel),
                        new WithArlasPartition(dataModel))
  }

  def asArlasVisibleSequencesFromTimestamp(dataModel: DataModel): DataFrame = {
    doPipelineTransform(
      df,
      new WithArlasVisibilityStateFromTimestamp(dataModel),
      new WithStateIdFromState(dataModel, arlasVisibilityStateColumn, ArlasVisibilityStates.APPEAR.toString, arlasVisibleSequenceIdColumn))
  }

  def asArlasBasicData(dataModel: DataModel, spark: SparkSession): DataFrame = {
    doPipelineTransform(
      df,
      new WithArlasDeltaTimestamp(dataModel, spark, dataModel.idColumn),
      new WithArlasGeopoint(dataModel, spark))
  }

  def asArlasVisibleSequencesThroughTempo(dataModel: DataModel, spark: SparkSession): DataFrame = {
    val tempoDF = doPipelineTransform(
      df,
      new WithArlasTempo(dataModel, spark, dataModel.idColumn),
      new OtherColValueReplacer(dataModel, arlasDeltaTimestampColumn, arlasTempoColumn, null, dataModel.irregularTempo))

    val salvoDF = dataModel.salvoTempoValues.foldLeft(tempoDF) {
                                                (df: DataFrame, salvo: String) => doPipelineTransform(
                                                  df,
                                                  new SameColValueReplacer(dataModel, arlasTempoColumn, salvo, dataModel.salvoTempo)
                                                )
                                              }

    doPipelineTransform(
      salvoDF,
      new WithArlasVisibilityStateFromTempo(dataModel, spark, dataModel.irregularTempo))
  }

  def asArlasMovingState(dataModel: DataModel,
                         spark: SparkSession): DataFrame = {

    doPipelineTransform(
      df,
      new WithSupportGeoPoint(dataModel, spark),
      new WithArlasMovingState(dataModel, spark, dataModel.idColumn),
      new RowRemover(dataModel, "keep", false))
  }

    def asArlasMotions(dataModel: DataModel,
                     spark: SparkSession): DataFrame = {

      doPipelineTransform(
        df,
        new WithArlasMotionIdFromMovingState(dataModel, spark),
        new WithArlasMotionDurationFromId(dataModel))
      .drop("keep")
  }

  def asArlasCourses(dataModel: DataModel,
                     spark: SparkSession): DataFrame = {
    doPipelineTransform(
      df,
      new WithArlasCourseOrStopFromMovingState(dataModel),
      new WithArlasCourseState(dataModel),
      new WithArlasCourseIdFromCourseOrStop(dataModel, spark),
      new WithArlasCourseDurationFromId(dataModel)
      )
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

  def withEmptyCol(colName: String, colType: DataType = StringType) = df.withColumn(colName, lit(null).cast(colType))
}
