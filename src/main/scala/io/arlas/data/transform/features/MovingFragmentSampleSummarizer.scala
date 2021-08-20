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

package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.fragments.FragmentSummaryTransformer
import io.arlas.data.transform.{ArlasMovingStates, VisibilityChange}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.immutable.ListMap
import scala.collection.mutable.WrappedArray

/**
  * @param spark
  * @param dataModel             Data model containing names of structuring columns (id, lat, lon, time)
  * @param irregularTempo        value of the irregular tempo (i.a. greater than defined tempos, so there were probably pauses)
  * @param tempoPropotionColumns
  * @param weightAveragedColumns columns to weight average over track duration, in aggregations
  */
class MovingFragmentSampleSummarizer(spark: SparkSession,
                                     dataModel: DataModel,
                                     irregularTempo: String = "irregular_tempo",
                                     tempoPropotionColumns: Map[String, String] = Map(),
                                     weightAveragedColumns: Seq[String] = Seq())
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      irregularTempo,
      tempoPropotionColumns,
      weightAveragedColumns
    ) {

  override def getAggregationColumn(): String = arlasTrackSampleId

  override def getAggregateCondition(): Column =
    col(arlasMovingStateColumn).equalTo(ArlasMovingStates.MOVE)

  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] =
    ListMap(
      arlasTrackTrail -> getTrailUDF(
        collect_list(col(arlasTrackTrail)).over(window),
        collect_list(col(arlasTrackLocationLat)).over(window),
        collect_list(col(arlasTrackLocationLon)).over(window),
        collect_list(col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.MOVE)))
          .over(window)
      ),
      arlasTrackVisibilityChange ->
        when(
          (lit(VisibilityChange.APPEAR).equalTo(first(arlasTrackVisibilityChange).over(window))
            || lit(VisibilityChange.APPEAR_DISAPPEAR).equalTo(first(arlasTrackVisibilityChange).over(window)))
            && (lit(VisibilityChange.DISAPPEAR).equalTo(last(arlasTrackVisibilityChange).over(window))
              || lit(VisibilityChange.APPEAR_DISAPPEAR).equalTo(last(arlasTrackVisibilityChange).over(window))),
          VisibilityChange.APPEAR_DISAPPEAR
        ).when(
            lit(VisibilityChange.APPEAR).equalTo(first(arlasTrackVisibilityChange).over(window))
              || lit(VisibilityChange.APPEAR_DISAPPEAR).equalTo(first(arlasTrackVisibilityChange).over(window)),
            VisibilityChange.APPEAR
          )
          .when(
            lit(VisibilityChange.DISAPPEAR).equalTo(last(arlasTrackVisibilityChange).over(window))
              || lit(VisibilityChange.APPEAR_DISAPPEAR).equalTo(last(arlasTrackVisibilityChange).over(window)),
            VisibilityChange.DISAPPEAR
          )
          .otherwise(null),
      arlasTrackVisibilityProportion -> mean(col(arlasTrackVisibilityProportion)).over(window)
    )

  def getTrailUDF =
    udf(
      (trails: WrappedArray[String], latitudes: WrappedArray[Double], longitudes: WrappedArray[Double], useTrail: WrappedArray[Boolean]) =>
        GeoTool.getTrailDataFromTrailsAndCoords(trails.toArray, latitudes.toArray, longitudes.toArray, useTrail.toArray) match {
          case Some(trailData) => Some(trailData.trail)
          case None            => None
      })

  override def getPropagatedColumns(): Seq[String] = {
    Seq(
      arlasMovingStateColumn,
      arlasMotionDurationColumn,
      arlasMotionIdColumn,
      dataModel.idColumn
    )
  }
}
