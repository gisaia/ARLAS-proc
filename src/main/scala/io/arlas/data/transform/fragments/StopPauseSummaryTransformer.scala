/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform.fragments

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.immutable.ListMap
import scala.collection.mutable.WrappedArray

/**
  * Concatenate all fragments associated to a stop/pause to create a single stop/pause fragment
  * @param spark                            Spark Session
  * @param dataModel                        Data model containing names of structuring columns (id, lat, lon, time)
  * @param irregularTempo                   value of the irregular tempo (i.a. greater than defined tempos, so there were probably pauses)
  * @param tempoProportionColumns           Map with all tempo proportion column associated to tempo value
  *                                         (ex: Map("tempo_emission_proportion_tempo_10s" -> "tempo_10s") )
  * @param weightAveragedColumns            Columns to weight average over track duration, in aggregations
  */
class StopPauseSummaryTransformer(spark: SparkSession,
                                  dataModel: DataModel,
                                  propagatedColumns: Seq[String] = Seq(),
                                  weightAveragedColumns: Seq[String] = Seq(),
                                  irregularTempo: String = "tempo_irregular",
                                  tempoProportionColumns: Map[String, String] = Map(),
                                  computePrecision: Boolean = false)
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      irregularTempo,
      tempoProportionColumns,
      weightAveragedColumns,
      computePrecision
    ) {

  override def getAggregationColumn(): String = arlasMotionIdColumn

  override def getAggregateCondition(): Column =
    col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)

  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] = {
    val listMapPrecision = if (computePrecision) {
      ListMap(
        arlasTrackLocationPrecisionGeometry -> getStandardDeviationEllipsis(
          col(arlasTrackLocationLat),
          col(arlasTrackLocationLon),
          when(col(arlasTrackLocationPrecisionValueLat).leq(0.001), col(arlasTrackLocationPrecisionValueLat)).otherwise(0.001),
          when(col(arlasTrackLocationPrecisionValueLon).leq(0.001), col(arlasTrackLocationPrecisionValueLon)).otherwise(0.001)
        )
      )
    } else {
      ListMap.empty[String, Column]
    }
    ListMap(
      arlasTrackTrail -> getTrailUDF(
        collect_list(col(arlasTrackTrail)).over(window),
        collect_list(col(arlasTrackLocationLat)).over(window),
        collect_list(col(arlasTrackLocationLon)).over(window),
        collect_list(col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.STILL)))
          .over(window)
      )
      //      arlasTrackTrail -> col(arlasTrackLocationPrecisionGeometry)
    ) ++ listMapPrecision
  }

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
      arlasCourseOrStopColumn,
      arlasCourseStateColumn,
      arlasMotionIdColumn,
      arlasMotionDurationColumn,
      arlasCourseIdColumn,
      arlasCourseDurationColumn,
      dataModel.idColumn
    ) ++ propagatedColumns
  }
}
