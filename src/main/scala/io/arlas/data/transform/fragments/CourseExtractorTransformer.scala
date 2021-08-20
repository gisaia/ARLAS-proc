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
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasCourseOrStop, ArlasCourseStates, ArlasMovingStates}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable.ListMap
import scala.collection.mutable.WrappedArray

/**
  * Concatenate all fragments associated to a course to create a single course fragment
  * @param spark Spark Session
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param irregularTempo Value of the irregular tempo. Irregular tempo cannot be the main tempo of a course if another is represented.
  * @param tempoColumns Map with all tempo proportion column associated to tempo value
  *                     (ex: Map("tempo_emission_proportion_tempo_10s" -> "tempo_10s") )
  * @param weightAveragedColumns Columns to weight average over track duration, in aggregations
  */
class CourseExtractorTransformer(spark: SparkSession,
                                 dataModel: DataModel,
                                 propagatedColumns: Seq[String] = Seq(),
                                 weightAveragedColumns: Seq[String] = Seq(),
                                 irregularTempo: String = "tempo_irregular",
                                 tempoColumns: Map[String, String] = Map(),
                                 computePrecision: Boolean = false)
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      irregularTempo,
      tempoColumns,
      weightAveragedColumns,
      computePrecision
    ) {

  override def getAggregationColumn(): String = arlasCourseIdColumn
  override def getAggregateCondition(): Column =
    col(arlasCourseOrStopColumn).notEqual(lit(ArlasCourseOrStop.STOP))
  override def getPropagatedColumns(): Seq[String] =
    Seq(
      dataModel.idColumn,
      arlasCourseDurationColumn,
      arlasCourseStateColumn,
      arlasCourseOrStopColumn,
      arlasMovingStateColumn
    ) ++ propagatedColumns

  val tmpTrailData = "tmp_trail_data"
  val tmpIsVisible = "tmp_is_visible"
  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] =
    ListMap(
      tmpIsVisible -> when(col(arlasTrackVisibilityProportion).geq(0.8), 1.0).otherwise(0.0),
      arlasTrackMotionsVisibleDuration -> sumMotionByVisibility(1.0, arlasTrackDuration, window, tmpIsVisible),
      arlasTrackMotionsVisibleLength -> sumMotionByVisibility(1.0, arlasTrackDistanceGpsTravelled, window, tmpIsVisible),
      arlasTrackMotionsInvisibleDuration -> sumMotionByVisibility(0.0, arlasTrackDuration, window, tmpIsVisible),
      arlasTrackMotionsInvisibleLength -> sumMotionByVisibility(0.0, arlasTrackDistanceGpsTravelled, window, tmpIsVisible),
      arlasTrackPausesDuration -> sum(
        when(col(arlasCourseStateColumn)
               .equalTo(lit(ArlasCourseStates.PAUSE)),
             col(arlasTrackDuration))
          .otherwise(lit(0))).over(window),
      arlasTrackPausesShortNumber -> sum(
        when(col(arlasCourseStateColumn)
               .equalTo(lit(ArlasCourseStates.PAUSE)),
             lit(1))
          .otherwise(lit(0))).over(window).cast(IntegerType),
      arlasTrackPausesLongNumber -> lit(0),
      arlasTrackPausesVisibilityProportion -> sum(
        when(col(arlasCourseStateColumn)
               .equalTo(lit(ArlasCourseStates.PAUSE)),
             col(arlasTrackVisibilityProportion) * col(arlasTrackDuration))
          .otherwise(0))
        .over(window)
        .divide(
          sum(
            when(col(arlasCourseStateColumn)
                   .equalTo(lit(ArlasCourseStates.PAUSE)),
                 col(arlasTrackDuration))
              .otherwise(lit(0))).over(window)),
      arlasTrackMotionVisibilityProportionDuration -> col(arlasTrackMotionsVisibleDuration) / (col(arlasTrackMotionsVisibleDuration) + col(
        arlasTrackMotionsInvisibleDuration)),
      arlasTrackMotionVisibilityProportionDistance -> col(arlasTrackMotionsVisibleLength) / (col(arlasTrackMotionsVisibleLength) + col(
        arlasTrackMotionsInvisibleLength)),
      arlasTrackPausesProportion -> col(arlasTrackPausesDuration) / col(arlasCourseDurationColumn),
      arlasDepartureTimestamp -> first(arlasTrackTimestampStart).over(window),
      arlasArrivalTimestamp -> last(arlasTrackTimestampEnd).over(window),
      arlasTrackPausesTrail -> getPauseTrailUDF(
        collect_list(when(col(arlasCourseStateColumn).equalTo(ArlasCourseStates.PAUSE), col(arlasTrackLocationPrecisionGeometry)))
          .over(window)),
      arlasTrackPausesLocation -> collect_list(
        when(col(arlasCourseStateColumn).equalTo(ArlasCourseStates.PAUSE),
             concat(col(arlasTrackLocationLat), lit(","), col(arlasTrackLocationLon))))
        .over(window),
      arlasTrackMotionsVisibleTrail -> getVisibilityTrailUDF(lit(1.0),
                                                             collect_list(col(tmpIsVisible)).over(window),
                                                             collect_list(col(arlasTrackTrail)).over(window)),
      arlasTrackMotionsInvisibleTrail ->
        getVisibilityTrailUDF(lit(0.0), collect_list(col(tmpIsVisible)).over(window), collect_list(col(arlasTrackTrail)).over(window)),
      tmpTrailData -> getTrailDataUDF(
        collect_list(col(arlasTrackTrail)).over(window),
        collect_list(col(arlasTrackLocationLat)).over(window),
        collect_list(col(arlasTrackLocationLon)).over(window),
        collect_list(col(arlasMovingStateColumn).notEqual(lit(ArlasMovingStates.STILL)))
          .over(window)
      ),
      arlasTrackTrail -> col(tmpTrailData + ".trail"),
      arlasDepartureLocationLon -> col(tmpTrailData + ".departureLon"),
      arlasDepartureLocationLat -> col(tmpTrailData + ".departureLat"),
      arlasArrivalLocationLat -> col(tmpTrailData + ".arrivalLat"),
      arlasArrivalLocationLon -> col(tmpTrailData + ".arrivalLon")
    )

  override def afterTransform(df: DataFrame): DataFrame = {
    val window = Window
      .partitionBy(dataModel.idColumn)
      .orderBy(arlasTrackTimestampStart)

    val whenIsCourseGetNext = (nextCol: String) =>
      when(
        col(arlasCourseOrStopColumn)
          .equalTo(lit(ArlasCourseOrStop.COURSE))
          .and(lead(arlasCourseOrStopColumn, 1).over(window).equalTo(lit(ArlasCourseOrStop.STOP))),
        lead(nextCol, 1).over(window)
    )
    val whenIsCourseGetPrev = (prevCol: String) =>
      when(
        col(arlasCourseOrStopColumn)
          .equalTo(lit(ArlasCourseOrStop.COURSE))
          .and(lag(arlasCourseOrStopColumn, 1).over(window).equalTo(lit(ArlasCourseOrStop.STOP))),
        lag(prevCol, 1).over(window)
    )

    df.withColumn(arlasArrivalStopAfterDuration, whenIsCourseGetNext(arlasTrackDuration))
      .withColumn(arlasArrivalStopAfterLocationLon, whenIsCourseGetNext(arlasTrackLocationLon))
      .withColumn(arlasArrivalStopAfterLocationLat, whenIsCourseGetNext(arlasTrackLocationLat))
      .withColumn(arlasArrivalStopAfterLocationPrecisionValueLat, whenIsCourseGetNext(arlasTrackLocationPrecisionValueLat))
      .withColumn(arlasArrivalStopAfterLocationPrecisionValueLon, whenIsCourseGetNext(arlasTrackLocationPrecisionValueLon))
      .withColumn(arlasArrivalStopAfterLocationPrecisionGeometry, whenIsCourseGetNext(arlasTrackLocationPrecisionGeometry))
      .withColumn(arlasArrivalStopAfterVisibilityProportion, whenIsCourseGetNext(arlasTrackVisibilityProportion))
      .withColumn(arlasDepartureStopBeforeDuration, whenIsCourseGetPrev(arlasTrackDuration))
      .withColumn(arlasDepartureStopBeforeLocationLon, whenIsCourseGetPrev(arlasTrackLocationLon))
      .withColumn(arlasDepartureStopBeforeLocationLat, whenIsCourseGetPrev(arlasTrackLocationLat))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionValueLat, whenIsCourseGetPrev(arlasTrackLocationPrecisionValueLat))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionValueLon, whenIsCourseGetPrev(arlasTrackLocationPrecisionValueLon))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionGeometry, whenIsCourseGetPrev(arlasTrackLocationPrecisionGeometry))
      .withColumn(arlasDepartureStopBeforeVisibilityProportion, whenIsCourseGetPrev(arlasTrackVisibilityProportion))
      .filter(col(arlasCourseOrStopColumn).notEqual(ArlasCourseOrStop.STOP))
      .drop(tmpTrailData, tmpIsVisible)
  }

  def sumMotionByVisibility(visibilityValue: Double,
                            sourceCol: String,
                            window: WindowSpec,
                            visibilityCol: String = arlasTrackVisibilityProportion) =
    sum(
      when(col(arlasCourseStateColumn)
             .equalTo(lit(ArlasCourseStates.MOTION))
             .and(col(visibilityCol).equalTo(lit(visibilityValue))),
           col(sourceCol))
        .otherwise(lit(0))).over(window)

  def getPauseTrailUDF =
    udf((trails: WrappedArray[String]) => GeoTool.lineStringsToSingleMultiLineString(trails.toArray))

  def getTrailDataUDF =
    udf(
      (trails: WrappedArray[String], latitudes: WrappedArray[Double], longitudes: WrappedArray[Double], useTrail: WrappedArray[Boolean]) =>
        GeoTool.getTrailDataFromTrailsAndCoords(trails.toArray, latitudes.toArray, longitudes.toArray, useTrail.toArray))

  def getVisibilityTrailUDF =
    udf(
      (expectedVisibility: Double, visibilityProportions: WrappedArray[Double], trails: WrappedArray[String]) =>
        GeoTool
          .groupTrailsByConsecutiveValue[Double](expectedVisibility, visibilityProportions.toArray, trails.toArray))

  override def transformSchema(schema: StructType): StructType = {
    checkRequiredColumns(schema, Vector(arlasTrackVisibilityProportion))
    checkSchema(schema)
      .add(StructField(arlasTrackMotionsVisibleDuration, LongType, true))
      .add(StructField(arlasTrackMotionsVisibleLength, DoubleType, true))
      .add(StructField(arlasTrackMotionsInvisibleDuration, LongType, true))
      .add(StructField(arlasTrackMotionsInvisibleLength, DoubleType, true))
      .add(StructField(arlasTrackPausesDuration, LongType, true))
      .add(StructField(arlasTrackPausesShortNumber, IntegerType, true))
      .add(StructField(arlasTrackPausesLongNumber, IntegerType, true))
      .add(StructField(arlasTrackPausesVisibilityProportion, DoubleType, true))
      .add(StructField(arlasTrackPausesTrail, StringType, true))
      .add(StructField(arlasTrackPausesLocation, ArrayType(StringType), true))
      .add(StructField(arlasTrackMotionVisibilityProportionDuration, DoubleType, true))
      .add(StructField(arlasTrackMotionVisibilityProportionDistance, DoubleType, true))
      .add(StructField(arlasTrackPausesProportion, DoubleType, true))
      .add(StructField(arlasDepartureTimestamp, LongType, true))
      .add(StructField(arlasArrivalTimestamp, LongType, true))
      .add(StructField(arlasDepartureLocationLat, DoubleType, true))
      .add(StructField(arlasDepartureLocationLon, DoubleType, true))
      .add(StructField(arlasArrivalLocationLat, DoubleType, true))
      .add(StructField(arlasArrivalLocationLon, DoubleType, true))
      .add(StructField(arlasTrackMotionsVisibleTrail, StringType, true))
      .add(StructField(arlasTrackMotionsInvisibleTrail, StringType, true))
      .add(StructField(arlasArrivalStopAfterDuration, LongType, true))
      .add(StructField(arlasArrivalStopAfterLocationLon, DoubleType, true))
      .add(StructField(arlasArrivalStopAfterLocationLat, DoubleType, true))
      .add(StructField(arlasArrivalStopAfterLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasArrivalStopAfterLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasArrivalStopAfterLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasArrivalStopAfterVisibilityProportion, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeDuration, LongType, true))
      .add(StructField(arlasDepartureStopBeforeLocationLon, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationLat, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasDepartureStopBeforeVisibilityProportion, DoubleType, true))
  }
}
