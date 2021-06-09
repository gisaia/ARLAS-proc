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

package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasCourseOrStop, ArlasCourseStates}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable.ListMap
import scala.collection.mutable.WrappedArray

class CourseExtractorTransformer(spark: SparkSession,
                                 dataModel: DataModel,
                                 standardDeviationEllipsisNbPoint: Int,
                                 irregularTempo: String,
                                 tempoColumns: Map[String, String],
                                 weightAveragedColumns: Seq[String])
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      standardDeviationEllipsisNbPoint,
      irregularTempo,
      tempoColumns,
      weightAveragedColumns
    ) {

  override def getAggregationColumn(): String = arlasCourseIdColumn
  override def getAggregateCondition(): Column =
    col(arlasCourseOrStopColumn).notEqual(lit(ArlasCourseOrStop.STOP))
  override def getPropagatedColumns(): Seq[String] = Seq(
    dataModel.idColumn,
    arlasCourseDurationColumn,
    arlasCourseStateColumn,
    arlasCourseOrStopColumn,
    arlasMovingStateColumn
  )

  val tmpTrailData = "tmp_trail_data"

  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] =
    ListMap(
      arlasTrackMotionsVisibleDuration -> sumMotionByVisibility(1.0, arlasTrackDuration, window),
      arlasTrackMotionsVisibleLength -> sumMotionByVisibility(1.0, arlasTrackDistanceGpsTravelled, window),
      arlasTrackMotionsInvisibleDuration -> sumMotionByVisibility(0.0, arlasTrackDuration, window),
      arlasTrackMotionsInvisibleLength -> sumMotionByVisibility(0.0, arlasTrackDistanceGpsTravelled, window),
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
        collect_list(when(col(arlasCourseStateColumn).equalTo(ArlasCourseStates.PAUSE), col(arlasTrackTrail))).over(window)),
      arlasTrackMotionsVisibleTrail -> getVisibilityTrailUDF(lit(1.0),
                                                             collect_list(col(arlasTrackVisibilityProportion)).over(window),
                                                             collect_list(col(arlasTrackTrail)).over(window)),
      arlasTrackMotionsInvisibleTrail ->
        getVisibilityTrailUDF(lit(0.0),
                              collect_list(col(arlasTrackVisibilityProportion)).over(window),
                              collect_list(col(arlasTrackTrail)).over(window)),
      tmpTrailData -> getTrailDataUDF(
        collect_list(col(arlasTrackTrail)).over(window),
        collect_list(col(arlasTrackLocationLat)).over(window),
        collect_list(col(arlasTrackLocationLon)).over(window),
        collect_list(col(arlasCourseStateColumn).equalTo(lit(ArlasCourseStates.MOTION)))
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
      when(col(arlasCourseOrStopColumn).equalTo(lit(ArlasCourseOrStop.COURSE)), lag(prevCol, 1).over(window))

    df.withColumn(arlasArrivalStopAfterDuration, whenIsCourseGetNext(arlasTrackDuration))
      .withColumn(arlasArrivalStopAfterLocationLon, whenIsCourseGetNext(arlasTrackLocationLon))
      .withColumn(arlasArrivalStopAfterLocationLat, whenIsCourseGetNext(arlasTrackLocationLat))
      .withColumn(arlasArrivalStopAfterLocationPrecisionValueLat, whenIsCourseGetNext(arlasTrackLocationPrecisionValueLat))
      .withColumn(arlasArrivalStopAfterLocationPrecisionValueLon, whenIsCourseGetNext(arlasTrackLocationPrecisionValueLon))
      .withColumn(arlasArrivalStopAfterLocationPrecisionGeometry, whenIsCourseGetNext(arlasTrackLocationPrecisionGeometry))
      .withColumn(arlasArrivalStopAfterVisibilityProportion, whenIsCourseGetNext(arlasTrackVisibilityProportion))
      .withColumn(arlasArrivalAddressState, whenIsCourseGetNext(arlasTrackAddressState))
      .withColumn(arlasArrivalAddressPostcode, whenIsCourseGetNext(arlasTrackAddressPostcode))
      .withColumn(arlasArrivalAddressCounty, whenIsCourseGetNext(arlasTrackAddressCounty))
      .withColumn(arlasArrivalAddressCountry, whenIsCourseGetNext(arlasTrackAddressCountry))
      .withColumn(arlasArrivalAddressCountryCode, whenIsCourseGetNext(arlasTrackAddressCountryCode))
      .withColumn(arlasArrivalAddressCity, whenIsCourseGetNext(arlasTrackAddressCity))
      .withColumn(arlasDepartureStopBeforeDuration, whenIsCourseGetPrev(arlasTrackDuration))
      .withColumn(arlasDepartureStopBeforeLocationLon, whenIsCourseGetPrev(arlasTrackLocationLon))
      .withColumn(arlasDepartureStopBeforeLocationLat, whenIsCourseGetPrev(arlasTrackLocationLat))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionValueLat, whenIsCourseGetPrev(arlasTrackLocationPrecisionValueLat))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionValueLon, whenIsCourseGetPrev(arlasTrackLocationPrecisionValueLon))
      .withColumn(arlasDepartureStopBeforeLocationPrecisionGeometry, whenIsCourseGetPrev(arlasTrackLocationPrecisionGeometry))
      .withColumn(arlasDepartureStopBeforeVisibilityProportion, whenIsCourseGetPrev(arlasTrackVisibilityProportion))
      .withColumn(arlasDepartureAddressState, whenIsCourseGetPrev(arlasTrackAddressState))
      .withColumn(arlasDepartureAddressPostcode, whenIsCourseGetPrev(arlasTrackAddressPostcode))
      .withColumn(arlasDepartureAddressCounty, whenIsCourseGetPrev(arlasTrackAddressCounty))
      .withColumn(arlasDepartureAddressCountry, whenIsCourseGetPrev(arlasTrackAddressCountry))
      .withColumn(arlasDepartureAddressCountryCode, whenIsCourseGetPrev(arlasTrackAddressCountryCode))
      .withColumn(arlasDepartureAddressCity, whenIsCourseGetPrev(arlasTrackAddressCity))
      .filter(col(arlasCourseOrStopColumn).equalTo(ArlasCourseOrStop.COURSE))
      .drop(tmpTrailData)
  }

  def sumMotionByVisibility(visibility: Double, sourceCol: String, window: WindowSpec) =
    sum(
      when(col(arlasCourseStateColumn)
             .equalTo(lit(ArlasCourseStates.MOTION))
             .and(col(arlasTrackVisibilityProportion).equalTo(lit(visibility))),
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
      .add(StructField(arlasArrivalAddressState, StringType, true))
      .add(StructField(arlasArrivalAddressPostcode, StringType, true))
      .add(StructField(arlasArrivalAddressCounty, StringType, true))
      .add(StructField(arlasArrivalAddressCountry, StringType, true))
      .add(StructField(arlasArrivalAddressCountryCode, StringType, true))
      .add(StructField(arlasArrivalAddressCity, StringType, true))
      .add(StructField(arlasDepartureStopBeforeDuration, LongType, true))
      .add(StructField(arlasDepartureStopBeforeLocationLon, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationLat, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasDepartureStopBeforeLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasDepartureStopBeforeVisibilityProportion, DoubleType, true))
      .add(StructField(arlasDepartureAddressState, StringType, true))
      .add(StructField(arlasDepartureAddressPostcode, StringType, true))
      .add(StructField(arlasDepartureAddressCounty, StringType, true))
      .add(StructField(arlasDepartureAddressCountry, StringType, true))
      .add(StructField(arlasDepartureAddressCountryCode, StringType, true))
      .add(StructField(arlasDepartureAddressCity, StringType, true))

  }
}
