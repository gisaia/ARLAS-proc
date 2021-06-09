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

package io.arlas.data.transform.testdata

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasCourseOrStop, ArlasCourseStates}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.ListMap

class CourseExtractorDataGenerator(spark: SparkSession,
                                   baseDF: DataFrame,
                                   dataModel: DataModel,
                                   speedColumn: String,
                                   tempoProportionsColumns: Map[String, String],
                                   tempoIrregular: String,
                                   standardDeviationEllipsisNbPoints: Int)
    extends FragmentSummaryDataGenerator(
      spark,
      baseDF,
      dataModel = dataModel,
      speedColumn = speedColumn,
      tempoProportionsColumns = tempoProportionsColumns,
      tempoIrregular = tempoIrregular,
      standardDeviationEllipsisNbPoints = standardDeviationEllipsisNbPoints,
      aggregationColumn = arlasCourseIdColumn,
      aggregationCondition = (arlasCourseOrStopColumn, ArlasCourseOrStop.COURSE),
      additionalAggregations = (values: Map[String, Any], rows: Array[Row]) => {

        def sumDurationByVisibility[T](visibility: Double) = {
          rows
            .filter(_.getAs[String](arlasCourseStateColumn) == ArlasCourseStates.MOTION)
            .filter(_.getAs[Double](arlasTrackVisibilityProportion) == visibility)
            .map(_.getAs[Long](arlasTrackDuration))
            .sum
        }

        def sumDistanceGpsTravelledByVisibility[T](visibility: Double) = {
          rows
            .filter(_.getAs[String](arlasCourseStateColumn) == ArlasCourseStates.MOTION)
            .filter(_.getAs[Double](arlasTrackVisibilityProportion) == visibility)
            .map(_.getAs[Double](arlasTrackDistanceGpsTravelled))
            .sum
        }

        val motionVisibleDuration = sumDurationByVisibility(1.0)
        val motionVisibleLength = sumDistanceGpsTravelledByVisibility(1.0)
        val motionInvisibleDuration = sumDurationByVisibility(0.0)
        val motionInvisibleLength = sumDistanceGpsTravelledByVisibility(0.0)
        val pauseRows = rows
          .filter(_.getAs[String](arlasCourseStateColumn) == ArlasCourseStates.PAUSE)
        val pauseDuration = pauseRows
          .map(_.getAs[Long](arlasTrackDuration))
          .sum
        val pauseNumber = pauseRows.count(r => true)
        val pauseVisibilityProportion = 1.0 * pauseRows
          .map(p => p.getAs[Double](arlasTrackVisibilityProportion) * p.getAs[Long](arlasTrackDuration))
          .sum / pauseDuration
        val trailData =
          GeoTool.getTrailDataFromTrailsAndCoords(
            rows.map(_.getAs[String](arlasTrackTrail)),
            rows.map(_.getAs[Double](arlasTrackLocationLat)),
            rows.map(_.getAs[Double](arlasTrackLocationLon)),
            rows.map(_.getAs[String](arlasCourseStateColumn) == ArlasCourseStates.MOTION)
          )

        values ++ Map(
          //update existing columns
          arlasTrackTrail -> trailData.get.trail,
          //add new columns
          arlasTrackVisibilityProportion -> null,
          arlasTrackAddressCity -> null,
          arlasTrackAddressCounty -> null,
          arlasTrackAddressCountryCode -> null,
          arlasTrackAddressCountry -> null,
          arlasTrackAddressState -> null,
          arlasTrackAddressPostcode -> null,
          arlasTrackMotionsVisibleDuration -> motionVisibleDuration,
          arlasTrackMotionsVisibleLength -> motionVisibleLength,
          arlasTrackMotionsInvisibleDuration -> motionInvisibleDuration,
          arlasTrackMotionsInvisibleLength -> motionInvisibleLength,
          arlasTrackPausesDuration -> pauseDuration,
          arlasTrackPausesShortNumber -> pauseNumber,
          arlasTrackPausesLongNumber -> 0,
          arlasTrackPausesVisibilityProportion -> (if (pauseVisibilityProportion.isNaN) null
                                                   else pauseVisibilityProportion),
          arlasTrackMotionVisibilityProportionDuration -> 1.0 * motionVisibleDuration / (motionVisibleDuration + motionInvisibleDuration),
          arlasTrackMotionVisibilityProportionDistance -> 1.0 * motionVisibleLength / (motionVisibleLength + motionInvisibleLength),
          arlasTrackPausesProportion -> 1.0 * pauseDuration / rows.head
            .getAs[Long](arlasCourseDurationColumn),
          arlasDepartureTimestamp -> rows.head.getAs[Long](arlasTrackTimestampStart),
          arlasArrivalTimestamp -> rows.last.getAs[Long](arlasTrackTimestampEnd),
          arlasTrackPausesTrail -> GeoTool
            .lineStringsToSingleMultiLineString(pauseRows.map(_.getAs[String](arlasTrackTrail)))
            .getOrElse(null),
          arlasTrackMotionsVisibleTrail -> GeoTool
            .groupTrailsByConsecutiveValue[Double](1.0,
                                                   rows.map(_.getAs[Double](arlasTrackVisibilityProportion)),
                                                   rows.map(_.getAs[String](arlasTrackTrail)))
            .getOrElse(null),
          arlasTrackMotionsInvisibleTrail -> GeoTool
            .groupTrailsByConsecutiveValue[Double](0.0,
                                                   rows.map(_.getAs[Double](arlasTrackVisibilityProportion)),
                                                   rows.map(_.getAs[String](arlasTrackTrail)))
            .getOrElse(null),
          arlasDepartureLocationLat -> trailData.get.departureLat,
          arlasDepartureLocationLon -> trailData.get.departureLon,
          arlasArrivalLocationLat -> trailData.get.arrivalLat,
          arlasArrivalLocationLon -> trailData.get.arrivalLon
        )
      },
      additionalAggregationsNewColumns = Seq(
        StructField(arlasTrackMotionsVisibleDuration, LongType, true),
        StructField(arlasTrackMotionsVisibleLength, DoubleType, true),
        StructField(arlasTrackMotionsInvisibleDuration, LongType, true),
        StructField(arlasTrackMotionsInvisibleLength, DoubleType, true),
        StructField(arlasTrackPausesDuration, LongType, true),
        StructField(arlasTrackPausesShortNumber, IntegerType, true),
        StructField(arlasTrackPausesLongNumber, IntegerType, true),
        StructField(arlasTrackPausesVisibilityProportion, DoubleType, true),
        StructField(arlasTrackMotionVisibilityProportionDuration, DoubleType, true),
        StructField(arlasTrackMotionVisibilityProportionDistance, DoubleType, true),
        StructField(arlasTrackPausesProportion, DoubleType, true),
        StructField(arlasDepartureTimestamp, LongType, true),
        StructField(arlasArrivalTimestamp, LongType, true),
        StructField(arlasTrackPausesTrail, StringType, true),
        StructField(arlasTrackMotionsVisibleTrail, StringType, true),
        StructField(arlasTrackMotionsInvisibleTrail, StringType, true),
        StructField(arlasDepartureLocationLat, DoubleType, true),
        StructField(arlasDepartureLocationLon, DoubleType, true),
        StructField(arlasArrivalLocationLat, DoubleType, true),
        StructField(arlasArrivalLocationLon, DoubleType, true)
      ),
      afterTransform = (rows, schema) => {
        val afterTransformSchema = StructType(
          schema ++ Seq(
            StructField(arlasArrivalStopAfterDuration, LongType, true),
            StructField(arlasArrivalStopAfterLocationLon, DoubleType, true),
            StructField(arlasArrivalStopAfterLocationLat, DoubleType, true),
            StructField(arlasArrivalStopAfterLocationPrecisionValueLat, DoubleType, true),
            StructField(arlasArrivalStopAfterLocationPrecisionValueLon, DoubleType, true),
            StructField(arlasArrivalStopAfterLocationPrecisionGeometry, StringType, true),
            StructField(arlasArrivalStopAfterVisibilityProportion, DoubleType, true),
            StructField(arlasArrivalAddressState, StringType, true),
            StructField(arlasArrivalAddressPostcode, StringType, true),
            StructField(arlasArrivalAddressCounty, StringType, true),
            StructField(arlasArrivalAddressCountry, StringType, true),
            StructField(arlasArrivalAddressCountryCode, StringType, true),
            StructField(arlasArrivalAddressCity, StringType, true),
            StructField(arlasDepartureStopBeforeDuration, LongType, true),
            StructField(arlasDepartureStopBeforeLocationLon, DoubleType, true),
            StructField(arlasDepartureStopBeforeLocationLat, DoubleType, true),
            StructField(arlasDepartureStopBeforeLocationPrecisionValueLat, DoubleType, true),
            StructField(arlasDepartureStopBeforeLocationPrecisionValueLon, DoubleType, true),
            StructField(arlasDepartureStopBeforeLocationPrecisionGeometry, StringType, true),
            StructField(arlasDepartureStopBeforeVisibilityProportion, DoubleType, true),
            StructField(arlasDepartureAddressState, StringType, true),
            StructField(arlasDepartureAddressPostcode, StringType, true),
            StructField(arlasDepartureAddressCounty, StringType, true),
            StructField(arlasDepartureAddressCountry, StringType, true),
            StructField(arlasDepartureAddressCountryCode, StringType, true),
            StructField(arlasDepartureAddressCity, StringType, true)
          ))

        val afterTransformRows = rows
          .groupBy(_.getAs[String](dataModel.idColumn))
          .flatMap {
            case (_, rows: Seq[Row]) => {
              val sortedRows = rows
                .sortBy(_.getAs[Long](arlasTrackTimestampStart))

              sortedRows.zipWithIndex
                .filter(_._1.getAs[String](arlasCourseOrStopColumn) == ArlasCourseOrStop.COURSE)
                .map {
                  case (r, i) =>
                    val stopBefore = sortedRows
                      .lift(i - 1)
                      .filter(_.getAs[String](arlasCourseOrStopColumn) == ArlasCourseOrStop.STOP)
                    val stopAfter = sortedRows
                      .lift(i + 1)
                      .filter(_.getAs[String](arlasCourseOrStopColumn) == ArlasCourseOrStop.STOP)

                    def getStopAfterValue[T](col: String) =
                      stopAfter
                        .map(_.getAs[T](col))
                        .getOrElse(null.asInstanceOf[T])

                    def getStopBeforeValue[T](col: String) =
                      stopBefore
                        .map(_.getAs[T](col))
                        .getOrElse(null.asInstanceOf[T])

                    val data = r.toSeq ++ ListMap(
                      arlasArrivalStopAfterDuration -> getStopAfterValue[Long](arlasTrackDuration),
                      arlasArrivalStopAfterLocationLon -> getStopAfterValue[Double](arlasTrackLocationLon),
                      arlasArrivalStopAfterLocationLat -> getStopAfterValue[Double](arlasTrackLocationLat),
                      arlasArrivalStopAfterLocationPrecisionValueLat -> getStopAfterValue[Double](arlasTrackLocationPrecisionValueLat),
                      arlasArrivalStopAfterLocationPrecisionValueLon -> getStopAfterValue[Double](arlasTrackLocationPrecisionValueLon),
                      arlasArrivalStopAfterLocationPrecisionGeometry -> getStopAfterValue[String](arlasTrackLocationPrecisionGeometry),
                      arlasArrivalStopAfterVisibilityProportion -> getStopAfterValue[Double](arlasTrackVisibilityProportion),
                      arlasArrivalAddressState -> getStopAfterValue[String](arlasTrackAddressState),
                      arlasArrivalAddressPostcode -> getStopAfterValue[String](arlasTrackAddressPostcode),
                      arlasArrivalAddressCounty -> getStopAfterValue[String](arlasTrackAddressCounty),
                      arlasArrivalAddressCountry -> getStopAfterValue[String](arlasTrackAddressCountry),
                      arlasArrivalAddressCountryCode -> getStopAfterValue[String](arlasTrackAddressCountryCode),
                      arlasArrivalAddressCity -> getStopAfterValue[String](arlasTrackAddressCity),
                      arlasDepartureStopBeforeDuration -> getStopBeforeValue[Long](arlasTrackDuration),
                      arlasDepartureStopBeforeLocationLon -> getStopBeforeValue[Double](arlasTrackLocationLon),
                      arlasDepartureStopBeforeLocationLat -> getStopBeforeValue[Double](arlasTrackLocationLat),
                      arlasDepartureStopBeforeLocationPrecisionValueLat -> getStopBeforeValue[Double](arlasTrackLocationPrecisionValueLat),
                      arlasDepartureStopBeforeLocationPrecisionValueLon -> getStopBeforeValue[Double](arlasTrackLocationPrecisionValueLon),
                      arlasDepartureStopBeforeLocationPrecisionGeometry -> getStopBeforeValue[String](arlasTrackLocationPrecisionGeometry),
                      arlasDepartureStopBeforeVisibilityProportion -> getStopBeforeValue[Double](arlasTrackVisibilityProportion),
                      arlasDepartureAddressState -> getStopBeforeValue[String](arlasTrackAddressState),
                      arlasDepartureAddressPostcode -> getStopBeforeValue[String](arlasTrackAddressPostcode),
                      arlasDepartureAddressCounty -> getStopBeforeValue[String](arlasTrackAddressCounty),
                      arlasDepartureAddressCountry -> getStopBeforeValue[String](arlasTrackAddressCountry),
                      arlasDepartureAddressCountryCode -> getStopBeforeValue[String](arlasTrackAddressCountryCode),
                      arlasDepartureAddressCity -> getStopBeforeValue[String](arlasTrackAddressCity)
                    ).values
                    new GenericRowWithSchema(data.toArray, afterTransformSchema)
                }
            }
          }
          .toSeq

        (afterTransformRows, afterTransformSchema)
      }
    )
