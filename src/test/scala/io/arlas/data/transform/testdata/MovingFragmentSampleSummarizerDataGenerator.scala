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

package io.arlas.data.transform.testdata

import io.arlas.data.model.DataModel
import io.arlas.data.transform.{ArlasMovingStates, VisibilityChange}
import io.arlas.data.transform.ArlasTransformerColumns.{arlasMotionDurationColumn, _}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap

class MovingFragmentSampleSummarizerDataGenerator(spark: SparkSession,
                                                  baseDF: DataFrame,
                                                  dataModel: DataModel,
                                                  speedColumn: String,
                                                  tempoProportionsColumns: Map[String, String],
                                                  tempoIrregular: String,
                                                  standardDeviationEllipsisNbPoints: Int)
    extends FragmentSummaryDataGenerator(
      spark = spark,
      baseDF = baseDF,
      dataModel = dataModel,
      speedColumn = speedColumn,
      tempoProportionsColumns = tempoProportionsColumns,
      tempoIrregular = tempoIrregular,
      standardDeviationEllipsisNbPoints = standardDeviationEllipsisNbPoints,
      aggregationColumn = arlasTrackSampleId,
      aggregationCondition = (arlasMovingStateColumn, ArlasMovingStates.MOVE),
      additionalAggregations = (values, rows) => {
        val trailData =
          GeoTool.getTrailDataFromTrailsAndCoords(
            rows.map(_.getAs[String](arlasTrackTrail)),
            rows.map(_.getAs[Double](arlasTrackLocationLat)),
            rows.map(_.getAs[Double](arlasTrackLocationLon)),
            rows.map(_.getAs[String](arlasMovingStateColumn) == ArlasMovingStates.MOVE)
          )

        values ++ ListMap(
          //update existing columns
          arlasTrackSampleId -> rows.head.getAs[Long](arlasTrackSampleId),
          arlasMotionDurationColumn -> rows.head.getAs[Long](arlasMotionDurationColumn),
          arlasMotionIdColumn -> rows.head.getAs[Long](arlasMotionIdColumn),
          arlasTrackTrail -> trailData.get.trail,
          arlasTrackVisibilityChange -> {
            val first = rows.head.getAs[String](arlasTrackVisibilityChange)
            val last = rows.last.getAs[String](arlasTrackVisibilityChange)
            if (first != null && first.startsWith(VisibilityChange.APPEAR) && last != null && last.endsWith(VisibilityChange.DISAPPEAR)) {
              VisibilityChange.APPEAR_DISAPPEAR
            } else if (first != null && first.startsWith(VisibilityChange.APPEAR)) {
              VisibilityChange.APPEAR
            } else if (last != null && last.endsWith(VisibilityChange.DISAPPEAR)) {
              VisibilityChange.DISAPPEAR
            } else {
              null
            }
          },
          arlasTrackVisibilityProportion -> {
            rows
              .map(w => w.getAs[Double](arlasTrackVisibilityProportion))
              .sum / rows.length
          }
        )
      }
    )
