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
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.immutable.ListMap

class StopPauseSummaryTransformer(spark: SparkSession,
                                  dataModel: DataModel,
                                  standardDeviationEllipsisNbPoint: Int,
                                  irregularTempo: String,
                                  tempoPropotionColumns: Map[String, String],
                                  weightAveragedColumns: Seq[String])
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      standardDeviationEllipsisNbPoint,
      irregularTempo,
      tempoPropotionColumns,
      weightAveragedColumns
    ) {

  override def getAggregationColumn(): String = arlasMotionIdColumn

  override def getAggregateCondition(): Column =
    col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)

  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] =
    ListMap(
      arlasTrackTrail -> col(arlasTrackLocationPrecisionGeometry)
    )

  override def getPropagatedColumns(): Seq[String] = {
    Seq(
      arlasMovingStateColumn,
      arlasCourseOrStopColumn,
      arlasCourseStateColumn,
      arlasMotionDurationColumn,
      arlasCourseIdColumn,
      arlasCourseDurationColumn,
      dataModel.idColumn
    )
  }
}
