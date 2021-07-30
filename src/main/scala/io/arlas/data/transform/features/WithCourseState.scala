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

import io.arlas.data.transform.{ArlasCourseOrStop, ArlasCourseStates, ArlasMovingStates, ArlasTransformer}
import io.arlas.data.transform.ArlasTransformerColumns.{arlasCourseOrStopColumn, arlasCourseStateColumn, arlasMovingStateColumn}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Determine the course state (MOTION, PAUSE) for course fragments
  * Requires courseOrStop column and movingState column
  */
class WithCourseState() extends ArlasTransformer(Vector(arlasCourseOrStopColumn, arlasMovingStateColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val courseState = when(
      col(arlasCourseOrStopColumn)
        .equalTo(ArlasCourseOrStop.COURSE)
        .and(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.MOVE)),
      ArlasCourseStates.MOTION
    ).otherwise(when(
      col(arlasCourseOrStopColumn)
        .equalTo(ArlasCourseOrStop.COURSE)
        .and(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)),
      ArlasCourseStates.PAUSE
    ))

    dataset
      .toDF()
      .withColumn(arlasCourseStateColumn, courseState)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasCourseStateColumn, StringType, false))
  }
}
