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

package io.arlas.data.transform

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class WithArlasCourseOrStopFromMovingState(dataModel: DataModel, courseTimeout: Int)
    extends ArlasTransformer(dataModel, Vector(arlasMovingStateColumn, arlasMotionDurationColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val courseState = when(
      col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.STILL.toString)),
      when(col(arlasMotionDurationColumn) < courseTimeout, lit(ArlasCourseOrStop.COURSE.toString))
        .otherwise(lit(ArlasCourseOrStop.STOP.toString))
    ).otherwise(lit(ArlasCourseOrStop.COURSE.toString))

    dataset
      .toDF()
      .withColumn(arlasCourseOrStopColumn, courseState)

  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasCourseOrStopColumn, StringType, false))
  }
}
