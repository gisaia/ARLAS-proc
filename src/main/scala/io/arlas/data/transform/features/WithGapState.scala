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

import io.arlas.data.transform.{ArlasMovingStates, ArlasTransformer}
import io.arlas.data.transform.ArlasTransformerColumns.arlasTrackDuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify a gap when the duration between two measures is longer than a threshold
  *
  * @param gapStateColumn Name of the target gap state column
  * @param durationSecondsColumn Name of the duration (s) column
  * @param durationThreshold Minimum duration between observation to be considered as a gap
  */
class WithGapState(gapStateColumn: String = "gap_state",
                   durationSecondsColumn: String = arlasTrackDuration,
                   durationThreshold: Long = 43200)
    extends ArlasTransformer(Vector(durationSecondsColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(gapStateColumn,
                  when(col(durationSecondsColumn).gt(lit(durationThreshold)), lit(ArlasMovingStates.GAP))
                    .otherwise(lit("not_a_gap")))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(gapStateColumn, DoubleType, false))
  }
}
