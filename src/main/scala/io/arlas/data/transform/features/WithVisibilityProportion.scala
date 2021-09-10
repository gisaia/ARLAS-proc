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

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTrackDuration, arlasTrackVisibilityProportion}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify a fragment as "invisible" if the duration between its two measures is longer than a threshold
  *
  * @param visibilityProportionColumn Name of the target gap state column
  * @param durationSecondsColumn      Name of the duration (s) column
  * @param durationThreshold          Minimum duration (s) between observation to be considered as a gap
  */
class WithVisibilityProportion(visibilityProportionColumn: String = arlasTrackVisibilityProportion,
                               durationSecondsColumn: String = arlasTrackDuration,
                               durationThreshold: Long = 1800)
    extends ArlasTransformer(Vector(durationSecondsColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(visibilityProportionColumn,
                  when(col(durationSecondsColumn).gt(lit(durationThreshold)), lit(0.0d))
                    .otherwise(lit(1.0d)))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(visibilityProportionColumn, DoubleType, false))
  }
}
