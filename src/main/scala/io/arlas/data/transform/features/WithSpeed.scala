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
import io.arlas.data.transform.ArlasTransformerColumns.arlasTrackDuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Compute mean speed in a given unit from distance (m) and duration (s).
  *
  * @param speedColumn Name of the target speed column
  * @param speedUnit Unit of the computed speed ("m/s", "km/h" or "nd")
  * @param durationSecondsColumn Name of the duration (s) column
  * @param distanceMetersColumn Name of the distance (m) column
  */
class WithSpeed(speedColumn: String, speedUnit: String, durationSecondsColumn: String = arlasTrackDuration, distanceMetersColumn: String)
    extends ArlasTransformer(Vector(durationSecondsColumn, distanceMetersColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    def coef(unit: String): Double = unit match {
      case "m/s"  => 1.0
      case "km/h" => 1.0 / 3.6
      case "nd"   => 1.9438444924406
      case _      => 1.0
    }

    dataset
      .toDF()
      .withColumn(
        speedColumn,
        col(distanceMetersColumn) / col(durationSecondsColumn) * lit(coef(speedUnit))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(speedColumn, DoubleType, false))
  }
}
