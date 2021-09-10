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
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Create a new speed column based on gps and sensor speed according to simple rules:
  *  - If fragment duration > Threshold : Sensor speed is punctual and not a good approximation of fragment speed, gps speed is chosen
  *  - If fragment duration < Threshold : Gps speed is to noisy because of gps precision and short duration, sensor speed is chosen
  *
  * @param newSpeedColumn Name of the new column containing the hybrid speed
  * @param gpsSpeedColumn Name of the column containing GPS Speed
  * @param sensorSpeedColumn Name of the column containing Sensor Speed
  * @param durationColumn Name of the column containing duration (s)
  * @param durationThreshold Duration threshold (s) permitting to choose between the two speed
  */
class WithGpsOrSensorSpeed(newSpeedColumn: String,
                           gpsSpeedColumn: String,
                           sensorSpeedColumn: String,
                           durationColumn: String,
                           durationThreshold: Int = 300)
    extends ArlasTransformer(Vector(gpsSpeedColumn, sensorSpeedColumn, durationColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(
        newSpeedColumn,
        when(col(sensorSpeedColumn).isNull, col(gpsSpeedColumn))
          .otherwise(
            when(col(durationColumn).gt(lit(durationThreshold)), col(gpsSpeedColumn))
              .otherwise(col(sensorSpeedColumn))
          )
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(newSpeedColumn, StringType, false))
  }
}
