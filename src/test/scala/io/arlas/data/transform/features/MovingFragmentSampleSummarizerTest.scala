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

import io.arlas.data.sql._
import io.arlas.data.transform._

class MovingFragmentSampleSummarizerTest extends ArlasTest {

  val transformedDF = getMovingFragmentSampleSummarizerBaseDF
    .drop(getMovingFragmentSampleSummarizerBaseDF.columns.filter(_.startsWith("arlas_course")): _*)
    .drop("arlas_track_location_precision_geometry", "arlas_track_distance_sensor_travelled_m", "arlas_track_location_precision_value_lat", "arlas_track_location_precision_value_lon")
    .enrichWithArlas(
      new MovingFragmentSampleSummarizer(
        spark,
        dataModel,
        tempoIrregular,
        Some(tempoProportionsColumns),
        Some(averagedColumns)
      ))

  "MovingFragmentSampleSummarizer transformation" should "aggregate moving fragments with the same arlas_track_sample_id against dataframe's timeseries" in {

    val expectedDF =
      movingFragmentSampleSummarizerDF.drop(movingFragmentSampleSummarizerDF.columns.filter(_.startsWith("arlas_course")): _*)
        .drop("arlas_track_location_precision_geometry", "arlas_track_distance_sensor_travelled_m", "arlas_track_location_precision_value_lat", "arlas_track_location_precision_value_lon")

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
