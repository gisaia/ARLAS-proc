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

package io.arlas.data.transform.fragments

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest

class CourseExtractorTransformerTest extends ArlasTest {

  val transformedDF = courseExtractorBaseDF
    .process(
      new CourseExtractorTransformer(
        spark,
        dataModel,
        propagatedColumns = None,
        irregularTempo = tempoIrregular,
        tempoColumns = Some(tempoProportionsColumns),
        weightAveragedColumns = Some(averagedColumns),
        computePrecision=true
      )
    )
    .drop("arlas_track_location_precision_geometry", "arlas_track_distance_sensor_travelled_m")
    //TODO fix when expected visibility can be computed
    .drop("arlas_track_motion_visibility_proportion_distance", "arlas_track_motion_visibility_proportion_duration", "arlas_track_motions_invisible_duration_s", "arlas_track_motions_invisible_length_m", "arlas_track_motions_invisible_trail", "arlas_track_motions_visible_duration_s", "arlas_track_motions_visible_length_m", "arlas_track_motions_visible_trail")
    //TODO fix when expected pauses location can be computed
    .drop("arlas_track_pauses_location")

  "CourseExtractorTransformer transformation" should "aggregate the course fragments against dataframe's timeseries" in {

    val expectedDF = courseExtractorDF
      .drop("arlas_track_location_precision_geometry", "arlas_track_distance_sensor_travelled_m")
      //TODO fix when expected visibility can be computed
      .drop("arlas_track_motion_visibility_proportion_distance", "arlas_track_motion_visibility_proportion_duration", "arlas_track_motions_invisible_duration_s", "arlas_track_motions_invisible_length_m", "arlas_track_motions_invisible_trail", "arlas_track_motions_visible_duration_s", "arlas_track_motions_visible_length_m", "arlas_track_motions_visible_trail")
      //TODO fix when expected pauses location can be computed
      .drop("arlas_track_pauses_location")

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
