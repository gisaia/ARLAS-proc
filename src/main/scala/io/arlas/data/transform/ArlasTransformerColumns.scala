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

object ArlasTransformerColumns {
  val arlasTimestampColumn = "arlas_timestamp"
  val arlasPartitionColumn = "arlas_partition"
  val arlasDistanceColumn = "arlas_distance"
  val arlasVisibilityStateColumn = "arlas_visibility_state"
  val arlasVisibleSequenceIdColumn = "arlas_visible_sequence_id"
  val arlasGeoPointColumn = "arlas_geopoint"
  val arlasIdColumn = "arlas_id"
  val arlasDeltaTimestampColumn = "arlas_delta_timestamp"
  val arlasPreviousDeltaTimestampColumn = "arlas_previous_delta_timestamp"
  val arlasDeltaTimestampVariationColumn = "arlas_delta_timestamp_variation"
  val arlasMovingStateColumn = "arlas_moving_state"
  val arlasTempoColumn = "arlas_tempo"
  val arlasMotionIdColumn = "arlas_motion_id"
  val arlasMotionDurationColumn = "arlas_motion_duration"
  val arlasCourseOrStopColumn = "arlas_course_or_stop"
  val arlasCourseStateColumn = "arlas_course_state"
  val arlasCourseIdColumn = "arlas_course_id"
  val arlasCourseDurationColumn = "arlas_course_duration"

  // TRACK COLUMNS
  val arlasTrackPrefix = "arlas_track_"
  val arlasTrackId = arlasTrackPrefix + "id"
  val arlasTrackNbGeopoints = arlasTrackPrefix + "nb_geopoints"
  val arlasTrackTrail = arlasTrackPrefix + "trail"
  val arlasTrackDuration = arlasTrackPrefix + "duration_s"
  val arlasTrackTimestampStart = arlasTrackPrefix + "timestamp_start"
  val arlasTrackTimestampEnd = arlasTrackPrefix + "timestamp_end"
  val arlasTrackTimestampCenter = arlasTrackPrefix + "timestamp_center"
  val arlasTrackLocationLat = arlasTrackPrefix + "location_lat"
  val arlasTrackLocationLon = arlasTrackPrefix + "location_lon"
  val arlasTrackVisibilityProportion = arlasTrackPrefix + "visibility_proportion"
  val arlasTrackVisibilityChange = arlasTrackPrefix + "visibility_change"
  val arlasTrackLocationPrecisionValueLon = arlasTrackPrefix + "location_precision_value_lon"
  val arlasTrackLocationPrecisionValueLat = arlasTrackPrefix + "location_precision_value_lat"
  val arlasTrackLocationPrecisionGeometry = arlasTrackPrefix + "location_precision_geometry"
  val arlasTrackDistanceGpsTravelledM = arlasTrackPrefix + "distance_gps_travelled_m"
  val arlasTrackDistanceGpsStraigthLineM = arlasTrackPrefix + "distance_gps_straigth_line_m"
  val arlasTrackDistanceGpsStraigthness = arlasTrackPrefix + "distance_gps_straigthness"
  val arlasTrackDynamicsGpsSpeedKmh = arlasTrackPrefix + "dynamics_gps_speed_kmh"
  val arlasTrackDynamicsGpsBearing = arlasTrackPrefix + "dynamics_gps_bearing"

}

/**
  * APPEAR = first visible fragment after an invisible fragment
  * DISAPPEAR = last visible fragment before an invisible fragment
  * APPEAR_DISAPPEAR = visible fragment between 2 invisible fragments
  */
object VisibilityChange {
  val DISAPPEAR = "disappear"
  val APPEAR = "appear"
  val APPEAR_DISAPPEAR = "appear_disappear"
}

object ArlasMovingStates {
  val STILL = "STILL"
  val MOVE = "MOVE"
  val MOVE_GAP = "MOVE_GAP"
}

object ArlasCourseOrStop {
  val STOP = "STOP"
  val COURSE = "COURSE"
  val COURSE_GAP = "COURSE_GAP"
}

object ArlasCourseStates {
  val MOTION = "MOTION"
  val PAUSE = "PAUSE"
}
