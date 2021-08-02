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

import io.arlas.data.transform.features.WithGeoData

object ArlasTransformerColumns {

  val arlasPartitionFormat = "yyyyMMdd"

  val arlasTimestampColumn = "arlas_timestamp"
  val arlasPartitionColumn = "arlas_partition"
  val arlasDistanceColumn = "arlas_distance"
  val arlasVisibilityStateColumn = "arlas_visibility_state"
  val arlasVisibleSequenceIdColumn = "arlas_visible_sequence_id"
  val arlasGeoPointColumn = "arlas_geopoint"
  val arlasIdColumn = "arlas_id"
  val arlasGapState = "arlas_gap_state"
  val arlasMovingStateColumn = "arlas_moving_state"
  val arlasTempoColumn = "arlas_tempo"
  val arlasMotionIdColumn = "arlas_motion_id"
  val arlasMotionDurationColumn = "arlas_motion_duration"
  val arlasCourseOrStopColumn = "arlas_course_or_stop"
  val arlasCourseStateColumn = "arlas_course_state"
  val arlasCourseIdColumn = "arlas_course_id"
  val arlasCourseDurationColumn = "arlas_course_duration"

  // TRACK COLUMNS
  val arlasTrackPrefix: String = "arlas_track_"
  val arlasTrackId = arlasTrackPrefix + "id"
  val arlasTrackSampleId = arlasTrackPrefix + "sample_id"
  val arlasTrackNbGeopoints = arlasTrackPrefix + "nb_geopoints"
  val arlasTrackTrail = arlasTrackPrefix + "trail"
  val arlasTrackTrailGeohashes = arlasTrackPrefix + "trail_geohashes"
  val arlasTrackDuration = arlasTrackPrefix + "duration_s"
  val arlasTrackTimestampStart = arlasTrackPrefix + "timestamp_start"
  val arlasTrackTimestampEnd = arlasTrackPrefix + "timestamp_end"
  val arlasTrackTimestampCenter = arlasTrackPrefix + "timestamp_center"
  val arlasTrackLocationLat = arlasTrackPrefix + "location_lat"
  val arlasTrackLocationLon = arlasTrackPrefix + "location_lon"
  val arlasTrackLocation = arlasTrackPrefix + "location"
  val arlasTrackEndLocation = arlasTrackPrefix + "end_location"
  val arlasTrackEndLocationLat = arlasTrackPrefix + "end_location_lat"
  val arlasTrackEndLocationLon = arlasTrackPrefix + "end_location_lon"
  val arlasTrackVisibilityProportion = arlasTrackPrefix + "visibility_proportion"
  val arlasTrackVisibilityChange = arlasTrackPrefix + "visibility_change"
  val arlasTrackLocationPrecisionValueLon = arlasTrackPrefix + "location_precision_value_lon"
  val arlasTrackLocationPrecisionValueLat = arlasTrackPrefix + "location_precision_value_lat"
  val arlasTrackLocationPrecisionGeometry = arlasTrackPrefix + "location_precision_geometry"
  val arlasTrackDistanceGpsTravelled = arlasTrackPrefix + "distance_gps_travelled_m"
  val arlasTrackDistanceGpsStraigthLine = arlasTrackPrefix + "distance_gps_straigth_line_m"
  val arlasTrackDistanceGpsStraigthness = arlasTrackPrefix + "distance_gps_straigthness"
  val arlasTrackDynamicsGpsSpeedKmh = arlasTrackPrefix + "dynamics_gps_speed_kmh"
  val arlasTrackDynamicsGpsBearing = arlasTrackPrefix + "dynamics_gps_bearing"
  val arlasTrackDistanceSensorTravelled = arlasTrackPrefix + "distance_sensor_travelled_m"
  val arlasTrackTempoEmissionIsMulti = arlasTrackPrefix + "tempo_emission_is_multi"

  // Address Geo Data
  val arlasTrackAddressPrefix = arlasTrackPrefix + "address_"
  val arlasTrackAddressState = arlasTrackAddressPrefix + WithGeoData.statePostfix
  val arlasTrackAddressPostcode = arlasTrackAddressPrefix + WithGeoData.postcodePostfix
  val arlasTrackAddressCounty = arlasTrackAddressPrefix + WithGeoData.countyPostfix
  val arlasTrackAddressCountry = arlasTrackAddressPrefix + WithGeoData.countryPostfix
  val arlasTrackAddressCountryCode = arlasTrackAddressPrefix + WithGeoData.countryCodePostfix
  val arlasTrackAddressCity = arlasTrackAddressPrefix + WithGeoData.cityPostfix

  val arlasTrackMotionsVisibleDuration = arlasTrackPrefix + "motions_visible_duration_s"
  val arlasTrackMotionsVisibleLength = arlasTrackPrefix + "motions_visible_length_m"
  val arlasTrackMotionsInvisibleDuration = arlasTrackPrefix + "motions_invisible_duration_s"
  val arlasTrackMotionsInvisibleLength = arlasTrackPrefix + "motions_invisible_length_m"
  val arlasTrackMotionVisibilityProportionDuration =
    arlasTrackPrefix + "motion_visibility_proportion_duration"
  val arlasTrackMotionVisibilityProportionDistance =
    arlasTrackPrefix + "motion_visibility_proportion_distance"
  val arlasTrackMotionsVisibleTrail = arlasTrackPrefix + "motions_visible_trail"
  val arlasTrackMotionsInvisibleTrail = arlasTrackPrefix + "motions_invisible_trail"

  val arlasTrackPausesLocation = arlasTrackPrefix + "pauses_location"
  val arlasTrackPausesDuration = arlasTrackPrefix + "pauses_duration_s"
  val arlasTrackPausesShortNumber = arlasTrackPrefix + "pauses_short_number"
  val arlasTrackPausesLongNumber = arlasTrackPrefix + "pauses_long_number"
  val arlasTrackPausesVisibilityProportion = arlasTrackPrefix + "pauses_visibility_proportion"
  val arlasTrackPausesTrail = arlasTrackPrefix + "pauses_trail"
  val arlasTrackPausesProportion = arlasTrackPrefix + "pauses_proportion"

  // Routing (itinerary)
  val arlasTrackRoutingPrefix = arlasTrackPrefix + "routing_"
  val arlasTrackRoutingTrailRefined = arlasTrackRoutingPrefix + "trail_refined"
  val arlasTrackRoutingDistance = arlasTrackRoutingPrefix + "distance"
  val arlasTrackRoutingDuration = arlasTrackRoutingPrefix + "duration"

  // Course info (for flow mode)
  val arlasCoursePrefix = "arlas_course_"
  val arlasCourseDistanceGpsTravelled = arlasCoursePrefix + "distance_gps_travelled"
  val arlasCourseDistanceGpsStraightLine = arlasCoursePrefix + "distance_gps_straight_line"
  val arlasCourseDistanceGpsStraightness = arlasCoursePrefix + "distance_gps_straightness"
  val arlasCourseDepartureLocation = arlasCoursePrefix + "departure_location"
  val arlasCourseDepartureTimestamp = arlasCoursePrefix + "departure_timestamp"
  val arlasCourseArrivalLocation = arlasCoursePrefix + "arrival_location"
  val arlasCourseArrivalTimestamp = arlasCoursePrefix + "arrival_timestamp"

  // DEPARTURE COLUMNS
  val arlasDeparturePrefix = "arlas_departure_"
  val arlasDepartureTimestamp = arlasDeparturePrefix + "timestamp"
  val arlasDepartureLocationLat = arlasDeparturePrefix + "location_lat"
  val arlasDepartureLocationLon = arlasDeparturePrefix + "location_lon"
  val arlasDepartureStopBeforeDuration = arlasDeparturePrefix + "stop_before_duration_s"
  val arlasDepartureStopBeforeLocation = arlasDeparturePrefix + "stop_before_location"
  val arlasDepartureStopBeforeLocationLon = arlasDeparturePrefix + "stop_before_location_lon"
  val arlasDepartureStopBeforeLocationLat = arlasDeparturePrefix + "stop_before_location_lat"
  val arlasDepartureStopBeforeLocationPrecisionValueLat =
    arlasDeparturePrefix + "stop_before_location_precision_value_lat"
  val arlasDepartureStopBeforeLocationPrecisionValueLon =
    arlasDeparturePrefix + "stop_before_location_precision_value_lon"
  val arlasDepartureStopBeforeLocationPrecisionGeometry =
    arlasDeparturePrefix + "stop_before_location_precision_geometry"
  val arlasDepartureStopBeforeVisibilityProportion =
    arlasDeparturePrefix + "stop_before_visibility_proportion"

  // Address Geo Data
  val arlasDepartureAddressPrefix = arlasDeparturePrefix + "address_"
  val arlasDepartureAddressState = arlasDepartureAddressPrefix + WithGeoData.statePostfix
  val arlasDepartureAddressPostcode = arlasDepartureAddressPrefix + WithGeoData.postcodePostfix
  val arlasDepartureAddressCounty = arlasDepartureAddressPrefix + WithGeoData.countyPostfix
  val arlasDepartureAddressCountry = arlasDepartureAddressPrefix + WithGeoData.countryPostfix
  val arlasDepartureAddressCountryCode = arlasDepartureAddressPrefix + WithGeoData.countryCodePostfix
  val arlasDepartureAddressCity = arlasDepartureAddressPrefix + WithGeoData.cityPostfix

  // ARRIVAL COLUMNS
  val arlasArrivalPrefix = "arlas_arrival_"
  val arlasArrivalTimestamp = arlasArrivalPrefix + "timestamp"
  val arlasArrivalLocationLat = arlasArrivalPrefix + "location_lat"
  val arlasArrivalLocationLon = arlasArrivalPrefix + "location_lon"
  val arlasArrivalStopAfterDuration = arlasArrivalPrefix + "stop_after_duration_s"
  val arlasArrivalStopAfterLocation = arlasArrivalPrefix + "stop_after_location"
  val arlasArrivalStopAfterLocationLon = arlasArrivalPrefix + "stop_after_location_lon"
  val arlasArrivalStopAfterLocationLat = arlasArrivalPrefix + "stop_after_location_lat"
  val arlasArrivalStopAfterLocationPrecisionValueLat =
    arlasArrivalPrefix + "stop_after_location_precision_value_lat"
  val arlasArrivalStopAfterLocationPrecisionValueLon =
    arlasArrivalPrefix + "stop_after_location_precision_value_lon"
  val arlasArrivalStopAfterLocationPrecisionGeometry =
    arlasArrivalPrefix + "stop_after_location_precision_geometry"
  val arlasArrivalStopAfterVisibilityProportion = arlasArrivalPrefix + "stop_after_visibility_proportion"

  // Address Geo Data
  val arlasArrivalAddressPrefix = arlasArrivalPrefix + "address_"
  val arlasArrivalAddressState = arlasArrivalAddressPrefix + WithGeoData.statePostfix
  val arlasArrivalAddressPostcode = arlasArrivalAddressPrefix + WithGeoData.postcodePostfix
  val arlasArrivalAddressCounty = arlasArrivalAddressPrefix + WithGeoData.countyPostfix
  val arlasArrivalAddressCountry = arlasArrivalAddressPrefix + WithGeoData.countryPostfix
  val arlasArrivalAddressCountryCode = arlasArrivalAddressPrefix + WithGeoData.countryCodePostfix
  val arlasArrivalAddressCity = arlasArrivalAddressPrefix + WithGeoData.cityPostfix

  // MISSION COLUMNS
  val arlasMissionPrefix = "arlas_mission_"
  val arlasMissionId = arlasMissionPrefix + "id"
  val arlasMissionDuration = arlasMissionPrefix + "duration"
  val arlasMissionDistanceSensorTravelled = arlasMissionPrefix + "distance_sensor_travelled"
  val arlasMissionDistanceGpsTravelled = arlasMissionPrefix + "distance_gps_travelled"
  val arlasMissionDistanceGpsStraigthline = arlasMissionPrefix + "distance_gps_straightline"
  val arlasMissionDistanceGpsStraigthness = arlasMissionPrefix + "distance_gps_straightness"

  // MISSION Departure
  val arlasMissionDeparturePrefix = arlasMissionPrefix + "departure_"
  val arlasMissionDepartureLocationLat = arlasMissionDeparturePrefix + "location_lat"
  val arlasMissionDepartureLocationLon = arlasMissionDeparturePrefix + "location_lon"
  val arlasMissionDepartureLocation = arlasMissionDeparturePrefix + "location"
  val arlasMissionDepartureTimestamp = arlasMissionDeparturePrefix + "timestamp"

  val arlasMissionDepartureAddressPrefix = arlasMissionDeparturePrefix + "address_"
  val arlasMissionDepartureAddressState = arlasMissionDepartureAddressPrefix + WithGeoData.statePostfix
  val arlasMissionDepartureAddressPostcode = arlasMissionDepartureAddressPrefix + WithGeoData.postcodePostfix
  val arlasMissionDepartureAddressCounty = arlasMissionDepartureAddressPrefix + WithGeoData.countyPostfix
  val arlasMissionDepartureAddressCountry = arlasMissionDepartureAddressPrefix + WithGeoData.countryPostfix
  val arlasMissionDepartureAddressCountryCode = arlasMissionDepartureAddressPrefix + WithGeoData.countryCodePostfix
  val arlasMissionDepartureAddressCity = arlasMissionDepartureAddressPrefix + WithGeoData.cityPostfix

  // MISSION Arrival
  val arlasMissionArrivalPrefix = arlasMissionPrefix + "arrival_"
  val arlasMissionArrivalLocation = arlasMissionArrivalPrefix + "location"
  val arlasMissionArrivalLocationLat = arlasMissionArrivalPrefix + "location_lat"
  val arlasMissionArrivalLocationLon = arlasMissionArrivalPrefix + "location_lon"
  val arlasMissionArrivalTimestamp = arlasMissionArrivalPrefix + "timestamp"

  val arlasMissionArrivalAddressPrefix = arlasMissionArrivalPrefix + "address_"
  val arlasMissionArrivalAddressState = arlasMissionArrivalAddressPrefix + WithGeoData.statePostfix
  val arlasMissionArrivalAddressPostcode = arlasMissionArrivalAddressPrefix + WithGeoData.postcodePostfix
  val arlasMissionArrivalAddressCounty = arlasMissionArrivalAddressPrefix + WithGeoData.countyPostfix
  val arlasMissionArrivalAddressCountry = arlasMissionArrivalAddressPrefix + WithGeoData.countryPostfix
  val arlasMissionArrivalAddressCountryCode = arlasMissionArrivalAddressPrefix + WithGeoData.countryCodePostfix
  val arlasMissionArrivalAddressCity = arlasMissionArrivalAddressPrefix + WithGeoData.cityPostfix

  // Temporary Outlier detection columns

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
  val GAP = "GAP"
}

object ArlasCourseOrStop {
  val STOP = "STOP"
  val COURSE = "COURSE"
  val GAP = "GAP"
}

object ArlasCourseStates {
  val MOTION = "MOTION"
  val PAUSE = "PAUSE"
}
