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
  val arlasDeltaTimestampColumn          = "arlas_delta_timestamp"
  val arlasPreviousDeltaTimestampColumn  = "arlas_previous_delta_timestamp"
  val arlasDeltaTimestampVariationColumn = "arlas_delta_timestamp_variation"
  val arlasMovingStateColumn = "arlas_moving_state"
  val arlasTempoColumn = "arlas_tempo"
  val arlasMotionIdColumn = "arlas_motion_id"
  val arlasMotionDurationColumn = "arlas_motion_duration"
  val arlasCourseOrStopColumn = "arlas_course_or_stop"
  val arlasCourseStateColumn = "arlas_course_state"
  val arlasCourseIdColumn = "arlas_course_id"
  val arlasCourseDurationColumn = "arlas_course_duration"
}

/**
 * APPEAR = first point of a visible sequence
 * DISAPPEAR = last point of a visible sequence
 * VISIBLE = other points of the sequence
 * INVISIBLE = points that are not visible
 */
object ArlasVisibilityStates  {
  sealed abstract class ArlasVisibilityStatesVal(visibilityState: String) {
    override def toString: String = visibilityState
  }
  case object APPEAR extends ArlasVisibilityStatesVal("APPEAR")
  case object DISAPPEAR extends ArlasVisibilityStatesVal("DISAPPEAR")
  case object VISIBLE extends ArlasVisibilityStatesVal("VISIBLE")
  case object INVISIBLE extends ArlasVisibilityStatesVal("INVISIBLE")
}

object ArlasMovingStates  {
  sealed abstract class ArlasMovingStates(movingState: String) {
    override def toString: String = movingState
  }
  case object STILL extends ArlasMovingStates("STILL")
  case object MOVE extends ArlasMovingStates("MOVE")
}

object ArlasCourseOrStop  {
  sealed abstract class ArlasCourseStates(courseState: String) {
    override def toString: String = courseState
  }
  case object STOP extends ArlasCourseStates("STOP")
  case object COURSE extends ArlasCourseStates("COURSE")
}

object ArlasCourseStates  {
  sealed abstract class ArlasCourseStates(courseState: String) {
    override def toString: String = courseState
  }
  case object MOTION extends ArlasCourseStates("MOTION")
  case object PAUSE extends ArlasCourseStates("PAUSE")
}