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
  val arlasMotionIdColumn = "arlas_motion_id"
  val arlasGeoPointColumn = "arlas_geopoint"
  val arlasDeltaTimestampColumn          = "arlas_delta_timestamp"
  val arlasPreviousDeltaTimestampColumn  = "arlas_previous_delta_timestamp"
  val arlasDeltaTimestampVariationColumn = "arlas_delta_timestamp_variation"
  val arlasMovingStateColumn = "arlas_moving_state"
  val arlasTempoColumn = "arlas_tempo"
}

object ArlasVisibilityStates  {
  sealed abstract class ArlasVisibilityStatesVal(visibilityState: String) {
    override def toString: String = visibilityState
  }
  case object APPEAR extends ArlasVisibilityStatesVal("appear")
  case object DISAPPEAR extends ArlasVisibilityStatesVal("disappear")
  case object VISIBLE extends ArlasVisibilityStatesVal("visible")
  case object INVISIBLE extends ArlasVisibilityStatesVal("invisible")
}