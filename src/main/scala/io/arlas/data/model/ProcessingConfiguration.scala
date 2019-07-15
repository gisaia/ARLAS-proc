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

package io.arlas.data.model

case class MotionConfiguration(
                                movingStateModel               : MLModel = null,
                                movingStateHmmWindowSize       : Int = 5000,
                                supportPointsConfiguration     : SupportPointsConfiguration = new SupportPointsConfiguration(),
                                tempoConfiguration             : TempoConfiguration = new TempoConfiguration()
                              )

case class SupportPointsConfiguration(
                                       supportPointDeltaTime          : Int = 120,
                                       supportPointColsToPropagate    : Seq[String] = Seq(),
                                       supportPointMaxNumberInGap     : Int = 10,
                                       supportPointMeanSpeedMultiplier: Double = 1.0
                                     )

//TODO move to customer specific project
case class TempoConfiguration(
                               tempoModel                     : MLModel = null,
                               tempoHmmWindowSize             : Int = 5000,
                               irregularTempo                 : String = "",
                               salvoTempo                     : String = "",
                               salvoTempoValues               : Seq[String] = Seq()
                             )

case class CourseConfiguration(
                                courseTimeout                  : Int = 3600 //in seconds
                              )