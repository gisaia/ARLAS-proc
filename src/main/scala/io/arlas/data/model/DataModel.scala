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

case class DataModel(
                      idColumn         : String = "id",
                      timestampColumn  : String = "timestamp",
                      timeFormat       : String = "yyyy-MM-dd'T'HH:mm:ssZ",
                      latColumn        : String = "lat",
                      lonColumn        : String = "lon",
                      speedColumn      : String = "",
                      dynamicFields    : Array[String] = Array("lat", "lon"),
                      visibilityTimeout: Int = 3600, //in seconds
                      timeSampling     : Long = 15, //in seconds
                      movingStateModel : MLModel = null
)
