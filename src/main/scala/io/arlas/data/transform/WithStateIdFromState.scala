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

import io.arlas.data.model.DataModel
import org.apache.spark.sql.functions._

/**
  * Compute ID column, the same ID is set for all consecutive rows between 2 occurences of fromState, for a same object
  * @param dataModel
  * @param stateColumn
  * @param fromState
  * @param targetIdColumn
  */
class WithStateIdFromState(dataModel: DataModel,
                           stateColumn: String,
                           orderColumn: String,
                           fromState: String,
                           targetIdColumn: String)
    extends WithStateId(dataModel,
                        stateColumn,
                        orderColumn,
                        targetIdColumn,
                        col(stateColumn).equalTo(fromState))
