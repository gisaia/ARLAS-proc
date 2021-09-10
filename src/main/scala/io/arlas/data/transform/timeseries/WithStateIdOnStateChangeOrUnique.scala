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

package io.arlas.data.transform.timeseries

import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

/**
  * Create an ID for each sequence of an object id sharing a same state or if the fragment has a "unique state"
  *
  * @param idColumn Column containing the id used to create sequences
  * @param stateColumn Column containing the state to use
  * @param orderColumn Column containing the field to order sequences (usually timestamp)
  * @param targetIdColumn Column to store the new created id
  * @param uniqueState Value of the states that is considered as "unique", the fragment associated has a unique new id
  */
class WithStateIdOnStateChangeOrUnique(idColumn: String,
                                       stateColumn: String,
                                       orderColumn: String = arlasTimestampColumn,
                                       targetIdColumn: String,
                                       uniqueState: Option[String] = None)
    extends WithStateId(
      idColumn,
      orderColumn,
      targetIdColumn, {
        val window = Window.partitionBy(idColumn).orderBy(orderColumn)
        val isNewId = lag(stateColumn, 1)
          .over(window)
          .notEqual(col(stateColumn))
          .or(lag(stateColumn, 1).over(window).isNull)
        uniqueState match {
          case Some(uniqueStateValue) => isNewId.or(col(stateColumn).equalTo(uniqueStateValue))
          case None                   => isNewId
        }
      }
    )
