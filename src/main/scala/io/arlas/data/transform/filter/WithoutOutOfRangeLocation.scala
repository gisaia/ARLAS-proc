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

package io.arlas.data.transform.filter

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Keep only lat/lon values included in a valid range.
  *
  * @param latColumn Name of latitude column
  * @param lonColumn Name of longitude column
  * @param latMin    Minimum valid value for latitude
  * @param lonMin    Minimum valid value for longitude
  * @param latMax    Maximum valid value for latitude
  * @param lonMax    Maximum valid value for longitude
  */
class WithoutOutOfRangeLocation(latColumn: String,
                                lonColumn: String,
                                latMin: Double = -90,
                                lonMin: Double = -180,
                                latMax: Double = 90,
                                lonMax: Double = 180)
    extends ArlasTransformer(Vector(latColumn, lonColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .filter(
        col(latColumn)
          .leq(lit(latMax))
          .and(col(latColumn).geq(lit(latMin)))
          .and(col(lonColumn).leq(lit(lonMax)))
          .and(col(lonColumn).geq(lit(lonMin))))
  }
}
