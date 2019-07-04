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

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class WithStateIdOnStateChangeTest extends ArlasTest {

  val stateCol  = when(col(arlasTimestampColumn).lt(lit(1527804601)), lit("state1")).otherwise(lit("state2"))
  val baseDF = cleanedDF
    .withColumn("state", stateCol)

  val idObjectA = when(col(arlasTimestampColumn).lt(lit(1527804601)), lit("1527804000"))
    .otherwise(lit("1527804601"))
  val idObjectB = when(col(arlasTimestampColumn).lt(lit(1527804601)), lit("1527804000"))
    .otherwise(lit("1527804451"))

  val expectedDF = baseDF
    .withColumn("state_id",
                concat(col(dataModel.idColumn),
                       lit("#"),
                       when(col(dataModel.idColumn).equalTo("ObjectA"), idObjectA).otherwise(idObjectB)))

  "WithStateIdOnStateChange transformation " should " fill/generate state id against dataframe's timeseries" in {

    val transformedDF: DataFrame = baseDF
      .withColumn("state", stateCol)
      .enrichWithArlas(
        new WithStateIdOnStateChange(dataModel, "state", "state_id"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}
