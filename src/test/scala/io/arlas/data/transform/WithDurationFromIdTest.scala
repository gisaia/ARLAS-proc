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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WithDurationFromIdTest extends ArlasTest {

  val durationObjA = when(col(arlasTimestampColumn).lt(lit(1527804601)), lit(291l))
    .otherwise(lit(480l))
  val durationObjB = when(col(arlasTimestampColumn).lt(lit(1527804451)), lit(60l))
    .otherwise(lit(540l))

  val baseDF = cleanedDF
    .enrichWithArlas(
      new FlowFragmentMapper(dataModel, spark, dataModel.idColumn, List("speed")),
      new WithArlasVisibilityStateFromTimestamp(dataModel, visibilityTimeout),
      new WithStateIdFromState(dataModel,
                               arlasVisibilityStateColumn,
                               arlasTimestampColumn,
                               ArlasVisibilityStates.APPEAR.toString,
                               arlasVisibleSequenceIdColumn)
    )

  val expectedDF = baseDF
    .withColumn(
      "duration",
      when(col(dataModel.idColumn).equalTo("ObjectA"), durationObjA).otherwise(durationObjB))
    .withColumn("duration", when(col("duration").isNotNull, col("duration")).otherwise(lit(null)))

  "WithDurationFromId transformation " should " compute duration of visibility sequences" in {

    val transformedDF: DataFrame = baseDF
      .enrichWithArlas(
        new WithDurationFromId(arlasVisibleSequenceIdColumn, "duration")
      )

    assertDataFrameEquality(
      transformedDF.select(dataModel.idColumn, "duration", arlasVisibleSequenceIdColumn),
      expectedDF.select(dataModel.idColumn, "duration", arlasVisibleSequenceIdColumn)
    )
  }

}
