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

package io.arlas.data.transform.ml

import io.arlas.data.sql._
import io.arlas.data.model.MLModelLocal
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.functions._

class HmmProcessorTest extends ArlasTest {

  val movingStateModel = MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json")
  private val partitionColumn = "id"
  private val expectedMovingStateColumn = "expected_moving_state"

  val testDF = arlasTestDF
    .withColumn(
      expectedMovingStateColumn,
      when(
        (col(partitionColumn)
          .equalTo("ObjectA")
          .and(
            col(arlasTimestampColumn)
              .leq(1527804059)
              .or(col(arlasTimestampColumn)
                .geq(1527804271)
                .and(col(arlasTimestampColumn).leq(1527804721)))))
          .or(
            col(partitionColumn)
              .equalTo("ObjectB")
              .and(col(arlasTimestampColumn)
                .geq(1527804029)
                .and(col(arlasTimestampColumn).leq(1527804561)))),
        lit("STILL")
        //using `otherwise(lit(null))` in an impossible case makes thee column nullable
      ).otherwise(when(lit(true), lit("MOVE")).otherwise(lit(null)))
    )

  "HmmProcessor " should " have unknown result with not existing source column" in {

    val transformedDF = testDF
      .drop(expectedMovingStateColumn)
      .enrichWithArlas(
        new HmmProcessor("notExisting", movingStateModel, partitionColumn, "result", 5000)
      )
      .withColumn("expected_result", lit("Unknown"))

    assertColumnsAreEqual(transformedDF, "result", "expected_result")
  }

  "HmmProcessor " should " have unknown result with not existing model" in {

    val transformedDF = testDF
      .drop(expectedMovingStateColumn)
      .enrichWithArlas(
        new HmmProcessor(speedColumn,
                         MLModelLocal(spark, "src/test/resources/not_existing.json"),
                         partitionColumn,
                         "result",
                         5000)
      )
      .withColumn("expected_result", lit("Unknown"))

    assertColumnsAreEqual(transformedDF, "result", "expected_result")
  }

  "HmmProcessor transformation" should " not break using a window shorter than input dataframe" in {

    testDF
      .enrichWithArlas(
        new HmmProcessor(speedColumn, movingStateModel, partitionColumn, "result", 10)
      )
      .count()
  }

  "HmmProcessor transformation" should " compute the moving state of a dataframe's timeseries" in {

    val transformedDF = testDF
    //avoid natural ordering to ensure that hmm doesn't depend on initial order
      .sort(speedColumn)
      .enrichWithArlas(
        new HmmProcessor(speedColumn,
                         movingStateModel,
                         partitionColumn,
                         arlasMovingStateColumn,
                         5000))

    assertColumnsAreEqual(transformedDF, arlasMovingStateColumn, expectedMovingStateColumn)
  }

  //we use a quite big window, the longest partition is 29 points ; because with too few points the results are bad
  //in a real environment, window size shoud be equal to some thousends
  "HmmProcessor transformation" should " compute the moving state of a dataframe's timeseries using windowing" in {

    val transformedDF = testDF
      .enrichWithArlas(
        new HmmProcessor(speedColumn,
                         movingStateModel,
                         partitionColumn,
                         arlasMovingStateColumn,
                         30))

    assertColumnsAreEqual(transformedDF, arlasMovingStateColumn, expectedMovingStateColumn)
  }

  "HmmProcessor transformation" should " compute the moving state from an ArrayTyped field" in {

    val transformedDF = testDF
      .withColumn(speedColumn, array(col(speedColumn)))
      .enrichWithArlas(
        new HmmProcessor(speedColumn,
                         movingStateModel,
                         partitionColumn,
                         arlasMovingStateColumn,
                         30))

    assertColumnsAreEqual(transformedDF, arlasMovingStateColumn, expectedMovingStateColumn)
  }

  "HmmProcessor transformation" should " compute the moving state from an ArrayTyped field with several values" in {

    val transformedDF = testDF
    //replace a single row with multiple speed values
      .withColumn(
        speedColumn,
        when(col(partitionColumn)
               .equalTo("ObjectB")
               .and(col("timestamp").equalTo("01/06/2018 00:10:00+02:00")),
             lit(Array(5.1, 5.1)))
          .otherwise(array(col(speedColumn)))
      )
      .enrichWithArlas(
        new HmmProcessor(speedColumn,
                         movingStateModel,
                         partitionColumn,
                         arlasMovingStateColumn,
                         30))

    assertColumnsAreEqual(transformedDF, arlasMovingStateColumn, expectedMovingStateColumn)
  }

}
