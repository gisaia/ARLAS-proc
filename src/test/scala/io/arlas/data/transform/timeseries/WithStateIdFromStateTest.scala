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
import io.arlas.data.sql._
import io.arlas.data.transform.timeseries.WithStateIdFromState
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import io.arlas.data.transform.ArlasTestHelper._
import scala.collection.immutable.ListMap

class WithStateIdFromStateTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 1, "new", "id1#1"),
      Seq("id1", 2, "new", "id1#2"),
      Seq("id1", 3, "not-new", "id1#2"),
      Seq("id1", 4, "not-new", "id1#2"),
      Seq("id1", 5, "not-new", "id1#2"),
      Seq("id1", 6, "new", "id1#6"),
      Seq("id1", 7, "not-new", "id1#6"),
      Seq("id2", 1, "new", "id2#1"),
      Seq("id2", 2, "not-new", "id2#1")
    ),
    ListMap("id" -> (StringType, true),
            "timestamp" -> (IntegerType, true),
            "state" -> (StringType, true),
            "expected_state_id" -> (StringType, true))
  )

  val baseDF = testDF.drop("expected_state_id")

  "WithStateIdFromState transformation " should " fill/generate state id against dataframe's timeseries" in {

    val expectedDF = testDF.withColumnRenamed("expected_state_id", "state_id")

    val transformedDF = baseDF
      .enrichWithArlas(
        new WithStateIdFromState(
          DataModel(),
          "state",
          "timestamp",
          "new",
          "state_id"
        ))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithStateIdFromState transformation " should " resume state id when adding a warm up period" in {

    val expectedDF = testDF.withColumnRenamed("expected_state_id", "state_id")

    val transformedDF = baseDF
      .enrichWithArlas(
        new WithStateIdFromState(
          DataModel(),
          "state",
          "timestamp",
          "new",
          "state_id"
        ))
      .withColumn("state_id",
                  when(col("id").equalTo("id1").and(col("timestamp").lt(5)), lit(null))
                    .otherwise(col("state_id")))
      .enrichWithArlas(
        new WithStateIdFromState(
          DataModel(),
          "state",
          "timestamp",
          "new",
          "state_id"
        ))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
