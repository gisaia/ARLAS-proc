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

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{LongType, StringType}

import scala.collection.immutable.ListMap

class WithStateIdOnStateChangeTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 0l, "state1", "id1#0"),
      Seq("id1", 30l, "state3", "id1#30"),
      Seq("id1", 10l, "state1", "id1#0"),
      Seq("id1", 20l, "state2", "id1#20"),
      Seq("id2", 0l, "state1", "id2#0"),
      Seq("id2", 10l, "state1", "id2#0")
    ),
    ListMap(
      "id" -> (StringType, true),
      arlasTimestampColumn -> (LongType, true),
      "state" -> (StringType, true),
      "expected_state_id" -> (StringType, true)
    )
  )

  val baseDF = testDF.drop("expected_state_id")

  "WithStateIdOnStateChange transformation " should " fill/generate state id against dataframe's timeseries" in {

    val expectedDF = testDF.withColumnRenamed("expected_state_id", "state_id")

    val transformedDF =
      baseDF.enrichWithArlas(new WithStateIdOnStateChangeOrUnique(dataModel.idColumn, "state", arlasTimestampColumn, "state_id"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}
