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

import io.arlas.data.model.DataModel
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.timeseries._
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.immutable.ListMap

class IdUpdaterTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", "1#0", 1, 10, "id1#1_100"),
      Seq("id1", "1#0", 10, 20, "id1#1_100"),
      Seq("id1", "1#0", 20, 50, "id1#1_100"),
      Seq("id1", "1#0", 50, 100, "id1#1_100"),
      Seq("id1", "1#100", 100, 1000, "id1#100_2000"),
      Seq("id1", "1#100", 1000, 2000, "id1#100_2000"),
      Seq("id2", "2#10000", 10000, 10001, "id2#10000_10001")
    ),
    ListMap(
      "id" -> (StringType, true),
      "seq_id" -> (StringType, true),
      arlasTrackTimestampStart -> (IntegerType, true),
      arlasTrackTimestampEnd -> (IntegerType, true),
      "expected_new_seq_id" -> (StringType, true)
    )
  )

  val baseDF = testDF.drop("expected_new_seq_id")

  "IdUpdater transformation " should " update idColumn as objectid#start_end" in {

    val expectedDF = testDF.drop("seq_id").withColumnRenamed("expected_new_seq_id", "seq_id")
    val transformedDF = baseDF.enrichWithArlas(new IdUpdater(DataModel(), "seq_id"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
