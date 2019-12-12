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

package io.arlas.data.transform.features

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.immutable.ListMap

class WithDurationFromIdTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 0, 10, 100),
      Seq("id1", 90, 100, 100),
      Seq("id2", 0, 50, 50),
      Seq("id3", 10, 20, 20),
      Seq("id3", 0, 10, 20)
    ),
    ListMap(
      "id" -> (StringType, true),
      arlasTrackTimestampStart -> (IntegerType, true),
      arlasTrackTimestampEnd -> (IntegerType, true),
      "expected_duration" -> (IntegerType, true)
    )
  )

  val baseDF = testDF.drop("expected_duration")

  "WithDurationFromId transformation " should " compute duration of visibility sequences" in {

    val expectedDF = testDF.withColumnRenamed("expected_duration", "duration")
    val transformedDF = baseDF.enrichWithArlas(new WithDurationFromId("id", "duration"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
