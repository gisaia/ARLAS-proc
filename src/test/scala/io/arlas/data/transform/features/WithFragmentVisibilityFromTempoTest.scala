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
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasTest, VisibilityChange}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

import scala.collection.immutable.ListMap

class WithFragmentVisibilityFromTempoTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      //id1
      Seq("id1", 0l, "irregular", 0.0, null),
      Seq("id1", 10l, "tempo1", 1.0, VisibilityChange.APPEAR),
      Seq("id1", 20l, "tempo2", 1.0, null),
      Seq("id1", 30l, "tempo1", 1.0, VisibilityChange.DISAPPEAR),
      Seq("id1", 40l, "irregular", 0.0, null),
      Seq("id1", 50l, "tempo1", 1.0, VisibilityChange.APPEAR_DISAPPEAR),
      Seq("id1", 60l, "irregular", 0.0, null),
      //id2
      Seq("id2", 0l, "tempo1", 1.0, null),
      Seq("id2", 10l, "tempo2", 1.0, VisibilityChange.DISAPPEAR),
      Seq("id2", 20l, "irregular", 0.0, null),
      Seq("id2", 30l, "tempo1", 1.0, VisibilityChange.APPEAR),
      Seq("id2", 40l, "tempo2", 1.0, null)
    ),
    ListMap(
      dataModel.idColumn -> (StringType, true),
      arlasTimestampColumn -> (LongType, true),
      arlasTempoColumn -> (StringType, true),
      "expected_track_visibility_proportion" -> (DoubleType, false),
      "expected_track_visibility_change" -> (StringType, true)
    )
  )

  val baseDF =
    testDF.drop("expected_track_visibility_proportion", "expected_track_visibility_change")

  "WithFragmentVisibilityFromTempo " should "add visibility proportion and change relying to tempo" in {

    val expectedDF = testDF
      .withColumnRenamed("expected_track_visibility_proportion", arlasTrackVisibilityProportion)
      .withColumnRenamed("expected_track_visibility_change", arlasTrackVisibilityChange)

    val transformedDF =
      baseDF.enrichWithArlas(new WithFragmentVisibilityFromTempo(dataModel, spark, "irregular"))

    assertDataFrameEquality(transformedDF, expectedDF)

  }
}
