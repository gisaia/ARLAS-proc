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
import io.arlas.data.transform.ArlasTestHelper.createDataFrameWithTypes
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasMovingStates, ArlasTest}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

import scala.collection.immutable.ListMap

class FragmentPseudoResamplerTest extends ArlasTest {

  val arlasSubMotionIdColumn = "arlas_sub_motion_id"

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      //id1
      Seq("id1", 0l, 10l, "id1_0"),
      Seq("id1", 10l, 5l, "id1_1"),
      Seq("id1", 20l, 2l, "id1_1"),
      Seq("id1", 30l, 3l, "id1_1"),
      Seq("id1", 40l, 4l, "id1_2"),
      Seq("id1", 50l, 3l, "id1_2"),
      Seq("id1", 60l, 6l, "id1_3"),
      //id2
      Seq("id2", 0l, 5l, "id2_0"),
      Seq("id2", 10l, 2l, "id2_0"),
      Seq("id2", 20l, 2l, "id2_0"),
      Seq("id2", 30l, 16l, "id2_1"),
      Seq("id2", 40l, 5l, "id2_2"),
      Seq("id2", 50l, 5l, "id2_3"),
      Seq("id2", 60l, 2l, "id2_3")
    ),
    ListMap(
      arlasMotionIdColumn -> (StringType, true),
      arlasTrackTimestampCenter -> (LongType, true),
      arlasTrackDuration -> (LongType, true),
      arlasSubMotionIdColumn -> (StringType, true)
    )
  )

  "FragmentPseudoResamplerTest withSubId transformation" should "resample fragments on a given tempo with best effort" in {

    val expectedDF = testDF

    val transformedDF: DataFrame = testDF
      .drop(arlasSubMotionIdColumn)
      .enrichWithArlas(
        new FragmentPseudoResampler(dataModel,
                                    spark,
                                    arlasMotionIdColumn,
                                    col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.MOVE)),
                                    arlasSubMotionIdColumn,
                                    10l))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
