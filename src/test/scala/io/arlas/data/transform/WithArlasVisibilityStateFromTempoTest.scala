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

import io.arlas.data.model.MLModelLocal
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class WithArlasVisibilityStateFromTempoTest extends ArlasTest {

  import spark.implicits._

  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 1527804000l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 1527804010l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 1527804031l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 1527804040l, "tempo_10s", "DISAPPEAR"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 1527804059l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:01:19+02:00", 1527804079l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:01:40+02:00", 1527804100l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:01:49+02:00", 1527804109l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:01:59+02:00", 1527804119l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:02:00+02:00", 1527804120l, "tempo_1s", "DISAPPEAR"),
    ("ObjectA", "01/06/2018 00:02:19+02:00", 1527804139l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:02:20+02:00", 1527804140l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:02:31+02:00", 1527804151l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:02:40+02:00", 1527804160l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:02:51+02:00", 1527804171l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:02:59+02:00", 1527804179l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:03:00+02:00", 1527804180l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:03:10+02:00", 1527804190l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:03:19+02:00", 1527804199l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:03:20+02:00", 1527804200l, "tempo_1s", "DISAPPEAR"),
    ("ObjectA", "01/06/2018 00:03:31+02:00", 1527804211l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:03:51+02:00", 1527804231l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:04:00+02:00", 1527804240l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:04:10+02:00", 1527804250l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:04:19+02:00", 1527804259l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:04:31+02:00", 1527804271l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:04:40+02:00", 1527804280l, "tempo_10s", "DISAPPEAR"),
    ("ObjectA", "01/06/2018 00:04:51+02:00", 1527804291l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:10:01+02:00", 1527804601l, "tempo_other", "APPEAR"),
    ("ObjectA", "01/06/2018 00:10:11+02:00", 1527804611l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:10:21+02:00", 1527804621l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:10:30+02:00", 1527804630l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:10:41+02:00", 1527804641l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:10:51+02:00", 1527804651l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:01+02:00", 1527804661l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:11+02:00", 1527804671l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:19+02:00", 1527804679l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:30+02:00", 1527804690l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:39+02:00", 1527804699l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:11:51+02:00", 1527804711l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:12:01+02:00", 1527804721l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:12:11+02:00", 1527804731l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:12:19+02:00", 1527804739l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:12:30+02:00", 1527804750l, "tempo_10s", "VISIBLE"),
    ("ObjectA", "01/06/2018 00:12:39+02:00", 1527804759l, "tempo_10s", "DISAPPEAR"),
    ("ObjectA", "01/06/2018 00:12:51+02:00", 1527804771l, "tempo_other", "APPEAR"),
    ("ObjectB", "01/06/2018 00:00:00+02:00", 1527804000l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 1527804010l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:00:21+02:00", 1527804021l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:00:29+02:00", 1527804029l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:00:40+02:00", 1527804040l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:00:50+02:00", 1527804050l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:01:00+02:00", 1527804060l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:07:31+02:00", 1527804451l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:07:41+02:00", 1527804461l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:07:50+02:00", 1527804470l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:00+02:00", 1527804480l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:10+02:00", 1527804490l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:31+02:00", 1527804511l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:21+02:00", 1527804501l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:41+02:00", 1527804521l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:08:50+02:00", 1527804530l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:00+02:00", 1527804540l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:11+02:00", 1527804551l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:21+02:00", 1527804561l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:31+02:00", 1527804571l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:41+02:00", 1527804581l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:09:50+02:00", 1527804590l, "tempo_10s", "VISIBLE"),
    ("ObjectB", "01/06/2018 00:10:00+02:00", 1527804600l, "tempo_10s", "VISIBLE")
    )

  val expectedSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("arlas_timestamp", LongType, false),
      StructField("arlas_tempo", StringType, true),
      StructField("arlas_visibility_state", StringType, false)
      ))

  val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, expectedSchema)

  "WithArlasVisibilityStateFromTempo " should "add visibility state related to the tempo" in {

    val transformedDF = cleanedDF.enrichWithArlas(
      new WithArlasDeltaTimestamp(dataModel, spark, dataModel.idColumn),
      new HmmProcessor(dataModel,
                       spark,
                       arlasDeltaTimestampColumn,
                       new MLModelLocal(spark, "src/test/resources/hmm_tempo_model.json"),
                       dataModel.idColumn,
                       arlasTempoColumn,
                       5000)).enrichWithArlas(
      new WithArlasVisibilityStateFromTempo(dataModel, spark, "tempo_other"))

    assertDataFrameEquality(
      transformedDF.drop(dataModel.latColumn, dataModel.lonColumn, dataModel.speedColumn, arlasPartitionColumn, arlasDeltaTimestampColumn,
                         arlasPreviousDeltaTimestampColumn, arlasDeltaTimestampVariationColumn),
      expectedDF)
  }



}