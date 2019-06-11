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
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class WithArlasVisibilityStateFromTempoTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("tempo", StringType, true)
      )
    )

  val expectedSchema = testSchema
    .add(StructField(arlasVisibilityStateColumn, StringType, false))

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "other"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.921028, 17.320418, "10s"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.921028, 17.320418, "10s"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.921028, 17.320418, "other"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.921028, 17.320418, "other"),
    ("ObjectB", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "other"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.921028, 17.320418, "10s"))


  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "other", ArlasVisibilityStates.APPEAR.toString),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.921028, 17.320418, "10s", ArlasVisibilityStates.VISIBLE.toString),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.921028, 17.320418, "10s", ArlasVisibilityStates.DISAPPEAR.toString),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.921028, 17.320418, "other", ArlasVisibilityStates.APPEAR.toString),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.921028, 17.320418, "other", ArlasVisibilityStates.APPEAR.toString),
    ("ObjectB", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "other", ArlasVisibilityStates.APPEAR.toString),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.921028, 17.320418, "10s", ArlasVisibilityStates.VISIBLE.toString))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)
  val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, expectedSchema)

  "WithArlasVisibilityStateFromTempo " should "add visibility state related to the tempo" in {

    val transformedDF = testDF.enrichWithArlas(
      new WithArlasTimestamp(dataModel),
      new WithArlasVisibilityStateFromTempo(dataModel, spark, "tempo", "other"))
      .drop(arlasTimestampColumn)

    assertDataFrameEquality(transformedDF, expectedDF)
  }



}
