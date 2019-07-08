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

import org.apache.spark.sql.functions._
import io.arlas.data.sql._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField}
import io.arlas.data.transform.ArlasTransformerColumns._

class WithSupportGeoPointTest extends ArlasTest {

  import spark.implicits._

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, 0.42065662232025, ArlasVisibilityStates.APPEAR.toString),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, 0.32068662532024, ArlasVisibilityStates.VISIBLE.toString),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601, 3.746582198175621, ArlasVisibilityStates.VISIBLE.toString),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335, 0.180505395097491, 1.624548555877419, ArlasVisibilityStates.VISIBLE.toString),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437, 18.3267208898955, 348.2076969080145, ArlasVisibilityStates.DISAPPEAR.toString))

  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, 0.42065662232025, ArlasVisibilityStates.APPEAR.toString, 1527804000l),
    ("ObjectA", null, 55.920875, 17.319322, 0.032068662532024, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804001l),
    ("ObjectA", null, 55.920875, 17.319322, 0.032068662532024, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804009l),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, 0.32068662532024, ArlasVisibilityStates.VISIBLE.toString, 1527804010l),
    ("ObjectA", null, 55.920583, 17.31733, 0.178408676103601, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804011l),
    ("ObjectA", null, 55.920583, 17.31733, 0.178408676103601, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804016l),
    ("ObjectA", null, 55.920583, 17.31733, 0.178408676103601, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804025l),
    ("ObjectA", null, 55.920583, 17.31733, 0.178408676103601, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804030l),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601, 3.746582198175621, ArlasVisibilityStates.VISIBLE.toString, 1527804031l),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335, 0.180505395097491, 1.624548555877419, ArlasVisibilityStates.VISIBLE.toString, 1527804040l),
    ("ObjectA", null, 55.920162, 17.314437, 18.3267208898955, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804041l),
    ("ObjectA", null, 55.920162, 17.314437, 18.3267208898955, -1.0, ArlasVisibilityStates.INVISIBLE.toString, 1527804058l),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437, 18.3267208898955, 348.2076969080145, ArlasVisibilityStates.DISAPPEAR.toString, 1527804059l))

  val testSchema = rawSchema
    .add(StructField("distance", DoubleType, true))
    .add(StructField("arlas_visibility_state", StringType, true))

  val expectedSchema = testSchema.add(
    StructField("arlas_timestamp", LongType, true)
    )

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)
  val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, expectedSchema)
    //cannot create df with int null values: see https://issues.apache.org/jira/browse/SPARK-20299 => using -1.0 as temporary value
    .withColumn("distance", when(col("distance").equalTo(lit(-1.0)), lit(null)).otherwise(col("distance")))

  "WithSupportGeoPoint " should "add invisible support geopoint" in {

    val transformedDF = testDF.enrichWithArlas(
      new WithArlasPartition(dataModel),
      new WithArlasTimestamp(dataModel),
      new WithArlasDeltaTimestamp(dataModel, spark, dataModel.idColumn),
      new WithSupportGeoPoint(dataModel, spark, "distance", 5,
                              Seq(dataModel.idColumn, dataModel.latColumn, dataModel.lonColumn, arlasTimestampColumn)))
        .drop(arlasPartitionColumn, arlasDeltaTimestampColumn, arlasPreviousDeltaTimestampColumn, arlasDeltaTimestampVariationColumn, "keep")

    assertDataFrameEquality(transformedDF, expectedDF)
  }



}
