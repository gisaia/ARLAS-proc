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
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class SameColValueReplacerTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("speed", DoubleType, true)
      )
    )

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.032068662532024))
  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)

  "ValueReplacer " should "replace values of different types in same column" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 0.0, 17.320418, 0.280577132616533),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.032068662532024))
    val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, testSchema)

    val transformedDF = testDF.enrichWithArlas(
      new SameColValueReplacer(dataModel, "lat", 55.921028, 0.0))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
