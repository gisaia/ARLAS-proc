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

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import io.arlas.data.sql._

class SeldomValuesByObjectReplacerTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("classifier", StringType, true)
      )
    )

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "repeated"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "repeated"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "other-repeated"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335, "other-repeated"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437, "single"),
    ("ObjectB", "01/06/2018 00:00:00+02:00", 55.920162, 17.314437, "single"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920162, 17.314437, "single"))


  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "repeated"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "repeated"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "other-repeated"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335, "other-repeated"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437, "default"),
    ("ObjectB", "01/06/2018 00:00:00+02:00", 55.920162, 17.314437, "single"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920162, 17.314437, "single"))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)
  val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, testSchema)

  "SeldomValuesReplacer " should "replace value occurring less that 40% of the rows" in {

    val transformedDF = testDF.enrichWithArlas(new SeldomValuesByObjectReplacer(dataModel, "classifier", 40d, "default"))
    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "SeldomValuesReplacer " should "fail on non-String target column " in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(new SeldomValuesByObjectReplacer(dataModel, "lat", 40d, "default"))
                                              }
  }

}
