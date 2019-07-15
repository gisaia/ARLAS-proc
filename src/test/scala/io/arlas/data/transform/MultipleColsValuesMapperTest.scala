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
import org.apache.spark.sql.functions._

class MultipleColsValuesMapperTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("speed", DoubleType, true),
      StructField("sourcestring", StringType, true),
      StructField("sourcedouble", DoubleType, true)
      ))

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, "value1", 0.0),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, "value2", 0.0),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601, "value1", 1.0),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, 0.180505395097491, "value2", 1.0))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)

  "MultipleColsValuesMapper " should "map values of multiple type to a single value of type String" in {

    val expectedSchema = testSchema.add(StructField("result", StringType))
    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, "value1", 0.0, "found"),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, "value2", 0.0, "null"),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601, "value1", 1.0, "null"),
      ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, 0.180505395097491, "value2", 1.0, "also_found"))
    val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, expectedSchema)
    //cannot create df with int null values: see https://issues.apache.org/jira/browse/SPARK-20299 => using -1.0 as temporary value
      .withColumn("result", when(col("result").equalTo("null"), lit(null)).otherwise(col("result")))

    val transformedDF = testDF.enrichWithArlas(
      new MultipleColsValuesMapper(dataModel, Map(
        "found" -> Map("sourcestring" -> "value1", "sourcedouble" -> 0.0),
        "also_found" -> Map("sourcestring" -> "value2", "sourcedouble" -> 1.0)
      ), "result"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "MultipleColsValuesMapper " should "not add any column with no input values " in {

    val transformedDF = testDF.enrichWithArlas(
      new MultipleColsValuesMapper(dataModel, Map(), "result"))

    assertDataFrameEquality(transformedDF, testDF)
  }

  "MultipleColsValuesMapper " should "fail with an unknown column in input values " in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(
                                                   new MultipleColsValuesMapper(dataModel, Map(
                                                     "found" -> Map("sourcestring" -> "value1", "notexists" -> 0.0)),
                                                                                "result"))
                                               }
    assert(thrown.getMessage.equals(
      "The notexists columns are not included in the DataFrame with the following columns id, timestamp, lat, lon, speed, sourcestring, sourcedouble"))
  }

}
