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

class OtherColValuesMapperTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("sourcestring", StringType, true),
      StructField("sourcedouble", DoubleType, true)
      ))

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "test", 0.0),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "test", 0.0),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "other-test", 1.0),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, "out", 2.0))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)

  "WithValuesMapper " should "replace values of type string in another column" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "test", 0.0, "success"),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "test", 0.0, "success"),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "other-test", 1.0, "other-success"),
      ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, "out", 2.0, "out"))

    val expectedSchema = testSchema.add(StructField("targetstring", StringType))

    val transformedDF = testDF.enrichWithArlas(
      new OtherColValuesMapper(dataModel, "sourcestring", "targetstring", Map("test" -> "success", "other-test" -> "other-success")))

    assertDataFrameEquality(transformedDF, spark.createDataFrame(expectedData.toDF().rdd, expectedSchema))
  }

  "WithValuesMapper " should "replace values of another type" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "test", 99.0),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "test", 99.0),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "other-test", 100.0),
      ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, "out", 2.0))

    val transformedDF = testDF.enrichWithArlas(
      new OtherColValuesMapper(dataModel, "sourcedouble", "sourcedouble", Map(0.0 -> 99.0, 1.0 -> 100.0)))

    assertDataFrameEquality(transformedDF, spark.createDataFrame(expectedData.toDF().rdd, testSchema))
  }

  "WithValuesMapper " should "throw exception with target column of another type" in {

    val thrown = intercept[DataFrameException] {
                                                        testDF.enrichWithArlas(
                                                          new OtherColValuesMapper(dataModel, "sourcestring", "sourcedouble", Map("test" -> "value")))
                                                      }

    assert(
      thrown.getMessage == "Source and target columns should be of the same type, currently source is StringType and target DoubleType")
  }

}
