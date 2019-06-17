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

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import io.arlas.data.sql._

class ValueReplacerTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("colstring", StringType, true),
      StructField("coldouble", DoubleType, true),
      StructField("colint", IntegerType, true)
      )
    )

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "value1", 1d, 1),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "value2", 2d, 2),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "value3", 3d, 3))


  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "new-value", 1d, 1),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "value2", 10d, 2),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "value3", 3d, 10))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)
  val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, testSchema)

  "ValueReplacer " should "replace values of different types" in {

    val transformedDF = testDF.enrichWithArlas(
      new ValueReplacer(dataModel, "colstring", "value1", "new-value"),
      new ValueReplacer[Double](dataModel, "coldouble", 2d, 10d),
      new ValueReplacer[Int](dataModel, "colint", 3, 10))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "ValueReplacer " should "fail with column type different from the expected one" in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(
                                                   new ValueReplacer[String](dataModel, "coldouble", "value1", "new-value"))
                                               }
    assert(thrown.getMessage.equals("The column coldouble should be of type string, actual: double"))
  }

  "ValueReplacer " should "fail with unsupported data type" in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(
                                                   new ValueReplacer[Exception](dataModel, "coldouble", new Exception, new Exception))
                                               }
    assert(thrown.getMessage.equals("Unsupported type java.lang.Exception"))
  }

}
