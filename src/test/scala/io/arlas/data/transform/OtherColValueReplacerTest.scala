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

class OtherColValueReplacerTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("colstring", StringType, true),
      StructField("coldouble", DoubleType, true),
      StructField("colint", IntegerType, true),
      StructField("colnull", DoubleType, true)
      )
    )

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "value1", 1d, 1, Some(1.0)),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "value2", 2d, 2, None),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "value3", 3d, 3, None))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, testSchema)

  "ValueReplacer " should "replace values of different types in another column of another type" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "value1", 999d, 1, Some(1.0)),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, "value2", 2d, 999, None),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, "new-value", 3d, 3, None))
    val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, testSchema)

    val transformedDF = testDF.enrichWithArlas(
      new OtherColValueReplacer(dataModel, "colstring", "coldouble", "value1", 999d),
      new OtherColValueReplacer(dataModel, "coldouble", "colint", 2d, 999),
      new OtherColValueReplacer(dataModel, "colint", "colstring", 3, "new-value"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "ValueReplacer " should "replace also null values, with null value" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, "value1", 1d, 1, Some(1.0)),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, null, 2d, 2, None),
      ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, null, 3d, 3, None))
    val expectedDF = spark.createDataFrame(expectedData.toDF().rdd, testSchema)

    val transformedDF = testDF.enrichWithArlas(
      new OtherColValueReplacer(dataModel, "colnull", "colstring", null, null))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "ValueReplacer " should "fail with column type different from the expected one" in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(
                                                   new OtherColValueReplacer(dataModel, "coldouble", "coldouble", "value1", "new-value"))
                                               }
    assert(thrown.getMessage.equals("The column coldouble is expected to be of type string, current: double"))
  }

  "ValueReplacer " should "fail with unsupported data type" in {

    val thrown = intercept[DataFrameException] {
                                                 testDF.enrichWithArlas(
                                                   new OtherColValueReplacer(dataModel, "coldouble", "coldouble", new Exception, new Exception))
                                               }
    assert(thrown.getMessage.equals("Unsupported type java.lang.Exception"))
  }

}
