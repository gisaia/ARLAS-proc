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

class SameColValuesMapperTest extends ArlasTest {

  import spark.implicits._

  val testSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("speed", DoubleType, true),
      StructField("sourcestring", StringType, true)
    ))

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, "test"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, "test"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601, "other-test"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, 0.180505395097491, "out")
  )

  val testDF = spark.createDataFrame(testData.toDF().rdd, testSchema)

  "WithValuesMapper " should "replace values of type string in same column" in {

    val expectedData = Seq(
      ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533, "success"),
      ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024, "success"),
      ("ObjectA",
       "01/06/2018 00:00:31+02:00",
       55.920583,
       17.31733,
       0.178408676103601,
       "other-success"),
      ("ObjectB", "01/06/2018 00:00:10+02:00", 55.920437, 17.316335, 0.180505395097491, "out")
    )

    val transformedDF = testDF.enrichWithArlas(
      new OtherColValuesMapper(dataModel,
                               "sourcestring",
                               "sourcestring",
                               Map("test" -> "success", "other-test" -> "other-success")))

    assertDataFrameEquality(transformedDF,
                            spark.createDataFrame(expectedData.toDF().rdd, testSchema))
  }

}
