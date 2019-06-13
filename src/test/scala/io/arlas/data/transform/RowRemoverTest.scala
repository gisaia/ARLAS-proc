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

class RowRemoverTest extends ArlasTest {

  import spark.implicits._

  val testData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601),
    ("ObjectB", "01/06/2018 00:01:59+02:00", 55.919267, 17.308382, 20.4161902434555753),
    ("ObjectB", "01/06/2018 00:02:00+02:00", 55.919267, 17.308382, 20.4670321139802))

  val testDF     = spark.createDataFrame(testData.toDF().rdd, rawSchema)

  "RowRemover " should "remove rows with given string value" in {

    val transformedDF = testDF.enrichWithArlas(
      new RowRemover(dataModel, dataModel.idColumn, "ObjectB"))

    assert (transformedDF.count() == 3)
  }

  "RowRemover " should "remove rows with given double value" in {

    val transformedDF = testDF.enrichWithArlas(
      new RowRemover(dataModel, dataModel.latColumn, 55.921028))

    assert (transformedDF.count() == 4)
  }



}
