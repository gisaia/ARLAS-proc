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

class ColumnsRenamerTest extends ArlasTest {

  import spark.implicits._

  "ColumnsRenamer" should "rename columns " in {

    val testDF = rawDF.enrichWithArlas(
      new ColumnsRenamer(dataModel, Map("lat" -> "latitude", "lon" -> "longitude")))

    assert(testDF.schema.fieldNames.contains(dataModel.idColumn))
    assert(testDF.schema.fieldNames.contains(dataModel.timestampColumn))
    assert(testDF.schema.fieldNames.contains("latitude"))
    assert(testDF.schema.fieldNames.contains("longitude"))

    assert(!testDF.schema.fieldNames.contains("lat"))
    assert(!testDF.schema.fieldNames.contains("lon"))


  }

  "ColumnsRenamer" should "fail because column to rename doesn't exist" in {

    val thrown = intercept[DataFrameException] {
                                                 rawDF.enrichWithArlas(
                                                   new ColumnsRenamer(dataModel, Map("unknown" -> "alsounknown", "stillunkown" -> "alsostillunknown")))
                                               }

    assert(thrown.getMessage.equals("The unknown, stillunkown columns are not included in the DataFrame with the following columns id, timestamp, lat, lon, speed"))
  }

}
