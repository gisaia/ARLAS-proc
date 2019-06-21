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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model.{DataModel}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._

class ArlasGolayResamplerTest
  extends ArlasTest {

  import spark.implicits._

  "withGolayFilter transformation" should " resample data against dataframe's sequences and smooth the lat/lon" in {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", timeSampling = 10)

    val timeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new ArlasGolayResampler(dataModel, spark, Some(ZonedDateTime.parse("01/06/2018 00:00:00+02:00", timeFormatter)), arlasVisibleSequenceIdColumn))
//      .drop(arlasTimestampColumn, arlasPartitionColumn)

    //force to compute the result in order to check it
    visibleSequencesDF.show(500, false)
    transformedDf.sort(dataModel.idColumn, arlasTimestampColumn).show(500, false)
    assert(transformedDf.schema.fieldNames.contains("dlat"))
    assert(transformedDf.schema.fieldNames.contains("ddlat"))
    assert(transformedDf.schema.fieldNames.contains("dlon"))
    assert(transformedDf.schema.fieldNames.contains("ddlon"))
  }

}
