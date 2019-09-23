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
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.VisibilityChange._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}

class WithFragmentVisibilityFromTempoTest extends ArlasTest {

  import spark.implicits._

  val expectedSchema = tempoSchema
    .add(StructField(arlasTrackVisibilityProportion, DoubleType, false))
    .add(StructField(arlasTrackVisibilityChange, StringType, true))

  // insert new irregular tempo to tests APPEAR_DISAPPEAR
  val updatedTempoData = tempoData.map {
    case (id, date, lat, lon, speed, partition, timestamp, tempo) => {
      val newTempo =
        if ((id.equals("ObjectA") && date.equals("01/06/2018 00:12:30+02:00")) || (id.equals(
              "ObjectA") && date.equals("01/06/2018 00:12:51+02:00"))) irregularTempo
        else tempo
      (id, date, lat, lon, speed, partition, timestamp, newTempo)
    }
  }

  val updatedTempoDF = spark.createDataFrame(
    updatedTempoData.toDF.rdd,
    tempoSchema
  )

  val expectedData = updatedTempoData.map {
    case (id, date, lat, lon, speed, partition, timestamp, tempo) => {
      val visibilityProportion = if (tempo.equals(irregularTempo)) 0.0d else 1.0d
      val visibilityChange =
        if (visibilityProportion.equals(0)) null
        else if ((id.equals("ObjectA") && date.equals("01/06/2018 00:04:51+02:00")) ||
                 (id.equals("ObjectB") && date.equals("01/06/2018 00:01:00+02:00")) ||
                 (id.equals("ObjectA") && date.equals("01/06/2018 00:12:19+02:00")))
          DISAPPEAR
        else if (date.equals("01/06/2018 00:00:10+02:00") ||
                 (id.equals("ObjectA") && date.equals("01/06/2018 00:10:11+02:00")) ||
                 (id.equals("ObjectB") && date.equals("01/06/2018 00:07:41+02:00")))
          APPEAR
        else if (id.equals("ObjectA") && date.equals("01/06/2018 00:12:39+02:00"))
          APPEAR_DISAPPEAR
        else null

      (id,
       date,
       lat,
       lon,
       speed,
       partition,
       timestamp,
       tempo,
       visibilityProportion,
       visibilityChange)
    }
  }

  val expectedDF = spark.createDataFrame(
    expectedData.toDF.rdd,
    expectedSchema
  )

  "WithFragmentVisibilityFromTempo " should "add visibility proportion and change relying to tempo" in {

    val transformedDF = updatedTempoDF
      .enrichWithArlas(new WithFragmentVisibilityFromTempo(dataModel, spark, irregularTempo))

    assertDataFrameEquality(
      transformedDF,
      expectedDF
    )
  }
}
