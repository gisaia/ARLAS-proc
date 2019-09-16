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
import org.apache.spark.sql.types.{StringType, StructField}

class WithArlasVisibilityStateFromTempoTest extends ArlasTest {

  import spark.implicits._

  val expectedSchema = tempoSchema
    .add(StructField(arlasVisibilityStateColumn, StringType, false))

  val expectedData = tempoData.map {
    case (id, date, lat, lon, speed, partition, timestamp, tempo) => {
      val visibility =
        if (tempo.equals(appearTempo)) ArlasVisibilityStates.APPEAR.toString
        else if ((id.equals("ObjectA") && date.equals("01/06/2018 00:04:51+02:00")) ||
                 (id.equals("ObjectB") && date.equals("01/06/2018 00:01:00+02:00")))
          ArlasVisibilityStates.DISAPPEAR.toString
        else ArlasVisibilityStates.VISIBLE.toString

      (id, date, lat, lon, speed, partition, timestamp, tempo, visibility)
    }
  }

  val expectedDF = spark.createDataFrame(
    expectedData.toDF.rdd,
    expectedSchema
  )

  "WithArlasVisibilityStateFromTempo " should "add visibility state related to the tempo" in {

    val transformedDF = tempoDF
      .enrichWithArlas(new WithArlasVisibilityStateFromTempo(dataModel, spark, appearTempo))

    assertDataFrameEquality(
      transformedDF,
      expectedDF
    )
  }

}
