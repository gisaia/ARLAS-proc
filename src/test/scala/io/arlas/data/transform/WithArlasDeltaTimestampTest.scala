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

import org.apache.spark.sql.{DataFrame, Row}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{LongType, StructField}

class WithArlasDeltaTimestampTest extends ArlasTest {

  val deltaTimestampSchema = visibleSequencesSchema
    .add(StructField(arlasDeltaTimestampColumn, LongType, true))
    .add(StructField(arlasPreviousDeltaTimestampColumn, LongType, true))
    .add(StructField(arlasDeltaTimestampVariationColumn, LongType, true))

  val deltaTimestampData = visibleSequencesData
    .groupBy(_._7)
    .flatMap(f => {
      val data = f._2
      Row(data(0)._1,
          data(0)._2,
          data(0)._3,
          data(0)._4,
          data(0)._5,
          data(0)._6,
          data(0)._7,
          data(0)._8,
          null,
          null,
          null) +: Row(data(1)._1,
                       data(1)._2,
                       data(1)._3,
                       data(1)._4,
                       data(1)._5,
                       data(1)._6,
                       data(1)._7,
                       data(1)._8,
                       data(1)._6 - data(0)._6,
                       null,
                       null) +: data
        .sliding(3)
        .map(window =>
               Row(
                 window(2)._1,
                 window(2)._2,
                 window(2)._3,
                 window(2)._4,
                 window(2)._5,
                 window(2)._6,
                 window(2)._7,
                 window(2)._8,
                 window(2)._6 - window(1)._6,
                 window(1)._6 - window(0)._6,
                 window(2)._6 - window(1)._6 - (window(1)._6 - window(0)._6)
                 ))
        .toSeq
    })
    .toSeq

  val deltaTimestampDF = spark.createDataFrame(
    spark.sparkContext.parallelize(deltaTimestampData),
    deltaTimestampSchema
    )

  "WithArlasDeltaTimestamp transformation" should "fill the arlasDeltaTimestamp against dataframe's timeseries" in {

    val sourceDF = visibleSequencesDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new WithArlasDeltaTimestamp(dataModel, spark, arlasVisibleSequenceIdColumn))

    val expectedDF = deltaTimestampDF
    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
