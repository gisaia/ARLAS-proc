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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model.DataModel
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.WithArlasVisibleSequence._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WithArlasVisibleSequenceTest extends ArlasTest {

  "WithArlasVisibleSequence transformation " should " fill/generate visible sequence id against dataframe's timeseries" in {

    val sourceDF = cleanedDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new WithArlasVisibleSequence(dataModel))

    val expectedDF = visibleSequencesDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithArlasVisibleSequence transformation " should " resume sequence id when adding a warm up period" in {

    val sourceDF = cleanedDF

    val warmupDF: DataFrame = sourceDF
      .filter(col(arlasTimestampColumn) < 1527804100)
      .enrichWithArlas(new WithArlasVisibleSequence(dataModel))

    val transformedDF: DataFrame = sourceDF
      .transform(withEmptyVisibileSequenceId())
      .transform(withEmptyVisibilityState())
      .filter(col(arlasTimestampColumn) >= 1527804100)
      .unionByName(warmupDF)
      .enrichWithArlas(new WithArlasVisibleSequence(dataModel))

    val expectedDF = visibleSequencesDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithArlasVisibleSequence transformation" should "be able to work with custom data model columns" in {

    val dataModel = DataModel(
      idColumn = "identifier",
      timestampColumn = "t",
      latColumn = "latitude",
      lonColumn = "longitude",
      dynamicFields = Array("latitude", "longitude"),
      timeFormat = "dd/MM/yyyy HH:mm:ssXXX",
      visibilityTimeout = 300
    )

    val sourceDF = cleanedDF
      .withColumnRenamed("id", "identifier")
      .withColumnRenamed("timestamp", "t")
      .withColumnRenamed("lat", "latitude")
      .withColumnRenamed("lon", "longitude")

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new WithArlasVisibleSequence(dataModel))

    val expectedDF = visibleSequencesDF
      .withColumnRenamed("id", "identifier")
      .withColumnRenamed("timestamp", "t")
      .withColumnRenamed("lat", "latitude")
      .withColumnRenamed("lon", "longitude")

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithArlasVisibleSequence transformation " should "consider timestamp without timezone as UTC" in {

    val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ss", visibilityTimeout = 300)
    val getNewTimestamp = udf((t: String) => {
      val oldTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")
      val newTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
      LocalDateTime
        .parse(t, oldTimeFormatter)
        .minusHours(2)
        .format(newTimeFormatter)
    })

    val sourceDF = cleanedDF.withColumn("timestamp", getNewTimestamp(col("timestamp")))

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new WithArlasVisibleSequence(dataModel))

    val expectedDF = visibleSequencesDF
      .withColumn("timestamp", getNewTimestamp(col("timestamp")))

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}