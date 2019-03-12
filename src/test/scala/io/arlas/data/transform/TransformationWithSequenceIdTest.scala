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

import io.arlas.data.extract.transformations.{
  arlasPartitionColumn,
  arlasTimestampColumn,
  withArlasPartition,
  withArlasTimestamp
}
import io.arlas.data.transform.transformations._
import io.arlas.data.model.DataModel
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.scalatest.{FlatSpec, Matchers}

class TransformationWithSequenceIdTest
    extends FlatSpec
    with Matchers
    with TestSparkSession
    with DataFrameTester {

  import spark.implicits._

  var source = Seq(
    // null ==> WithSequenceId transformation should generate new sequenceId
    ("02/01/2018 00:00:00+02:00", 265513250, 55.915727, 12.621154, null),
    ("02/01/2018 00:00:30+02:00", 265513250, 55.916692, 12.620489, null),
    ("02/01/2018 00:01:23+02:00", 265513250, 55.918345, 12.629845, null),
    ("02/01/2018 00:01:59+02:00", 265513250, 55.913415, 12.635524, null),
    ("02/01/2018 03:00:00+02:00", 265513250, 55.925727, 12.651154, "265513250#1514862000"),
    ("02/01/2018 03:00:12+02:00", 265513250, 55.926692, 12.650489, "265513250#1514862000"),
    ("02/01/2018 03:00:23+02:00", 265513250, 55.928345, 12.649845, null),
    ("02/01/2018 03:00:34+02:00", 265513250, 55.973415, 12.645524, null),
    // existence sequenceId ==> WithSequenceId transformation should not overwrite this; NOTE: 1514862999 is wrong timestamp value
    ("02/01/2018 03:06:11+02:00", 265513260, 56.013067, 12.644289, "265513260#1514862999"),
    ("02/01/2018 03:06:23+02:00", 265513260, 56.035357, 12.645459, null),
    ("02/01/2018 03:06:34+02:00", 265513260, 56.073053, 12.585218, null),
    ("02/01/2018 03:06:41+02:00", 265513260, 56.118303, 12.497399, null),
    ("02/01/2018 03:06:44+02:00", 265513260, 56.169133, 12.29887, null),
    ("02/01/2018 03:06:51+02:00", 265513260, 56.170132, 12.058547, null),
    ("02/01/2018 03:09:13+02:00", 265513260, 56.174596, 11.885253, null),
    ("02/01/2018 03:09:17+02:00", 265513260, 56.11243, 11.666634, null),
    ("02/01/2018 03:09:22+02:00", 265513260, 56.148124, 11.390748, null),
    ("02/01/2018 03:09:26+02:00", 265513260, 56.080822, 11.026482, null)
  )

  var expected = Seq(
    ("02/01/2018 00:00:00+02:00", 265513250, 55.915727, 12.621154, "265513250#1514844000"),
    ("02/01/2018 00:00:30+02:00", 265513250, 55.916692, 12.620489, "265513250#1514844000"),
    ("02/01/2018 00:01:23+02:00", 265513250, 55.918345, 12.629845, "265513250#1514844000"),
    ("02/01/2018 00:01:59+02:00", 265513250, 55.913415, 12.635524, "265513250#1514844000"),
    ("02/01/2018 03:00:00+02:00", 265513250, 55.925727, 12.651154, "265513250#1514862000"),
    ("02/01/2018 03:00:12+02:00", 265513250, 55.926692, 12.650489, "265513250#1514862000"),
    ("02/01/2018 03:00:23+02:00", 265513250, 55.928345, 12.649845, "265513250#1514862000"),
    ("02/01/2018 03:00:34+02:00", 265513250, 55.973415, 12.645524, "265513250#1514862000"),
    ("02/01/2018 03:06:11+02:00", 265513260, 56.013067, 12.644289, "265513260#1514862999"),
    ("02/01/2018 03:06:23+02:00", 265513260, 56.035357, 12.645459, "265513260#1514862999"),
    ("02/01/2018 03:06:34+02:00", 265513260, 56.073053, 12.585218, "265513260#1514862999"),
    ("02/01/2018 03:06:41+02:00", 265513260, 56.118303, 12.497399, "265513260#1514862999"),
    ("02/01/2018 03:06:44+02:00", 265513260, 56.169133, 12.29887, "265513260#1514862999"),
    ("02/01/2018 03:06:51+02:00", 265513260, 56.170132, 12.058547, "265513260#1514862999"),
    ("02/01/2018 03:09:13+02:00", 265513260, 56.174596, 11.885253, "265513260#1514855353"),
    ("02/01/2018 03:09:17+02:00", 265513260, 56.11243, 11.666634, "265513260#1514855353"),
    ("02/01/2018 03:09:22+02:00", 265513260, 56.148124, 11.390748, "265513260#1514855353"),
    ("02/01/2018 03:09:26+02:00", 265513260, 56.080822, 11.026482, "265513260#1514855353")
  )

  "fillSequenceId transformation " should " fill/generate sequence id against dataframe's sequences" in {

    val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", sequenceGap = 120)

    val sourceDF = source
      .toDF("timestamp", "id", "lat", "lon", arlasSequenceIdColumn)
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val transformedDF: DataFrame = doPipelineTransform(
      sourceDF,
      new WithSequenceId(dataModel)
    ).drop(arlasTimestampColumn, arlasPartitionColumn)

    val expectedDF = expected.toDF("timestamp", "id", "lat", "lon", arlasSequenceIdColumn)

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withSequenceId transformation" should "be able to work with custom data model columns" in {

    val dataModel = DataModel(
      idColumn = "identifier",
      timestampColumn = "t",
      latColumn = "latitude",
      lonColumn = "longitude",
      dynamicFields = Array("latitude", "longitude"),
      timeFormat = "dd/MM/yyyy HH:mm:ssXXX",
      sequenceGap = 120
    )

    val sourceDF = source
      .toDF("t", "identifier", "latitude", "longitude", arlasSequenceIdColumn)
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val transformedDF: DataFrame = doPipelineTransform(
      sourceDF,
      new WithSequenceId(dataModel)
    ).drop(arlasTimestampColumn, arlasPartitionColumn)

    val expectedDF =
      expected.toDF("t", "identifier", "latitude", "longitude", arlasSequenceIdColumn)

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withSequenceId transformation " should "consider timestamp without timezone as UTC" in {

    val oldTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")
    val newTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")

    val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ss", sequenceGap = 120)

    val sourceDF = source
      .map(
        row =>
          (LocalDateTime
             .parse(row._1, oldTimeFormatter)
             .minusHours(2)
             .format(newTimeFormatter),
           row._2,
           row._3,
           row._4,
           row._5))
      .toDF("timestamp", "id", "lat", "lon", arlasSequenceIdColumn)
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val transformedDF: DataFrame = doPipelineTransform(
      sourceDF,
      new WithSequenceId(dataModel)
    ).drop(arlasTimestampColumn, arlasPartitionColumn)

    val expectedDF = expected
      .map(
        row =>
          (LocalDateTime
             .parse(row._1, oldTimeFormatter)
             .minusHours(2)
             .format(newTimeFormatter),
           row._2,
           row._3,
           row._4,
           row._5))
      .toDF("timestamp", "id", "lat", "lon", arlasSequenceIdColumn)

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}
