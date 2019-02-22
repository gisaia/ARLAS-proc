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

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import io.arlas.data.extract.transformations.{
  arlasPartitionColumn,
  arlasTimestampColumn,
  withArlasPartition,
  withArlasTimestamp
}
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.transform.transformations.{
  arlasSequenceIdColumn,
  doPipelineTransform
}
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class TransformationWithoutEdgingPeriodTest
    extends FlatSpec
    with Matchers
    with TestSparkSession
    with DataFrameTester {

  import spark.implicits._

  val timeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")
  timeFormatter.withZone(ZoneOffset.UTC)

  val source = testData

  val seq_A_1_first_30 = Seq(
    ("ObjectA",
     "01/06/2018 00:00:00+02:00",
     55.921028,
     17.320418,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:00:10+02:00",
     55.920875,
     17.319322,
     "ObjectA#1527804000")
  )

  val seq_A_1_middle = Seq(
    ("ObjectA",
     "01/06/2018 00:00:31+02:00",
     55.920583,
     17.31733,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:00:40+02:00",
     55.920437,
     17.316335,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:00:59+02:00",
     55.920162,
     17.314437,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:01:19+02:00",
     55.91987,
     17.312425,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:01:40+02:00",
     55.91956,
     17.310317,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:01:49+02:00",
     55.919417,
     17.30939,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:01:59+02:00",
     55.919267,
     17.308382,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:00+02:00",
     55.919267,
     17.308382,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:19+02:00",
     55.918982,
     17.306395,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:20+02:00",
     55.918982,
     17.306395,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:31+02:00",
     55.91882,
     17.305205,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:40+02:00",
     55.918697,
     17.304312,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:51+02:00",
     55.918558,
     17.303307,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:02:59+02:00",
     55.918435,
     17.302402,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:00+02:00",
     55.918435,
     17.302402,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:10+02:00",
     55.918285,
     17.301295,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:19+02:00",
     55.918163,
     17.300385,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:20+02:00",
     55.918163,
     17.300385,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:31+02:00",
     55.917997,
     17.29917,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:03:51+02:00",
     55.917727,
     17.29726,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:04:00+02:00",
     55.9176,
     17.296363,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:04:10+02:00",
     55.917447,
     17.295262,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:04:19+02:00",
     55.917322,
     17.294355,
     "ObjectA#1527804000")
  )

  val seq_A_1_last_30 = Seq(
    ("ObjectA",
     "01/06/2018 00:04:31+02:00",
     55.917155,
     17.293157,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:04:40+02:00",
     55.917027,
     17.292233,
     "ObjectA#1527804000"),
    ("ObjectA",
     "01/06/2018 00:04:51+02:00",
     55.916883,
     17.291198,
     "ObjectA#1527804000")
  )

  val seq_A_2_first_30 = Seq(
    ("ObjectA",
     "01/06/2018 00:10:01+02:00",
     55.912597,
     17.259977,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:10:01+02:00",
     55.912597,
     17.259977,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:10:11+02:00",
     55.912463,
     17.258973,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:10:21+02:00",
     55.912312,
     17.25786,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:10:30+02:00",
     55.91219,
     17.256948,
     "ObjectA#1527804601")
  )

  val seq_A_2_middle = Seq(
    ("ObjectA",
     "01/06/2018 00:10:41+02:00",
     55.912043,
     17.25584,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:10:51+02:00",
     55.911913,
     17.254835,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:01+02:00",
     55.911793,
     17.253932,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:01+02:00",
     55.911793,
     17.253932,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:11+02:00",
     55.911653,
     17.252918,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:19+02:00",
     55.911528,
     17.252012,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:30+02:00",
     55.911378,
     17.250905,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:39+02:00",
     55.911263,
     17.249997,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:11:51+02:00",
     55.911108,
     17.248792,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:12:01+02:00",
     55.910995,
     17.247897,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:12:11+02:00",
     55.91086,
     17.246888,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:12:19+02:00",
     55.910738,
     17.245978,
     "ObjectA#1527804601")
  )

  val seq_A_2_last_30 = Seq(
    ("ObjectA",
     "01/06/2018 00:12:30+02:00",
     55.910592,
     17.244872,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:12:39+02:00",
     55.910472,
     17.243963,
     "ObjectA#1527804601"),
    ("ObjectA",
     "01/06/2018 00:12:51+02:00",
     55.910308,
     17.242745,
     "ObjectA#1527804601")
  )

  val seq_B_1_first_30 = Seq(
    ("ObjectB",
     "01/06/2018 00:00:00+02:00",
     56.590177,
     11.830633,
     "ObjectB#1527804000"),
    ("ObjectB",
     "01/06/2018 00:00:10+02:00",
     56.590058,
     11.83063,
     "ObjectB#1527804000"),
    ("ObjectB",
     "01/06/2018 00:00:21+02:00",
     56.58993,
     11.830625,
     "ObjectB#1527804000"),
    ("ObjectB",
     "01/06/2018 00:00:29+02:00",
     56.589837,
     11.83062,
     "ObjectB#1527804000")
  )

  val seq_B_1_middle = Seq()

  val seq_B_1_last_30 = Seq(
    ("ObjectB",
     "01/06/2018 00:00:40+02:00",
     56.58971,
     11.830603,
     "ObjectB#1527804000"),
    ("ObjectB",
     "01/06/2018 00:00:50+02:00",
     56.589603,
     11.830595,
     "ObjectB#1527804000"),
    ("ObjectB",
     "01/06/2018 00:01:00+02:00",
     56.58949,
     11.83058,
     "ObjectB#1527804000")
  )

  val seq_B_2_first_30 = Seq(
    ("ObjectB",
     "01/06/2018 00:07:31+02:00",
     56.584978,
     11.830578,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:07:41+02:00",
     56.584867,
     11.830587,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:07:50+02:00",
     56.584767,
     11.830597,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:08:00+02:00",
     56.584652,
     11.830608,
     "ObjectB#1527804451")
  )

  val seq_B_2_middle = Seq(
    ("ObjectB",
     "01/06/2018 00:08:10+02:00",
     56.584535,
     11.83062,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:08:21+02:00",
     56.584412,
     11.830625,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:08:31+02:00",
     56.5843,
     11.830632,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:08:41+02:00",
     56.584183,
     11.830645,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:08:50+02:00",
     56.584083,
     11.830653,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:09:00+02:00",
     56.58398,
     11.830665,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:09:11+02:00",
     56.583827,
     11.830682,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:09:21+02:00",
     56.58372,
     11.830692,
     "ObjectB#1527804451")
  )

  val seq_B_2_last_30 = Seq(
    ("ObjectB",
     "01/06/2018 00:09:31+02:00",
     56.583603,
     11.830705,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:09:41+02:00",
     56.583485,
     11.83071,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:09:50+02:00",
     56.583383,
     11.830713,
     "ObjectB#1527804451"),
    ("ObjectB",
     "01/06/2018 00:10:00+02:00",
     56.58327,
     11.830705,
     "ObjectB#1527804451")
  )

  def performTestByPeriod(warmingPeriod: Int,
                          endingPeriod: Int,
                          expectedDF: DataFrame) = {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", sequenceGap = 300)

    val oldTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")
    val sourceDF = source.toDF("id", "timestamp", "lat", "lon")

    var runOptions = new RunOptions(
      source = "",
      target = "",
      start = Some(
        ZonedDateTime.parse("01/06/2018 00:00:00+02:00", oldTimeFormatter)),
      stop = Some(
        ZonedDateTime.parse("01/06/2018 00:15:00+02:00", oldTimeFormatter)),
      warmingPeriod = Some(warmingPeriod),
      endingPeriod = Some(endingPeriod)
    )

    val actualDF = sourceDF
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val transformedDf: DataFrame = doPipelineTransform(
      actualDF,
      new WithSequenceIdTransformer(dataModel),
      new WithoutEdgingPeriod(dataModel, runOptions, spark)
    ).drop(arlasTimestampColumn, arlasPartitionColumn)

    assertDataFrameEquality(transformedDf, expectedDF)

  }

  "withoutEdgingPeriod (warming period = 30 & ending period =30 ) transformation" should " filter data using both `warming/ending` periods against dataframe's sequences" in {
    val expectedDF =
      (seq_A_1_middle ++
        seq_A_2_middle ++
        seq_B_1_middle ++
        seq_B_2_middle).toDF("id",
                             "timestamp",
                             "lat",
                             "lon",
                             arlasSequenceIdColumn)

    performTestByPeriod(30, 30, expectedDF)
  }

  "withoutEdgingPeriod (warming period = 30 & ending period =0 ) transformation" should " filter data using only `warming` period against dataframe's sequences" in {
    val expectedDF =
      (seq_A_1_middle ++ seq_A_1_last_30 ++
        seq_A_2_middle ++ seq_A_2_last_30 ++
        seq_B_1_middle ++ seq_B_1_last_30 ++
        seq_B_2_middle ++ seq_B_2_last_30)
        .toDF("id", "timestamp", "lat", "lon", arlasSequenceIdColumn)

    performTestByPeriod(30, 0, expectedDF)
  }

  "withoutEdgingPeriod (warming period = 0 & ending period =30 ) transformation" should " filter data using only `ending` period against dataframe's sequences" in {
    val expectedDF =
      (seq_A_1_first_30 ++ seq_A_1_middle ++
        seq_A_2_first_30 ++ seq_A_2_middle ++
        seq_B_1_first_30 ++ seq_B_1_middle ++
        seq_B_2_first_30 ++ seq_B_2_middle)
        .toDF("id", "timestamp", "lat", "lon", arlasSequenceIdColumn)

    performTestByPeriod(0, 30, expectedDF)
  }

  "withoutEdgingPeriod (warming period=0 & ending period =0 ) transformation" should " keep the data as it is" in {
    val expectedDF =
      (seq_A_1_first_30 ++ seq_A_1_middle ++ seq_A_1_last_30 ++
        seq_A_2_first_30 ++ seq_A_2_middle ++ seq_A_2_last_30 ++
        seq_B_1_first_30 ++ seq_B_1_middle ++ seq_B_1_last_30 ++
        seq_B_2_first_30 ++ seq_B_2_middle ++ seq_B_2_last_30)
        .toDF("id", "timestamp", "lat", "lon", arlasSequenceIdColumn)

    performTestByPeriod(0, 0, expectedDF)
  }
}
