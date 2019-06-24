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
import io.arlas.data.model.DataModel
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StringType, StructField, StructType}

class EdgingPeriodRemoverTest extends ArlasTest {

  import spark.implicits._

  val seq_A_1_first_30 = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", "ObjectA#1527804000")
  )

  val seq_A_1_middle = Seq(
    ("ObjectA", "01/06/2018 00:00:31+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:01:19+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:01:40+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:01:49+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:01:59+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:00+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:19+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:20+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:31+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:40+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:51+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:02:59+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:00+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:10+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:19+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:20+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:31+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:03:51+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:04:00+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:04:10+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:04:19+02:00", "ObjectA#1527804000")
  )

  val seq_A_1_last_30 = Seq(
    ("ObjectA", "01/06/2018 00:04:31+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:04:40+02:00", "ObjectA#1527804000"),
    ("ObjectA", "01/06/2018 00:04:51+02:00", "ObjectA#1527804000")
  )

  val seq_A_2_first_30 = Seq(
    ("ObjectA", "01/06/2018 00:10:01+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:10:11+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:10:21+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:10:30+02:00", "ObjectA#1527804601")
  )

  val seq_A_2_middle = Seq(
    ("ObjectA", "01/06/2018 00:10:41+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:10:51+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:01+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:11+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:19+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:30+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:39+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:11:51+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:12:01+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:12:11+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:12:19+02:00", "ObjectA#1527804601")
  )

  val seq_A_2_last_30 = Seq(
    ("ObjectA", "01/06/2018 00:12:30+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:12:39+02:00", "ObjectA#1527804601"),
    ("ObjectA", "01/06/2018 00:12:51+02:00", "ObjectA#1527804601")
  )

  val seq_B_1_first_30 = Seq(
    ("ObjectB", "01/06/2018 00:00:00+02:00", "ObjectB#1527804000"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", "ObjectB#1527804000"),
    ("ObjectB", "01/06/2018 00:00:21+02:00", "ObjectB#1527804000"),
    ("ObjectB", "01/06/2018 00:00:29+02:00", "ObjectB#1527804000")
  )

  val seq_B_1_middle = Seq()

  val seq_B_1_last_30 = Seq(
    ("ObjectB", "01/06/2018 00:00:40+02:00", "ObjectB#1527804000"),
    ("ObjectB", "01/06/2018 00:00:50+02:00", "ObjectB#1527804000"),
    ("ObjectB", "01/06/2018 00:01:00+02:00", "ObjectB#1527804000")
  )

  val seq_B_2_first_30 = Seq(
    ("ObjectB", "01/06/2018 00:07:31+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:07:41+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:07:50+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:08:00+02:00", "ObjectB#1527804451")
  )

  val seq_B_2_middle = Seq(
    ("ObjectB", "01/06/2018 00:08:10+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:08:21+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:08:31+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:08:41+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:08:50+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:09:00+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:09:11+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:09:21+02:00", "ObjectB#1527804451")
  )

  val seq_B_2_last_30 = Seq(
    ("ObjectB", "01/06/2018 00:09:31+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:09:41+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:09:50+02:00", "ObjectB#1527804451"),
    ("ObjectB", "01/06/2018 00:10:00+02:00", "ObjectB#1527804451")
  )

  val schema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField(arlasVisibleSequenceIdColumn, StringType, true)
      )
    )

  def performTestByPeriod(warmingPeriod: Int, endingPeriod: Int, expectedDF: DataFrame) = {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", visibilityTimeout = 300)

    val oldTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")

    val transformedDf: DataFrame = rawDF
      .asArlasCleanedData(dataModel)
      .asArlasVisibleSequencesFromTimestamp(dataModel)
      .enrichWithArlas(
        new EdgingPeriodRemover(dataModel, Some(warmingPeriod), Some(endingPeriod), spark))
      .select("id", "timestamp", arlasVisibleSequenceIdColumn)

    assertDataFrameEquality(transformedDf, expectedDF)

  }

  "withoutEdgingPeriod (warming period = 30 & ending period =30 ) transformation" should " filter data using both `warming/ending` periods against dataframe's visible sequences" in {
    val expectedDF = spark.createDataFrame(
      (seq_A_1_middle ++
        seq_A_2_middle ++
        seq_B_1_middle ++
        seq_B_2_middle)
        .toDF("id", "timestamp", arlasVisibleSequenceIdColumn)
        .rdd,
      schema
    )
    performTestByPeriod(30, 30, expectedDF)
  }

  "withoutEdgingPeriod (warming period = 30 & ending period =0 ) transformation" should " filter data using only `warming` period against dataframe's visible sequences" in {
    val expectedDF = spark.createDataFrame(
      (seq_A_1_middle ++ seq_A_1_last_30 ++
        seq_A_2_middle ++ seq_A_2_last_30 ++
        seq_B_1_middle ++ seq_B_1_last_30 ++
        seq_B_2_middle ++ seq_B_2_last_30)
        .toDF("id", "timestamp", arlasVisibleSequenceIdColumn)
        .rdd,
      schema
    )
    performTestByPeriod(30, 0, expectedDF)
  }

  "withoutEdgingPeriod (warming period = 0 & ending period =30 ) transformation" should " filter data using only `ending` period against dataframe's visible sequences" in {
    val expectedDF = spark.createDataFrame(
      (seq_A_1_first_30 ++ seq_A_1_middle ++
        seq_A_2_first_30 ++ seq_A_2_middle ++
        seq_B_1_first_30 ++ seq_B_1_middle ++
        seq_B_2_first_30 ++ seq_B_2_middle)
        .toDF("id", "timestamp", arlasVisibleSequenceIdColumn)
        .rdd,
      schema
    )
    performTestByPeriod(0, 30, expectedDF)
  }

  "withoutEdgingPeriod (warming period=0 & ending period =0 ) transformation" should " keep the data as it is" in {
    val expectedDF = spark.createDataFrame(
      (seq_A_1_first_30 ++ seq_A_1_middle ++ seq_A_1_last_30 ++
        seq_A_2_first_30 ++ seq_A_2_middle ++ seq_A_2_last_30 ++
        seq_B_1_first_30 ++ seq_B_1_middle ++ seq_B_1_last_30 ++
        seq_B_2_first_30 ++ seq_B_2_middle ++ seq_B_2_last_30)
        .toDF("id", "timestamp", arlasVisibleSequenceIdColumn)
        .rdd,
      schema
    )
    performTestByPeriod(0, 0, expectedDF)
  }
}
