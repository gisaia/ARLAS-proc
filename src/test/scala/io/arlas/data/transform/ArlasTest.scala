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

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

trait ArlasTest extends FlatSpec with Matchers with TestSparkSession with DataFrameTester {

  val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", visibilityTimeout = 300)

  val rawData = Seq(
    //ObjectA : first time serie
    ("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437),
    ("ObjectA", "01/06/2018 00:01:19+02:00", 55.91987, 17.312425),
    ("ObjectA", "01/06/2018 00:01:40+02:00", 55.91956, 17.310317),
    ("ObjectA", "01/06/2018 00:01:49+02:00", 55.919417, 17.30939),
    ("ObjectA", "01/06/2018 00:01:59+02:00", 55.919267, 17.308382),
    ("ObjectA", "01/06/2018 00:02:00+02:00", 55.919267, 17.308382),
    ("ObjectA", "01/06/2018 00:02:19+02:00", 55.918982, 17.306395),
    ("ObjectA", "01/06/2018 00:02:20+02:00", 55.918982, 17.306395),
    ("ObjectA", "01/06/2018 00:02:31+02:00", 55.91882, 17.305205),
    ("ObjectA", "01/06/2018 00:02:40+02:00", 55.918697, 17.304312),
    ("ObjectA", "01/06/2018 00:02:51+02:00", 55.918558, 17.303307),
    ("ObjectA", "01/06/2018 00:02:59+02:00", 55.918435, 17.302402),
    ("ObjectA", "01/06/2018 00:03:00+02:00", 55.918435, 17.302402),
    ("ObjectA", "01/06/2018 00:03:10+02:00", 55.918285, 17.301295),
    ("ObjectA", "01/06/2018 00:03:19+02:00", 55.918163, 17.300385),
    ("ObjectA", "01/06/2018 00:03:20+02:00", 55.918163, 17.300385),
    ("ObjectA", "01/06/2018 00:03:31+02:00", 55.917997, 17.29917),
    ("ObjectA", "01/06/2018 00:03:51+02:00", 55.917727, 17.29726),
    ("ObjectA", "01/06/2018 00:04:00+02:00", 55.9176, 17.296363),
    ("ObjectA", "01/06/2018 00:04:10+02:00", 55.917447, 17.295262),
    ("ObjectA", "01/06/2018 00:04:19+02:00", 55.917322, 17.294355),
    ("ObjectA", "01/06/2018 00:04:31+02:00", 55.917155, 17.293157),
    ("ObjectA", "01/06/2018 00:04:40+02:00", 55.917027, 17.292233),
    ("ObjectA", "01/06/2018 00:04:51+02:00", 55.916883, 17.291198),
    //ObjectA : second time serie
    ("ObjectA", "01/06/2018 00:10:01+02:00", 55.912597, 17.259977),
    ("ObjectA", "01/06/2018 00:10:11+02:00", 55.912463, 17.258973),
    ("ObjectA", "01/06/2018 00:10:21+02:00", 55.912312, 17.25786),
    ("ObjectA", "01/06/2018 00:10:30+02:00", 55.91219, 17.256948),
    ("ObjectA", "01/06/2018 00:10:41+02:00", 55.912043, 17.25584),
    ("ObjectA", "01/06/2018 00:10:51+02:00", 55.911913, 17.254835),
    ("ObjectA", "01/06/2018 00:11:01+02:00", 55.911793, 17.253932),
    ("ObjectA", "01/06/2018 00:11:11+02:00", 55.911653, 17.252918),
    ("ObjectA", "01/06/2018 00:11:19+02:00", 55.911528, 17.252012),
    ("ObjectA", "01/06/2018 00:11:30+02:00", 55.911378, 17.250905),
    ("ObjectA", "01/06/2018 00:11:39+02:00", 55.911263, 17.249997),
    ("ObjectA", "01/06/2018 00:11:51+02:00", 55.911108, 17.248792),
    ("ObjectA", "01/06/2018 00:12:01+02:00", 55.910995, 17.247897),
    ("ObjectA", "01/06/2018 00:12:11+02:00", 55.91086, 17.246888),
    ("ObjectA", "01/06/2018 00:12:19+02:00", 55.910738, 17.245978),
    ("ObjectA", "01/06/2018 00:12:30+02:00", 55.910592, 17.244872),
    ("ObjectA", "01/06/2018 00:12:39+02:00", 55.910472, 17.243963),
    ("ObjectA", "01/06/2018 00:12:51+02:00", 55.910308, 17.242745),
    //Object B : first time serie
    ("ObjectB", "01/06/2018 00:00:00+02:00", 56.590177, 11.830633),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 56.590058, 11.83063),
    ("ObjectB", "01/06/2018 00:00:21+02:00", 56.58993, 11.830625),
    ("ObjectB", "01/06/2018 00:00:29+02:00", 56.589837, 11.83062),
    ("ObjectB", "01/06/2018 00:00:40+02:00", 56.58971, 11.830603),
    ("ObjectB", "01/06/2018 00:00:50+02:00", 56.589603, 11.830595),
    ("ObjectB", "01/06/2018 00:01:00+02:00", 56.58949, 11.83058),
    //Object B : second time serie
    ("ObjectB", "01/06/2018 00:07:31+02:00", 56.584978, 11.830578),
    ("ObjectB", "01/06/2018 00:07:41+02:00", 56.584867, 11.830587),
    ("ObjectB", "01/06/2018 00:07:50+02:00", 56.584767, 11.830597),
    ("ObjectB", "01/06/2018 00:08:00+02:00", 56.584652, 11.830608),
    ("ObjectB", "01/06/2018 00:08:10+02:00", 56.584535, 11.83062),
    ("ObjectB", "01/06/2018 00:08:21+02:00", 56.584412, 11.830625),
    ("ObjectB", "01/06/2018 00:08:31+02:00", 56.5843, 11.830632),
    ("ObjectB", "01/06/2018 00:08:41+02:00", 56.584183, 11.830645),
    ("ObjectB", "01/06/2018 00:08:50+02:00", 56.584083, 11.830653),
    ("ObjectB", "01/06/2018 00:09:00+02:00", 56.58398, 11.830665),
    ("ObjectB", "01/06/2018 00:09:11+02:00", 56.583827, 11.830682),
    ("ObjectB", "01/06/2018 00:09:21+02:00", 56.58372, 11.830692),
    ("ObjectB", "01/06/2018 00:09:31+02:00", 56.583603, 11.830705),
    ("ObjectB", "01/06/2018 00:09:41+02:00", 56.583485, 11.83071),
    ("ObjectB", "01/06/2018 00:09:50+02:00", 56.583383, 11.830713),
    ("ObjectB", "01/06/2018 00:10:00+02:00", 56.58327, 11.830705)
  )

  import spark.implicits._

  val rawSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true)
    )
  )

  val rawDF = spark.createDataFrame(
    rawData.toDF.rdd,
    rawSchema
  )

  val cleanedSchema = rawSchema
    .add(StructField(arlasPartitionColumn, IntegerType, false))
    .add(StructField(arlasTimestampColumn, LongType, false))

  val cleanedData = rawData.map {
    case (id, date, lat, lon) => {
      val timeFormatter = DateTimeFormatter.ofPattern(dataModel.timeFormat)
      val timestamp = ZonedDateTime.parse(date, timeFormatter).toEpochSecond
      val partitionTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val partition = ZonedDateTime.parse(date, timeFormatter).format(partitionTimeFormatter).toInt
      (id, date, lat, lon, partition, timestamp)
    }
  }

  val cleanedDF = spark.createDataFrame(
    cleanedData.toDF.rdd,
    cleanedSchema
  )

  val visibleSequencesSchema = cleanedSchema
    .add(StructField(arlasVisibleSequenceIdColumn, StringType, true))
    .add(StructField(arlasVisibilityStateColumn, StringType, true))

  val visibleSequencesData = cleanedData.map {
    case (id, date, lat, lon, partition, timestamp) => {
      val sequence =
        if (id.equals("ObjectA") && timestamp < 1527804601) "ObjectA#1527804000"
        else if (id.equals("ObjectA") && timestamp >= 1527804601) "ObjectA#1527804601"
        else if (id.equals("ObjectB") && timestamp < 1527804451) "ObjectB#1527804000"
        else if (id.equals("ObjectB") && timestamp >= 1527804451) "ObjectB#1527804451"
        else "N/A"
      val visibility =
        if (id.equals("ObjectA") && (date.equals("01/06/2018 00:04:51+02:00") || date.equals(
              "01/06/2018 00:12:51+02:00"))) "DISAPPEAR"
        else if (id.equals("ObjectA") && (date.equals("01/06/2018 00:10:01+02:00") || date.equals(
                   "01/06/2018 00:00:00+02:00"))) "APPEAR"
        else if (id.equals("ObjectB") && date.equals("01/06/2018 00:01:00+02:00") || date.equals(
                   "01/06/2018 00:10:00+02:00")) "DISAPPEAR"
        else if (id.equals("ObjectB") && (date.equals("01/06/2018 00:07:31+02:00") || date.equals(
                   "01/06/2018 00:00:00+02:00"))) "APPEAR"
        else "VISIBLE"

      (id, date, lat, lon, partition, timestamp, sequence, visibility)
    }
  }

  val visibleSequencesDF = spark.createDataFrame(
    visibleSequencesData.toDF.rdd,
    visibleSequencesSchema
  )
}
