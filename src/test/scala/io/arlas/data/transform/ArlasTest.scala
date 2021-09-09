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
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.features._
import io.arlas.data.transform.testdata._
import io.arlas.data.transform.timeseries._
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.immutable.ListMap

trait ArlasTest extends AnyFlatSpec with Matchers with TestSparkSession with DataFrameTester {

  val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX")
  val speedColumn = "speed"
  val standardDeviationEllipsisNbPoints = 12
  val tempo1s = "tempo_1s"
  val tempo10s = "tempo_10s"
  val tempo20s = "tempo_20s"
  val tempoIrregular = "tempo_irregular"
  val tempoProportion1s = "tempo_proportion_1s"
  val tempoProportion10s = "tempo_proportion_10s"
  val tempoProportion20s = "tempo_proportion_20s"
  val tempoProportionIrregular = "tempo_proportion_irregular"
  val tempoProportionsColumns = Map(
    tempoProportion1s -> tempo1s,
    tempoProportion10s -> tempo10s,
    tempoProportion20s -> tempo20s,
    tempoProportionIrregular -> tempoIrregular
  )

  val averagedColumns = List(speedColumn)

  val baseTestDF = {
    val timestampUDF = udf((s: String) => ZonedDateTime.parse(s, DateTimeFormatter.ofPattern(dataModel.timeFormat)).toEpochSecond)

    createDataFrameWithTypes(
      spark,
      List(
        Seq("ObjectA", "01/06/2018 00:00:00+02:00", 55.921028, 17.320418, 0.280577132616533),
        Seq("ObjectA", "01/06/2018 00:00:10+02:00", 55.920875, 17.319322, 0.032068662532024),
        Seq("ObjectA", "01/06/2018 00:00:31+02:00", 55.920583, 17.31733, 0.178408676103601),
        Seq("ObjectA", "01/06/2018 00:00:40+02:00", 55.920437, 17.316335, 0.180505395097491),
        Seq("ObjectA", "01/06/2018 00:00:59+02:00", 55.920162, 17.314437, 18.3267208898955),
        Seq("ObjectA", "01/06/2018 00:01:19+02:00", 55.91987, 17.312425, 25.3224385919895),
        Seq("ObjectA", "01/06/2018 00:01:40+02:00", 55.91956, 17.310317, 33.3774309272405),
        Seq("ObjectA", "01/06/2018 00:01:49+02:00", 55.919417, 17.30939, 22.276237318997),
        Seq("ObjectA", "01/06/2018 00:01:59+02:00", 55.919267, 17.308382, 20.4161902434555),
        Seq("ObjectA", "01/06/2018 00:02:00+02:00", 55.919267, 17.308382, 20.4670321139802),
        Seq("ObjectA", "01/06/2018 00:02:19+02:00", 55.918982, 17.306395, 20.3483994565575),
        Seq("ObjectA", "01/06/2018 00:02:20+02:00", 55.918982, 17.306395, 30.4938344029634),
        Seq("ObjectA", "01/06/2018 00:02:31+02:00", 55.91882, 17.305205, 30.3776738025963),
        Seq("ObjectA", "01/06/2018 00:02:40+02:00", 55.918697, 17.304312, 30.0678204838492),
        Seq("ObjectA", "01/06/2018 00:02:51+02:00", 55.918558, 17.303307, 30.1765108328227),
        Seq("ObjectA", "01/06/2018 00:02:59+02:00", 55.918435, 17.302402, 30.3668360449314),
        Seq("ObjectA", "01/06/2018 00:03:00+02:00", 55.918435, 17.302402, 30.0231635627232),
        Seq("ObjectA", "01/06/2018 00:03:10+02:00", 55.918285, 17.301295, 8.56836840558571),
        Seq("ObjectA", "01/06/2018 00:03:19+02:00", 55.918163, 17.300385, 2.45593324496988),
        Seq("ObjectA", "01/06/2018 00:03:20+02:00", 55.918163, 17.300385, 6.75165074981546),
        Seq("ObjectA", "01/06/2018 00:03:31+02:00", 55.917997, 17.29917, 18.3267208898955),
        Seq("ObjectA", "01/06/2018 00:03:51+02:00", 55.917727, 17.29726, 5.32243859198952),
        Seq("ObjectA", "01/06/2018 00:04:00+02:00", 55.9176, 17.296363, 3.37743092724052),
        Seq("ObjectA", "01/06/2018 00:04:10+02:00", 55.917447, 17.295262, 12.276237318997),
        Seq("ObjectA", "01/06/2018 00:04:19+02:00", 55.917322, 17.294355, 10.7522265887458),
        Seq("ObjectA", "01/06/2018 00:04:31+02:00", 55.917155, 17.293157, 0.01784086761036),
        Seq("ObjectA", "01/06/2018 00:04:40+02:00", 55.917027, 17.292233, 0.021282527439162),
        Seq("ObjectA", "01/06/2018 00:04:51+02:00", 55.916883, 17.291198, 0.028057713261653),
        //ObjectA : second time serie
        Seq("ObjectA", "01/06/2018 00:10:01+02:00", 55.912597, 17.259977, 0.315663876001882),
        Seq("ObjectA", "01/06/2018 00:10:11+02:00", 55.912463, 17.258973, 0.456108807052109),
        Seq("ObjectA", "01/06/2018 00:10:21+02:00", 55.912312, 17.25786, 0.228462068370223),
        Seq("ObjectA", "01/06/2018 00:10:30+02:00", 55.91219, 17.256948, 0.467020766459182),
        Seq("ObjectA", "01/06/2018 00:10:41+02:00", 55.912043, 17.25584, 0.483341900268076),
        Seq("ObjectA", "01/06/2018 00:10:51+02:00", 55.911913, 17.254835, 8.81283283812886),
        Seq("ObjectA", "01/06/2018 00:11:01+02:00", 55.911793, 17.253932, 15.9843172672194),
        Seq("ObjectA", "01/06/2018 00:11:11+02:00", 55.911653, 17.252918, 0.205582240119662),
        Seq("ObjectA", "01/06/2018 00:11:19+02:00", 55.911528, 17.252012, 0.181221811179837),
        Seq("ObjectA", "01/06/2018 00:11:30+02:00", 55.911378, 17.250905, 0.389421933375371),
        Seq("ObjectA", "01/06/2018 00:11:39+02:00", 55.911263, 17.249997, 0.350440829164028),
        Seq("ObjectA", "01/06/2018 00:11:51+02:00", 55.911108, 17.248792, 0.465420115412907),
        Seq("ObjectA", "01/06/2018 00:12:01+02:00", 55.910995, 17.247897, 0.493559686120728),
        Seq("ObjectA", "01/06/2018 00:12:11+02:00", 55.91086, 17.246888, 8.02481006722881),
        Seq("ObjectA", "01/06/2018 00:12:19+02:00", 55.910738, 17.245978, 8.81283283812886),
        Seq("ObjectA", "01/06/2018 00:12:30+02:00", 55.910592, 17.244872, 13.9539835805817),
        Seq("ObjectA", "01/06/2018 00:12:39+02:00", 55.910472, 17.243963, 15.9843172672194),
        Seq("ObjectA", "01/06/2018 00:12:51+02:00", 55.910308, 17.242745, 10.0170687089863),
        //Object B : first time serie
        Seq("ObjectB", "01/06/2018 00:00:00+02:00", 56.590177, 11.830633, 7.40493227823278),
        Seq("ObjectB", "01/06/2018 00:00:10+02:00", 56.590058, 11.83063, 9.05637224437842),
        Seq("ObjectB", "01/06/2018 00:00:21+02:00", 56.58993, 11.830625, 8.46042615525682),
        Seq("ObjectB", "01/06/2018 00:00:29+02:00", 56.589837, 11.83062, 0.351557093086739),
        Seq("ObjectB", "01/06/2018 00:00:40+02:00", 56.58971, 11.830603, 0.440739581348716),
        Seq("ObjectB", "01/06/2018 00:00:50+02:00", 56.589603, 11.830595, 0.444570858095414),
        Seq("ObjectB", "01/06/2018 00:01:00+02:00", 56.58949, 11.83058, 0.221747356810941),
        //Object B : second time serie
        Seq("ObjectB", "01/06/2018 00:07:31+02:00", 56.584978, 11.830578, 0.124387757577155),
        Seq("ObjectB", "01/06/2018 00:07:41+02:00", 56.584867, 11.830587, 0.181239176204038),
        Seq("ObjectB", "01/06/2018 00:07:50+02:00", 56.584767, 11.830597, 0.309184859549785),
        Seq("ObjectB", "01/06/2018 00:08:00+02:00", 56.584652, 11.830608, 0.405071433266632),
        Seq("ObjectB", "01/06/2018 00:08:10+02:00", 56.584535, 11.83062, 0.099140439262067),
        Seq("ObjectB", "01/06/2018 00:08:21+02:00", 56.584412, 11.830625, 0.473493901701287),
        Seq("ObjectB", "01/06/2018 00:08:31+02:00", 56.5843, 11.830632, 0.195232493568888),
        Seq("ObjectB", "01/06/2018 00:08:41+02:00", 56.584183, 11.830645, 0.273669959210024),
        Seq("ObjectB", "01/06/2018 00:08:50+02:00", 56.584083, 11.830653, 0.139048843309677),
        Seq("ObjectB", "01/06/2018 00:09:00+02:00", 56.58398, 11.830665, 0.491463951082084),
        Seq("ObjectB", "01/06/2018 00:09:11+02:00", 56.583827, 11.830682, 0.296330460155968),
        Seq("ObjectB", "01/06/2018 00:09:21+02:00", 56.58372, 11.830692, 18.716745664061),
        Seq("ObjectB", "01/06/2018 00:09:31+02:00", 56.583603, 11.830705, 17.1281210029655),
        Seq("ObjectB", "01/06/2018 00:09:41+02:00", 56.583485, 11.83071, 16.5994315098718),
        Seq("ObjectB", "01/06/2018 00:09:50+02:00", 56.583383, 11.830713, 5.11729658662259),
        Seq("ObjectB", "01/06/2018 00:10:00+02:00", 56.58327, 11.830705, 7.289531322081)
      ),
      ListMap(
        "id" -> (StringType, true),
        "timestamp" -> (StringType, true),
        "lat" -> (DoubleType, true),
        "lon" -> (DoubleType, true),
        "speed" -> (DoubleType, true)
      )
    ).withColumn(arlasTimestampColumn, timestampUDF(col("timestamp")))
  }

  val flowFragmentTestDF =
    new FlowFragmentDataGenerator(spark, baseTestDF, dataModel, averagedColumns, standardDeviationEllipsisNbPoints).get()

  val getStopPauseSummaryBaseDF =
    flowFragmentTestDF
      .withColumn(arlasTrackDistanceSensorTravelled, col(arlasTrackDistanceGpsTravelled) + 5)
      .withColumn(
        arlasMovingStateColumn,
        when(col(arlasTrackDistanceGpsStraigthLine).lt(50.0), lit(ArlasMovingStates.STILL))
          .otherwise(lit(ArlasMovingStates.MOVE))
      )
      //make tempo columns nullable with `when(lit(true)`
      .withColumn(tempoProportion1s, when(lit(true), when(col(arlasTrackDuration).between(0, 4), lit(1.0)).otherwise(0.0)))
      .withColumn(tempoProportion10s, when(lit(true), when(col(arlasTrackDuration).between(5, 10), lit(1.0)).otherwise(0.0)))
      .withColumn(tempoProportion20s, when(lit(true), when(col(arlasTrackDuration).between(11, 20), lit(1.0)).otherwise(0.0)))
      .withColumn(tempoProportionIrregular, when(lit(true), when(col(arlasTrackDuration).geq(21), lit(1.0)).otherwise(0.0)))
      .withColumn(
        arlasCourseOrStopColumn,
        when(lit(true),
             when(col(arlasTrackDistanceGpsStraigthLine).equalTo(0), lit(ArlasCourseOrStop.STOP))
               .otherwise(lit(ArlasCourseOrStop.COURSE)))
      )
      .withColumn(
        arlasCourseStateColumn,
        when(
          lit(true),
          when(col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.STILL)), lit(ArlasCourseStates.PAUSE))
            .otherwise(lit(ArlasCourseStates.MOTION))
        )
      )
      .enrichWithArlas(
        new WithStateIdOnStateChangeOrUnique(dataModel.idColumn, arlasMovingStateColumn, arlasTrackTimestampStart, arlasMotionIdColumn),
        new WithDurationFromId(arlasMotionIdColumn, arlasMotionDurationColumn),
        new WithStateIdOnStateChangeOrUnique(dataModel.idColumn, arlasCourseOrStopColumn, arlasTrackTimestampStart, arlasCourseIdColumn),
        new WithDurationFromId(arlasCourseIdColumn, arlasCourseDurationColumn)
      )

  val stopPauseSummaryDF = new StopPauseSummaryDataGenerator(
    spark,
    getStopPauseSummaryBaseDF,
    dataModel,
    speedColumn,
    tempoProportionsColumns,
    tempoIrregular,
    standardDeviationEllipsisNbPoints
  ).get()

  val getMovingFragmentSampleSummarizerBaseDF =
    getStopPauseSummaryBaseDF
      .withColumn(
        arlasTempoColumn,
        when(
          lit(true),
          when(col(arlasTrackDuration).between(0, 4), lit(tempo1s))
            .when(col(arlasTrackDuration).between(5, 10), lit(tempo10s))
            .when(col(arlasTrackDuration).between(11, 20), lit(tempo20s))
            .when(col(arlasTrackDuration).geq(21), lit(tempoIrregular))
            .otherwise(null)
        )
      )
      .enrichWithArlas(
        new WithFragmentVisibilityFromTempo(dataModel, spark, tempoIrregular),
        new WithFragmentSampleId(dataModel, arlasMotionIdColumn, 60l)
      )

  val movingFragmentSampleSummarizerDF = new MovingFragmentSampleSummarizerDataGenerator(
    spark,
    getMovingFragmentSampleSummarizerBaseDF,
    dataModel,
    speedColumn,
    tempoProportionsColumns,
    tempoIrregular,
    standardDeviationEllipsisNbPoints
  ).get()

  val courseExtractorBaseDF = stopPauseSummaryDF
    .withColumn(arlasTrackVisibilityProportion,
                when(lit(true), when(col(tempoProportionIrregular).equalTo(1.0), lit(0.0)).otherwise(lit(1.0))))
    .withColumn(arlasTrackAddressCity, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("Blagnac")))
    .withColumn(arlasTrackAddressCounty, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("Haute-Garonne")))
    .withColumn(arlasTrackAddressCountryCode, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("FR")))
    .withColumn(arlasTrackAddressCountry, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("France")))
    .withColumn(arlasTrackAddressState, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("Midi-Pyrénées")))
    .withColumn(arlasTrackAddressPostcode, when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL), lit("31700")))

  val courseExtractorDF = new CourseExtractorDataGenerator(
    spark,
    courseExtractorBaseDF,
    dataModel,
    speedColumn,
    tempoProportionsColumns,
    tempoIrregular,
    standardDeviationEllipsisNbPoints
  ).get()
    .drop("arlas_arrival_address_city", "arlas_arrival_address_country", "arlas_arrival_address_country_code", "arlas_arrival_address_county", "arlas_arrival_address_postcode", "arlas_arrival_address_state")
    .drop("arlas_arrival_stop_after_location_precision_geometry", "arlas_arrival_stop_after_location_precision_value_lat", "arlas_arrival_stop_after_location_precision_value_lon")
    .drop("arlas_departure_address_city", "arlas_departure_address_country", "arlas_departure_address_country_code", "arlas_departure_address_county", "arlas_departure_address_postcode", "arlas_departure_address_state")
    .drop("arlas_departure_stop_before_location_precision_geometry", "arlas_departure_stop_before_location_precision_value_lat", "arlas_departure_stop_before_location_precision_value_lon")
    .withColumn(arlasTrackPausesLocation, when(col(arlasCourseStateColumn).isNotNull,collect_list(
      when(col(arlasCourseStateColumn).equalTo(ArlasCourseStates.PAUSE),
        concat(col(arlasTrackLocationLat), lit(","), col(arlasTrackLocationLon))).otherwise(lit(null)))
      .over(Window
        .partitionBy(col(dataModel.idColumn),col(arlasCourseOrStopColumn).notEqual(lit(ArlasCourseOrStop.STOP))))
      ).otherwise(lit(null))
      .cast(ArrayType(StringType,true)))
}
