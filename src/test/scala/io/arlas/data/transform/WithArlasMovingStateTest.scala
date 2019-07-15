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

import io.arlas.data.model.{MLModelLocal, MotionConfiguration}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class WithArlasMovingStateTest extends ArlasTest {

  import spark.implicits._

  val expectedData = Seq(
    //Object A
    ("ObjectA", "01/06/2018 00:00:00+02:00", 0.280577132616533, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 0.032068662532024, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 0.178408676103601, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 0.180505395097491, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 18.3267208898955, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:01:19+02:00", 25.3224385919895, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:01:40+02:00", 33.3774309272405, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:01:49+02:00", 22.276237318997, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:01:59+02:00", 20.4161902434555, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:00+02:00", 20.4670321139802, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:19+02:00", 20.3483994565575, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:20+02:00", 30.4938344029634, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:31+02:00", 30.3776738025963, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:40+02:00", 30.0678204838492, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:51+02:00", 30.1765108328227, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:02:59+02:00", 30.3668360449314, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:00+02:00", 30.0231635627232, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:10+02:00", 8.56836840558571, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:19+02:00", 2.45593324496988, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:20+02:00", 6.75165074981546, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:31+02:00", 18.3267208898955, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:03:51+02:00", 5.32243859198952, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:04:00+02:00", 3.37743092724052, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:04:10+02:00", 12.276237318997, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:04:19+02:00", 10.7522265887458, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:04:31+02:00", 0.01784086761036, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:04:40+02:00", 0.021282527439162, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:04:51+02:00", 0.028057713261653, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:01+02:00", 0.315663876001882, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:11+02:00", 0.456108807052109, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:21+02:00", 0.228462068370223, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:30+02:00", 0.467020766459182, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:41+02:00", 0.483341900268076, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:10:51+02:00", 8.81283283812886, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:01+02:00", 15.9843172672194, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:11+02:00", 0.205582240119662, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:19+02:00", 0.181221811179837, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:30+02:00", 0.389421933375371, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:39+02:00", 0.350440829164028, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:11:51+02:00", 0.465420115412907, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:12:01+02:00", 0.493559686120728, ArlasMovingStates.STILL.toString),
    ("ObjectA", "01/06/2018 00:12:11+02:00", 8.02481006722881, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:12:19+02:00", 8.81283283812886, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:12:30+02:00", 13.9539835805817, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:12:39+02:00", 15.9843172672194, ArlasMovingStates.MOVE.toString),
    ("ObjectA", "01/06/2018 00:12:51+02:00", 10.0170687089863, ArlasMovingStates.MOVE.toString),
    //Object B
    ("ObjectB", "01/06/2018 00:00:00+02:00", 7.40493227823278, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 9.05637224437842, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:00:21+02:00", 8.46042615525682, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:00:29+02:00", 0.351557093086739, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:00:40+02:00", 0.440739581348716, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:00:50+02:00", 0.444570858095414, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:01:00+02:00", 0.221747356810941, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:07:31+02:00", 0.124387757577155, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:07:41+02:00", 0.181239176204038, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:07:50+02:00", 0.309184859549785, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:00+02:00", 0.405071433266632, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:10+02:00", 0.099140439262067, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:21+02:00", 0.473493901701287, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:31+02:00", 0.195232493568888, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:41+02:00", 0.273669959210024, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:08:50+02:00", 0.139048843309677, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:09:00+02:00", 0.491463951082084, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:09:11+02:00", 0.296330460155968, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:09:21+02:00", 18.716745664061, ArlasMovingStates.STILL.toString),
    ("ObjectB", "01/06/2018 00:09:31+02:00", 17.1281210029655, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:09:41+02:00", 16.5994315098718, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:09:50+02:00", 5.11729658662259, ArlasMovingStates.MOVE.toString),
    ("ObjectB", "01/06/2018 00:10:00+02:00", 7.289531322081, ArlasMovingStates.MOVE.toString)
    )

  val expectedSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("speed", DoubleType, true),
      StructField(arlasMovingStateColumn, StringType, true)
      ))

  val expectedDf = spark.createDataFrame(expectedData.toDF("id", "timestamp", "speed", arlasMovingStateColumn).rdd, expectedSchema)

  "WithArlasMovingState transformation" should " compute the moving state of a dataframe's timeseries" in {

    val testProcessingConfig = processingConfig.copy(motionConfiguration = new MotionConfiguration(
      movingStateModel = MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json")
      ))
    val transformedDf = visibleSequencesDF
      //avoid natural ordering to ensure that hmm doesn't depend on initial order
      .sort(dataModel.latColumn, dataModel.lonColumn)
      .enrichWithArlas(
        new WithArlasMovingState(dataModel, spark, testProcessingConfig.motionConfiguration))
      .drop(dataModel.latColumn, dataModel.lonColumn, arlasPartitionColumn, arlasTimestampColumn, arlasVisibleSequenceIdColumn, arlasVisibilityStateColumn)

    assertDataFrameEquality(transformedDf, expectedDf)
  }

  //we use a quite big window, the longest partition is 29 points ; because with too few points the results are bad
  //in a real environment, window size shoud be equal to some thousends
  "WithArlasMovingState transformation" should " compute the moving state of a dataframe's timeseries using windowing" in {

    val testProcessingConfig = processingConfig.copy(motionConfiguration = new MotionConfiguration(
      movingStateModel = MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json"),
      movingStateHmmWindowSize = 30
      ))

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new WithArlasMovingState(dataModel, spark, testProcessingConfig.motionConfiguration))
      .drop(dataModel.latColumn, dataModel.lonColumn, arlasPartitionColumn, arlasTimestampColumn, arlasVisibleSequenceIdColumn, arlasVisibilityStateColumn)

    assertDataFrameEquality(transformedDf, expectedDf)
  }

}
