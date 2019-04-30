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

import io.arlas.data.model.DataModel
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class WithArlasMovingStateTest extends ArlasTest {

  import spark.implicits._

  val expectedData = Seq(
    ("ObjectA", "01/06/2018 00:00:00+02:00", 0.280577132616533, "STILL"),
    ("ObjectA", "01/06/2018 00:00:10+02:00", 0.032068662532024, "STILL"),
    ("ObjectA", "01/06/2018 00:00:31+02:00", 0.178408676103601, "STILL"),
    ("ObjectA", "01/06/2018 00:00:40+02:00", 0.180505395097491, "STILL"),
    ("ObjectA", "01/06/2018 00:00:59+02:00", 18.3267208898955, "STILL"),
    ("ObjectA", "01/06/2018 00:01:19+02:00", 25.3224385919895, "MOVE"),
    ("ObjectA", "01/06/2018 00:01:40+02:00", 33.3774309272405, "MOVE"),
    ("ObjectA", "01/06/2018 00:01:49+02:00", 22.276237318997, "MOVE"),
    ("ObjectA", "01/06/2018 00:01:59+02:00", 20.4161902434555, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:00+02:00", 20.4670321139802, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:19+02:00", 20.3483994565575, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:20+02:00", 30.4938344029634, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:31+02:00", 30.3776738025963, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:40+02:00", 30.0678204838492, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:51+02:00", 30.1765108328227, "MOVE"),
    ("ObjectA", "01/06/2018 00:02:59+02:00", 30.3668360449314, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:00+02:00", 30.0231635627232, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:10+02:00", 8.56836840558571, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:19+02:00", 2.45593324496988, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:20+02:00", 6.75165074981546, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:31+02:00", 18.3267208898955, "MOVE"),
    ("ObjectA", "01/06/2018 00:03:51+02:00", 5.32243859198952, "MOVE"),
    ("ObjectA", "01/06/2018 00:04:00+02:00", 3.37743092724052, "MOVE"),
    ("ObjectA", "01/06/2018 00:04:10+02:00", 12.276237318997, "MOVE"),
    ("ObjectA", "01/06/2018 00:04:19+02:00", 10.7522265887458, "MOVE"),
    ("ObjectA", "01/06/2018 00:04:31+02:00", 0.01784086761036, "STILL"),
    ("ObjectA", "01/06/2018 00:04:40+02:00", 0.021282527439162, "STILL"),
    ("ObjectA", "01/06/2018 00:04:51+02:00", 0.028057713261653, "STILL"),
    //ObjectA : second time serie
    ("ObjectA", "01/06/2018 00:10:01+02:00", 0.315663876001882, "STILL"),
    ("ObjectA", "01/06/2018 00:10:11+02:00", 0.456108807052109, "STILL"),
    ("ObjectA", "01/06/2018 00:10:21+02:00", 0.228462068370223, "STILL"),
    ("ObjectA", "01/06/2018 00:10:30+02:00", 0.467020766459182, "STILL"),
    ("ObjectA", "01/06/2018 00:10:41+02:00", 0.483341900268076, "STILL"),
    ("ObjectA", "01/06/2018 00:10:51+02:00", 8.81283283812886, "STILL"),
    ("ObjectA", "01/06/2018 00:11:01+02:00", 15.9843172672194, "STILL"),
    ("ObjectA", "01/06/2018 00:11:11+02:00", 0.205582240119662, "STILL"),
    ("ObjectA", "01/06/2018 00:11:19+02:00", 0.181221811179837, "STILL"),
    ("ObjectA", "01/06/2018 00:11:30+02:00", 0.389421933375371, "STILL"),
    ("ObjectA", "01/06/2018 00:11:39+02:00", 0.350440829164028, "STILL"),
    ("ObjectA", "01/06/2018 00:11:51+02:00", 0.465420115412907, "STILL"),
    ("ObjectA", "01/06/2018 00:12:01+02:00", 0.493559686120728, "STILL"),
    ("ObjectA", "01/06/2018 00:12:11+02:00", 8.02481006722881, "MOVE"),
    ("ObjectA", "01/06/2018 00:12:19+02:00", 8.81283283812886, "MOVE"),
    ("ObjectA", "01/06/2018 00:12:30+02:00", 13.9539835805817, "MOVE"),
    ("ObjectA", "01/06/2018 00:12:39+02:00", 15.9843172672194, "MOVE"),
    ("ObjectA", "01/06/2018 00:12:51+02:00", 10.0170687089863, "MOVE"),
    //Object B : first time serie
    ("ObjectB", "01/06/2018 00:00:00+02:00", 7.40493227823278, "MOVE"),
    ("ObjectB", "01/06/2018 00:00:10+02:00", 9.05637224437842, "MOVE"),
    ("ObjectB", "01/06/2018 00:00:21+02:00", 8.46042615525682, "MOVE"),
    ("ObjectB", "01/06/2018 00:00:29+02:00", 0.351557093086739, "STILL"),
    ("ObjectB", "01/06/2018 00:00:40+02:00", 0.440739581348716, "STILL"),
    ("ObjectB", "01/06/2018 00:00:50+02:00", 0.444570858095414, "STILL"),
    ("ObjectB", "01/06/2018 00:01:00+02:00", 0.221747356810941, "STILL"),
    //Object B : second time serie
    ("ObjectB", "01/06/2018 00:07:31+02:00", 0.124387757577155, "STILL"),
    ("ObjectB", "01/06/2018 00:07:41+02:00", 0.181239176204038, "STILL"),
    ("ObjectB", "01/06/2018 00:07:50+02:00", 0.309184859549785, "STILL"),
    ("ObjectB", "01/06/2018 00:08:00+02:00", 0.405071433266632, "STILL"),
    ("ObjectB", "01/06/2018 00:08:10+02:00", 0.099140439262067, "STILL"),
    ("ObjectB", "01/06/2018 00:08:21+02:00", 0.473493901701287, "STILL"),
    ("ObjectB", "01/06/2018 00:08:31+02:00", 0.195232493568888, "STILL"),
    ("ObjectB", "01/06/2018 00:08:41+02:00", 0.273669959210024, "STILL"),
    ("ObjectB", "01/06/2018 00:08:50+02:00", 0.139048843309677, "STILL"),
    ("ObjectB", "01/06/2018 00:09:00+02:00", 0.491463951082084, "STILL"),
    ("ObjectB", "01/06/2018 00:09:11+02:00", 0.296330460155968, "STILL"),
    ("ObjectB", "01/06/2018 00:09:21+02:00", 18.716745664061, "STILL"),
    ("ObjectB", "01/06/2018 00:09:31+02:00", 17.1281210029655, "MOVE"),
    ("ObjectB", "01/06/2018 00:09:41+02:00", 16.5994315098718, "MOVE"),
    ("ObjectB", "01/06/2018 00:09:50+02:00", 5.11729658662259, "MOVE"),
    ("ObjectB", "01/06/2018 00:10:00+02:00", 7.289531322081, "MOVE")
    )

  val expectedSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("speed", DoubleType, true),
      StructField(arlasMovingStateColumn, StringType, false)
      ))

  val expectedDf = spark.createDataFrame(expectedData.toDF("id", "timestamp", "speed", arlasMovingStateColumn).rdd, expectedSchema)

  "WithArlasMovingState transformation" should " compute the moving state of a dataframe's timeseries" in {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", visibilityTimeout = 300, speedColumn = "speed", movingStateModelPath =
        "src/test/resources/hmm_test_model.json")

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new WithArlasMovingState(dataModel, spark))
      .select("id", "timestamp", "speed", arlasMovingStateColumn)

    assertDataFrameEquality(transformedDf, expectedDf)
  }
}
