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

import io.arlas.data.model.{DataModel, MLModelLocal}
import io.arlas.data.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import io.arlas.data.transform.ArlasTransformerColumns._

class WithArlasMotionIdFromMotionStateTest extends ArlasTest {

  import spark.implicits._

  val testDataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX",
                                visibilityTimeout = 300,
                                speedColumn = "speed",
                                movingStateModel = MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json"))

  val expectedData = Seq(
    ("ObjectA", 1527804000l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804010l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804031l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804040l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804059l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804079l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804100l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804109l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804119l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804120l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804139l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804140l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804151l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804160l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804171l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804179l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804180l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804190l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804199l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804200l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804211l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804231l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804240l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804250l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804259l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804079"),
    ("ObjectA", 1527804271l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804280l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804291l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804601l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804611l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804621l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804630l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804641l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804651l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804661l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804671l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804679l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804690l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804699l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804711l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804721l, ArlasMotionStates.PAUSE.toString, "ObjectA#1527804271"),
    ("ObjectA", 1527804731l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804731"),
    ("ObjectA", 1527804739l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804731"),
    ("ObjectA", 1527804750l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804731"),
    ("ObjectA", 1527804759l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804731"),
    ("ObjectA", 1527804771l, ArlasMotionStates.MOTION.toString, "ObjectA#1527804731"),
    ("ObjectB", 1527804000l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804010l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804021l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804029l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804040l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804050l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804060l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804451l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804461l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804470l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804480l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804490l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804501l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804511l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804521l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804530l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804540l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804551l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804561l, ArlasMotionStates.PAUSE.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804571l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804581l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804590l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804600l, ArlasMotionStates.MOTION.toString, "ObjectB#1527804571"))

  val expectedSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("arlas_timestamp", LongType, false),
      StructField("arlas_motion_state", StringType, true),
      StructField("arlas_motion_id", StringType, true)
      ))
  val expectedDF = spark.createDataFrame(expectedData.toDF.rdd, expectedSchema)

  "WithArlasMotionFromMovingState" should "set motion_id" in {

    val sourceDF = cleanedDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(
        new WithArlasMovingState(testDataModel, spark, testDataModel.idColumn),
        new OtherColValuesMapper(dataModel, arlasMovingStateColumn, arlasMotionStateColumn,
                                 Map(ArlasMovingStates.MOVE.toString -> ArlasMotionStates.MOTION.toString,
                                     ArlasMovingStates.STILL.toString -> ArlasMotionStates.PAUSE.toString)),
        new WithArlasMotionIdFromMotionState(testDataModel, spark))

    assertDataFrameEquality(transformedDF.drop(dataModel.timestampColumn, dataModel.latColumn, dataModel.lonColumn, dataModel.speedColumn,
                                               arlasPartitionColumn, arlasMovingStateColumn), expectedDF)
  }

}
