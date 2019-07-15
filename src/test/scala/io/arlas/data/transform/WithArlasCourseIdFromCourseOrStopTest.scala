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

import io.arlas.data.model.{CourseConfiguration, MLModelLocal, MotionConfiguration}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class WithArlasCourseIdFromCourseOrStopTest extends ArlasTest {

  import spark.implicits._

  val courseConfig = new CourseConfiguration(courseTimeout = 500)
  val motionConfig = new MotionConfiguration(movingStateModel = MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json"))

  val expectedData = Seq(
    ("ObjectA", 1527804000l, ArlasMovingStates.STILL.toString, 59l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804010l, ArlasMovingStates.STILL.toString, 59l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804031l, ArlasMovingStates.STILL.toString, 59l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804040l, ArlasMovingStates.STILL.toString, 59l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804059l, ArlasMovingStates.STILL.toString, 59l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804079l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804100l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804109l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804119l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804120l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804139l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804140l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804151l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804160l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804171l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804179l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804180l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804190l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804199l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804200l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804211l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804231l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804240l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804250l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804259l, ArlasMovingStates.MOVE.toString, 180l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804271l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804280l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804291l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804601l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804611l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804621l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804630l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804641l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804651l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804661l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804671l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804679l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804690l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804699l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804711l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804721l, ArlasMovingStates.STILL.toString, 450l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804731l, ArlasMovingStates.MOVE.toString, 40l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804739l, ArlasMovingStates.MOVE.toString, 40l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804750l, ArlasMovingStates.MOVE.toString, 40l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804759l, ArlasMovingStates.MOVE.toString, 40l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectA", 1527804771l, ArlasMovingStates.MOVE.toString, 40l, ArlasCourseOrStop.COURSE.toString, "ObjectA#1527804000"),
    ("ObjectB", 1527804000l, ArlasMovingStates.MOVE.toString, 21l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804010l, ArlasMovingStates.MOVE.toString, 21l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804021l, ArlasMovingStates.MOVE.toString, 21l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804000"),
    ("ObjectB", 1527804029l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804040l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804050l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804060l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804451l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804461l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804470l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804480l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804490l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804501l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804511l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804521l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804530l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804540l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804551l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804561l, ArlasMovingStates.STILL.toString, 532l, ArlasCourseOrStop.STOP.toString, "ObjectB#1527804029"),
    ("ObjectB", 1527804571l, ArlasMovingStates.MOVE.toString, 29l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804581l, ArlasMovingStates.MOVE.toString, 29l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804590l, ArlasMovingStates.MOVE.toString, 29l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804571"),
    ("ObjectB", 1527804600l, ArlasMovingStates.MOVE.toString, 29l, ArlasCourseOrStop.COURSE.toString, "ObjectB#1527804571"))

  val expectedSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("arlas_timestamp", LongType, false),
      StructField("arlas_moving_state", StringType, true),
      StructField("arlas_motion_duration", LongType, true),
      StructField("arlas_course_or_stop", StringType, false),
      StructField("arlas_course_id", StringType, true)
      ))
  val expectedDF = spark.createDataFrame(expectedData.toDF.rdd, expectedSchema)

  "WithCourseFromCourseState" should "compute arlas_course_id from arlas_course_state" in {

    val sourceDF = cleanedDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(
        new WithArlasMovingState(dataModel, spark, motionConfig),
        new WithArlasMotionIdFromMovingState(dataModel, spark),
        new WithDurationFromId(dataModel, arlasMotionIdColumn, arlasMotionDurationColumn),
        new WithArlasCourseOrStopFromMovingState(dataModel, courseConfig.courseTimeout),
        new WithArlasCourseIdFromCourseOrStop(dataModel, spark))
      .drop(dataModel.timestampColumn, dataModel.latColumn, dataModel.lonColumn, dataModel.speedColumn, arlasPartitionColumn,
            arlasMotionIdColumn)

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
