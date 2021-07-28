package io.arlas.data.transform.features

import io.arlas.data.transform.{ArlasCourseOrStop, ArlasCourseStates, ArlasMovingStates, ArlasTransformer}
import io.arlas.data.transform.ArlasTransformerColumns.{arlasCourseOrStopColumn, arlasCourseStateColumn, arlasMovingStateColumn}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Determine the course state (MOTION, PAUSE) for course fragments
  * Requires courseOrStop column and movingState column
  */
class WithCourseState() extends ArlasTransformer(Vector(arlasCourseOrStopColumn, arlasMovingStateColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val courseState = when(
      col(arlasCourseOrStopColumn)
        .equalTo(ArlasCourseOrStop.COURSE)
        .and(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.MOVE)),
      ArlasCourseStates.MOTION
    ).otherwise(when(
      col(arlasCourseOrStopColumn)
        .equalTo(ArlasCourseOrStop.COURSE)
        .and(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)),
      ArlasCourseStates.PAUSE
    ))

    dataset
      .toDF()
      .withColumn(arlasCourseStateColumn, courseState)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasCourseStateColumn, StringType, false))
  }
}
