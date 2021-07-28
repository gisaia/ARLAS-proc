package io.arlas.data.transform.features

import io.arlas.data.transform.{ArlasCourseOrStop, ArlasMovingStates, ArlasTransformer}
import io.arlas.data.transform.ArlasTransformerColumns.{arlasCourseOrStopColumn, arlasMotionDurationColumn, arlasMovingStateColumn}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Determine if a fragment is a course or a stop (still during a duration longer than a threshold)
  * Requires the movingState column
  *
  * @param courseTimeout
  */
class WithCourseOrStop(courseTimeout: Int) extends ArlasTransformer(Vector(arlasMovingStateColumn, arlasMotionDurationColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val courseState = when(
      col(arlasMovingStateColumn).equalTo(lit(ArlasMovingStates.STILL)),
      when(col(arlasMotionDurationColumn) < courseTimeout, lit(ArlasCourseOrStop.COURSE))
        .otherwise(lit(ArlasCourseOrStop.STOP))
    ).otherwise(when(col(arlasMovingStateColumn).equalTo(ArlasMovingStates.GAP), lit(ArlasCourseOrStop.GAP))
      .otherwise(lit(ArlasCourseOrStop.COURSE)))

    dataset
      .toDF()
      .withColumn(arlasCourseOrStopColumn, courseState)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasCourseOrStopColumn, StringType, false))
  }
}
