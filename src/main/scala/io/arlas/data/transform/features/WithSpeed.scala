package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.arlasTrackDuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Compute mean speed in a given unit from distance (m) and duration (s).
  *
  * @param speedColumn Name of the target speed column
  * @param speedUnit Unit of the computed speed
  * @param durationSecondsColumn Name of the duration (s) column
  * @param distanceMetersColumn Name of the distance (m) column
  */
class WithSpeed(speedColumn: String, speedUnit: String, durationSecondsColumn: String = arlasTrackDuration, distanceMetersColumn: String)
    extends ArlasTransformer(Vector(durationSecondsColumn, distanceMetersColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    def coef(unit: String): Double = unit match {
      case "m/s"  => 1.0
      case "km/h" => 1.0 / 3.6
      case "nd"   => 1.9438444924406
      case _      => 1.0
    }

    dataset
      .toDF()
      .withColumn(
        speedColumn,
        col(distanceMetersColumn) / col(durationSecondsColumn) * lit(coef(speedUnit))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(speedColumn, DoubleType, false))
  }
}
