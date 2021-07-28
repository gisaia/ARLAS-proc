package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Create a new speed column based on gps and sensor speed according to simple rules:
  *  - If fragment duration > Threshold : Sensor speed is punctual and not a good approximation of fragment speed, gps speed is chosen
  *  - If fragment duration < Threshold : Gps speed is to noisy because of gps precision and short duration, sensor speed is chosen
  *
  * @param newSpeedColumn Name of the new column containing the hybrid speed
  * @param gpsSpeedColumn Name of the column containing GPS Speed
  * @param sensorSpeedColumn Name of the column containing Sensor Speed
  * @param durationColumn Name of the column containing duration (s)
  * @param durationThreshold Duration threshold (s) permitting to choose between the two speed
  */
class WithGpsOrSensorSpeed(newSpeedColumn: String,
                           gpsSpeedColumn: String,
                           sensorSpeedColumn: String,
                           durationColumn: String,
                           durationThreshold: Int = 300)
    extends ArlasTransformer(Vector(gpsSpeedColumn, sensorSpeedColumn, durationColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(
        newSpeedColumn,
        when(col(sensorSpeedColumn).isNull, col(gpsSpeedColumn))
          .otherwise(
            when(col(durationColumn).gt(lit(durationThreshold)), col(gpsSpeedColumn))
              .otherwise(col(sensorSpeedColumn))
          )
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(newSpeedColumn, StringType, false))
  }
}
