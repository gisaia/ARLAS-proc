package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTrackDuration, arlasTrackVisibilityProportion}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify a fragment as "invisible" if the duration between its two measures is longer than a threshold
  *
  * @param visibilityProportionColumn Name of the target gap state column
  * @param durationSecondsColumn      Name of the duration (s) column
  * @param durationThreshold          Minimum duration (s) between observation to be considered as a gap
  */
class WithVisibilityProportion(visibilityProportionColumn: String = arlasTrackVisibilityProportion,
                               durationSecondsColumn: String = arlasTrackDuration,
                               durationThreshold: Long = 1800)
    extends ArlasTransformer(Vector(durationSecondsColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(visibilityProportionColumn,
                  when(col(durationSecondsColumn).gt(lit(durationThreshold)), lit(0.0d))
                    .otherwise(lit(1.0d)))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(visibilityProportionColumn, DoubleType, false))
  }
}
