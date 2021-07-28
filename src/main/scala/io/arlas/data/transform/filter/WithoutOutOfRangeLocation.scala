package io.arlas.data.transform.filter

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Keep only lat/lon values included in a valid range.
  *
  * @param latColumn Name of latitude column
  * @param lonColumn Name of longitude column
  * @param latMin    Minimum valid value for latitude
  * @param lonMin    Minimum valid value for longitude
  * @param latMax    Maximum valid value for latitude
  * @param lonMax    Maximum valid value for longitude
  */
class WithoutOutOfRangeLocation(latColumn: String,
                                lonColumn: String,
                                latMin: Double = -90,
                                lonMin: Double = -180,
                                latMax: Double = 90,
                                lonMax: Double = 180)
    extends ArlasTransformer(Vector(latColumn, lonColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .filter(
        col(latColumn)
          .leq(lit(latMax))
          .and(col(latColumn).geq(lit(latMin)))
          .and(col(lonColumn).leq(lit(lonMax)))
          .and(col(lonColumn).geq(lit(lonMin))))
  }
}
