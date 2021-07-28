package io.arlas.data.transform.filter

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Remove outliers and clean temporary associated columns
  *
  * @param outlierColumn Name of boolean column containing outlier identification (True: outlier)
  */
class WithoutIdentifiedOutliers(outlierColumn: String) extends ArlasTransformer(Vector(outlierColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .filter(not(col(outlierColumn)))
      .drop(outlierColumn, "is_return_point", "is_local_outlier")
  }
}
