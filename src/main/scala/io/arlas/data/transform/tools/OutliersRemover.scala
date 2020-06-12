package io.arlas.data.transform.tools

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

/**
  * Remove the outliers by computing the median over a window.
  * If the (value - median) is over a threshold, then it is an outlier.
  * We do it for lattitude then longitude.
  *
  * As there is no built-in median transformer over a window,
  * for each row within a window, we consider the list of lat/lon,
  * then we sort this list and keep the middle element: this is our
  * median.
  * @param dataModel
  * @param threshold
  * @param windowSize
  */
class OutliersRemover(dataModel: DataModel, idColumn: String, windowSize: Integer, threshold: Double)
    extends ArlasTransformer(Vector(idColumn, dataModel.timestampColumn, dataModel.latColumn, dataModel.lonColumn)) {

  val COLLECT_COLUMN = "collect"
  val INNOVATION_COLUMN = "innovation"

  override def transform(dataset: Dataset[_]): DataFrame = {
    val window = Window
      .partitionBy(idColumn)
      .orderBy(dataModel.timestampColumn)
      .rowsBetween(-windowSize / 2, windowSize / 2)

    val latFiltered = applyToCol(dataset.toDF(), dataModel.latColumn, window)
    val lonFiltered = applyToCol(latFiltered, dataModel.lonColumn, window)

    lonFiltered
  }

  private def applyToCol(df: DataFrame, appliedCol: String, window: WindowSpec): DataFrame = {
    df.withColumn(COLLECT_COLUMN, sort_array(collect_list(appliedCol).over(window)))
      //not applied to first and last elements i.a. only if size == windowSize
      .withColumn(INNOVATION_COLUMN,
                  when(size(col(COLLECT_COLUMN)).equalTo(windowSize), col(appliedCol) - col(COLLECT_COLUMN)(windowSize / 2 + 1))
                    .otherwise(lit(0.0)))
      .filter(abs(col(INNOVATION_COLUMN)).leq(threshold))
      .drop(COLLECT_COLUMN, INNOVATION_COLUMN)
  }

}
