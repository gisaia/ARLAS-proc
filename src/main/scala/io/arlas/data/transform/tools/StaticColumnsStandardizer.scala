package io.arlas.data.transform.tools

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._

/**
  * Standardize the static columns of each object.
  * If, for an object and a static column, there is no known defined value, use a default value.
  * Algorithm:
  * - at first for each row, we compute the number of undefined static columns
  * - then for each object (unique dataModel.idColumn), we order a window by this number
  * - finally for each row, the static columns take the value of the first row of the window - if it is defined - or a default value.
  * We suppose that for each object, there is at least one row with all static values.
  * @param dataModel
  * @param cols a map with (static column name -> sequence of possible undefined values for this column).
  *             You don't need to pass `null` as undefined value, this is always checked
  */
class StaticColumnsStandardizer(dataModel: DataModel, cols: Map[String, Seq[String]])
    extends ArlasTransformer(Vector(cols.keys.toSeq: _*)) {

  private val TEMP_NB_UNDEFINED = "tmp_nb_undefined"

  def whenColInValuesOrNull(column: Column, values: Seq[String], value: Column, otherwise: Column) = {
    when(column.isin(values: _*).or(column.isNull), value).otherwise(otherwise)
  }

  def getFirstIfDefinedOrDefault(colName: String, undefinedCols: Seq[String], window: WindowSpec) = {
    whenColInValuesOrNull(first(col(colName)).over(window),
                          undefinedCols,
                          lit(StaticColumnsStandardizer.DEFAULT_UNDEFINED),
                          first(col(colName)).over(window))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val withNbUndefinedDF = dataset
      .withColumn(TEMP_NB_UNDEFINED, cols.map(c => whenColInValuesOrNull(col(c._1), c._2, lit(1), lit(0))).reduce(_ + _))

    val window = Window.partitionBy(dataModel.idColumn).orderBy(col(TEMP_NB_UNDEFINED))

    cols
      .foldLeft(withNbUndefinedDF) {
        case (df, c) =>
          df.withColumn(c._1, whenColInValuesOrNull(col(c._1), c._2, getFirstIfDefinedOrDefault(c._1, c._2, window), col(c._1)))
      }
      .drop(TEMP_NB_UNDEFINED)
  }

}

object StaticColumnsStandardizer {
  val DEFAULT_UNDEFINED = "Unknown"
}
