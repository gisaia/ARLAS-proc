package io.arlas.data.utils

import io.arlas.data.model.DataModel
import io.arlas.data.extract.transformations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType

object DataFrameHelper {

  // DataFrame transformations
  def withValidColumnNames()(df: DataFrame): DataFrame = {
    df.select( df.columns.map { c => df.col(c).as(
      c.replaceAll("\\s", "_") // replace whitespaces with '_'
        .replaceAll("[^A-Za-z0-9_]", "") // remove special characters
        .replaceAll("^_", "") // replace strings that start with '_' with empty char
        .toLowerCase()) } : _* )
  }

  def withValidDynamicColumnsType(dataModel: DataModel)(df: DataFrame): DataFrame = {
    //Dynamic columns only support double values
    val columns = df.columns.map(column => {
      if (dataModel.dynamicFields.contains(column)) df.col(column).cast(DoubleType)
      else df.col(column)
    })
    df.select(columns:_*)
  }

}

case class DataFrameException(message: String) extends Exception(message)
