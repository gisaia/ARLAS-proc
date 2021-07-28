package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param targetDurationColumn Name of the column to store computed duration (s)
  */
class WithDuration(dataModel: DataModel, targetDurationColumn: String)
    extends ArlasTransformer(Vector(dataModel.idColumn, dataModel.timestampColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // spark window
    val window = Window
      .partitionBy(dataModel.idColumn)
      .orderBy(dataModel.timestampColumn)

    def whenPreviousPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lag(dataModel.timestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    dataset
      .toDF()
      .withColumn( // track_duration_s = ts(start) - ts(end)
        targetDurationColumn,
        whenPreviousPointExists(col(dataModel.timestampColumn) - lag(dataModel.timestampColumn, 1).over(window))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(targetDurationColumn, IntegerType, false))
  }
}
