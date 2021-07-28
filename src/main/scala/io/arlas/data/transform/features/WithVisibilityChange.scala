package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTrackVisibilityChange, arlasTrackVisibilityProportion}
import io.arlas.data.transform.VisibilityChange.{APPEAR, APPEAR_DISAPPEAR, DISAPPEAR}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Detect if a fragment is part of a visibility change:
  * - APPEAR = first visible fragment after an invisible fragment
  * - DISAPPEAR = last visible fragment before an invisible fragment
  * - APPEAR_DISAPPEAR = visible fragment between 2 invisible fragments
  *
  * @param visibilityChangeColumn Name of the target visibility change column
  * @param visibilityProportionColumn Name of the visibility proportion column
  * @param dataModel     Data model containing names of structuring columns (id, lat, lon, time)
  */
class WithVisibilityChange(visibilityChangeColumn: String = arlasTrackVisibilityChange,
                           visibilityProportionColumn: String = arlasTrackVisibilityProportion,
                           dataModel: DataModel)
    extends ArlasTransformer(Vector(visibilityProportionColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window.partitionBy(dataModel.idColumn).orderBy(dataModel.timestampColumn)

    val previousVisibilityProportion = lag(visibilityProportionColumn, 1).over(window)
    val nextVisibilityProportion = lead(visibilityProportionColumn, 1).over(window)

    dataset
      .toDF()
      /*
       * APPEAR = first visible fragment after an invisible fragment
       * DISAPPEAR = last visible fragment before an invisible fragment
       * APPEAR_DISAPPEAR = visible fragment between 2 invisible fragments
       * null = other fragments
       */
      .withColumn(
        visibilityChangeColumn,
        when(col(visibilityProportionColumn).equalTo(0), null)
          .otherwise(
            when(previousVisibilityProportion.equalTo(0) && nextVisibilityProportion.equalTo(0), APPEAR_DISAPPEAR)
              .when(previousVisibilityProportion.equalTo(0), APPEAR)
              .when(nextVisibilityProportion.equalTo(0), DISAPPEAR)
              .otherwise(null))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(visibilityChangeColumn, DoubleType, false))
  }
}
