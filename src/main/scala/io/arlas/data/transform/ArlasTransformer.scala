package io.arlas.data.transform

import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.model.DataModel
import io.arlas.data.utils.DataFrameException
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

abstract class ArlasTransformer(val dataModel: DataModel, val requiredCols: Vector[String]) extends Transformer  {

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    val allRequiredCols = requiredCols ++ Vector(dataModel.timestampColumn, dataModel.idColumn, dataModel.latColumn, dataModel.lonColumn) ++ dataModel.dynamicFields.toSeq
    val colsNotFound = allRequiredCols.distinct.diff(schema.fieldNames)
    if (colsNotFound.length > 0) {
      throw new DataFrameException(s"The ${colsNotFound.mkString(", ")} columns are not included in the DataFrame with the following columns ${schema.fieldNames.mkString(", ")}")
    }

    schema
  }

  override val uid: String = {
    Identifiable.randomUID(this.getClass.getSimpleName)
  }
}
