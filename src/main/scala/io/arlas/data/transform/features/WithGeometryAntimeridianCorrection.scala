package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Fix linestring geometry antimeridian crossing by creating a corrected multilinestring geometry
  *
  * @param inputGeometryCol Name of the column containing the geometry to process
  * @param outputGeometryCol Name of the column to store the corrected geometry
  */
class WithGeometryAntimeridianCorrection(inputGeometryCol: String, outputGeometryCol: String)
    extends ArlasTransformer(Vector(inputGeometryCol, outputGeometryCol)) {

  val antimeridianUDF = udf((geometryCol: String) => GeoTool.fixAntimeridianCrossingGeometries(geometryCol))

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(outputGeometryCol, antimeridianUDF(col(inputGeometryCol)))
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema).add(StructField(outputGeometryCol, StringType, true))
}
