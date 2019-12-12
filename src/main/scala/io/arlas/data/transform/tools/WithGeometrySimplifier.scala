package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{udf, col}

class WithGeometrySimplifier(inputGeometryCol: String,
                             outputGeometryCol: String,
                             distanceTolerance: Option[Double] = None)
    extends ArlasTransformer(Vector(inputGeometryCol)) {

  val simplifierUDF = udf(
    (geometryCol: String) => GeoTool.simplifyGeometry(geometryCol, distanceTolerance))

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(outputGeometryCol, simplifierUDF(col(inputGeometryCol)))
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema).add(StructField(outputGeometryCol, StringType, true))
}
