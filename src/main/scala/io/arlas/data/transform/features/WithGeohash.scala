package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
  * Get the geohashes of every point from the input trail.
  * @param trailColumn the input trail column
  * @param geohashColumn the output column, containing an array of the geohashes
  * @param precision optional precision of the resolved geohash
  */
class WithGeohash(trailColumn: String, geohashColumn: String, precision: Int = 6)
    extends ArlasTransformer(Vector(trailColumn)) {

  def getGeoHashUDF =
    udf((trail: String) => {

      GeoTool
        .wktToGeometry(trail)
        .map(c => GeoTool.getGeohashFrom(c._1, c._2, precision))
        .toSet
        .toArray
    })

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(geohashColumn, getGeoHashUDF(col(trailColumn)))
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema).add(StructField(geohashColumn, ArrayType(StringType), true))
}
