/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform.features

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Get the geohashes of every point from the input trail.
  * @param trailColumn the input trail column
  * @param geohashColumn the output column, containing an array of the geohashes
  * @param precision optional precision of the resolved geohash
  */
class WithGeohash(trailColumn: String, geohashColumn: String, precision: Int = 6) extends ArlasTransformer(Vector(trailColumn)) {

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
