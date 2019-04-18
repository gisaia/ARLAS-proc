/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Coordinate

class WithArlasDistance(dataModel: DataModel, spark: SparkSession)
    extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn, arlasPartitionColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window
      .partitionBy(arlasTimeSerieIdColumn)
      .orderBy(arlasTimestampColumn)
      .rowsBetween(-1, 0)

    spark.udf.register("distance", distance, DataTypes.DoubleType)

    dataset
      .withColumn(
        arlasDistanceColumn,
        callUDF(
          "distance",
          first(dataModel.latColumn).over(window),
          first(dataModel.lonColumn).over(window),
          last(dataModel.latColumn).over(window),
          last(dataModel.lonColumn).over(window)
        )
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema).add(StructField(arlasDistanceColumn, DoubleType, false))
  }

  private val distance = new UDF4[Double, Double, Double, Double, Double]() {
    @throws[Exception]
    override def call(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val start = new Coordinate(lon1, lat1)
      val end = new Coordinate(lon2, lat2)
      val sourceCRS = CRS.decode("EPSG:4326")
      try {
        val distance = JTS.orthodromicDistance(start, end, sourceCRS)
        distance.toDouble
      } catch {
        case e: IllegalArgumentException =>
          0
      }
    }
  }
}
