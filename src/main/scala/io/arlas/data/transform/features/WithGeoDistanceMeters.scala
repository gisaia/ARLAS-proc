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

package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * Compute the travelled distance (m) between previous and current observations
  *
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param targetDistanceColumn Name of the column containing dis
  * @param spark Spark Session
  */
class WithGeoDistanceMeters(dataModel: DataModel, targetDistanceColumn: String, spark: SparkSession)
    extends ArlasTransformer(Vector(dataModel.idColumn, dataModel.latColumn, dataModel.lonColumn, arlasTimestampColumn)) {

  // Function to apply before the fragments creation
  spark.udf.register("getDistanceTravelled", GeoTool.getDistanceBetween _)

  override def transform(dataset: Dataset[_]): DataFrame = {

    // spark window
    val window = Window
      .partitionBy(dataModel.idColumn)
      .orderBy(arlasTimestampColumn)

    def whenPreviousPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lag(arlasTimestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    dataset
      .toDF()
      .withColumn( //track_distance_travelled_m = distance between previous and current point
        targetDistanceColumn,
        whenPreviousPointExists(
          callUDF(
            "getDistanceTravelled",
            lag(dataModel.latColumn, 1).over(window),
            lag(dataModel.lonColumn, 1).over(window),
            col(dataModel.latColumn),
            col(dataModel.lonColumn)
          ))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(targetDistanceColumn, IntegerType, false))
  }
}
