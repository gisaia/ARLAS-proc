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

package io.arlas.data.transform.filter

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Keep only identified outliers observations, enriched with a geometry linking to neighbor locations
  *
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param outlierColumn Name of boolean column containing outlier identification result (True if outlier)
  * @param aggregationColumnName Column containing group identifier to identify neighbors in observation sequences
  * @param targetTrailColumn Column that will contain the geometry linking outliers to neighbor location
  * @param targetLocationColumn Column that will contain the outliers location geometry
  */
class WithOutliersToStore(dataModel: DataModel,
                          outlierColumn: String,
                          aggregationColumnName: String,
                          targetTrailColumn: String,
                          targetLocationColumn: String)
    extends ArlasTransformer(Vector(aggregationColumnName, arlasTimestampColumn, dataModel.latColumn, dataModel.lonColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // spark window
    val window = Window
      .partitionBy(aggregationColumnName)
      .orderBy(arlasTimestampColumn)

    def whenPreviousPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lag(arlasTimestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    def whenNextPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lead(arlasTimestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    dataset
      .toDF()
      .withColumn( // Create the trail by connecting the points
        targetTrailColumn,
        whenPreviousPointExists(
          whenNextPointExists(
            concat(
              lit("LINESTRING ("),
              lag(dataModel.lonColumn, 1).over(window),
              lit(" "),
              lag(dataModel.latColumn, 1).over(window),
              lit(", "),
              col(dataModel.lonColumn),
              lit(" "),
              col(dataModel.latColumn),
              lit(", "),
              lead(dataModel.lonColumn, 1).over(window),
              lit(" "),
              lead(dataModel.latColumn, 1).over(window),
              lit(")")
            )
          )
        )
      )
      .withColumn( // Create the outlier point geometry
        targetLocationColumn,
        whenPreviousPointExists(
          concat(
            lit("POINT ("),
            col(dataModel.lonColumn),
            lit(" "),
            col(dataModel.latColumn),
            lit(")")
          )
        )
      )
      .filter(col(outlierColumn))
  }
}
