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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Update the first outlier detection by identifying sequences of outliers.
  * The first outlier detection is based on the computed gps speed from previous observation.
  * It is completed with following rules:
  *  - If a point is located between two outliers (window size 5), it is also considered as "local" outlier
  *  - The last point of an outlier sequence is considered as a "return point", it is not an outlier
  *  - If a return point is isolated, the preceding point is considered as an outlier
  *
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param outlierColumn Name of boolean column containing outlier identification result (True if outlier)
  * @param aggregationColumnName Column containing group identifier to identify neighbors in observation sequences
  */
class WithOutlierPropagated(dataModel: DataModel, outlierColumn: String, aggregationColumnName: String)
    extends ArlasTransformer(Vector(aggregationColumnName, dataModel.timestampColumn, dataModel.latColumn, dataModel.lonColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // spark window
    val window = Window
      .partitionBy(aggregationColumnName)
      .orderBy(dataModel.timestampColumn)

    def whenPreviousPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lag(dataModel.timestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    def whenNextPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lead(dataModel.timestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    dataset
      .toDF()
      // "Local outliers" are points between 2 important outliers (5 max) they are considered as outlier too
      .withColumn(
        "is_local_outlier",
        not(col(outlierColumn))
          .and(whenNextPointExists(lead(col(outlierColumn), 1).over(window), 1, true)
            .or(whenNextPointExists(lead(col(outlierColumn), 2).over(window), 2, true))
            .or(whenNextPointExists(lead(col(outlierColumn), 3).over(window), 3, true)))
          .and(whenPreviousPointExists(lag(col(outlierColumn), 1).over(window), 1, true)
            .or(whenPreviousPointExists(lag(col(outlierColumn), 2).over(window), 2, true))
            .or(whenPreviousPointExists(lag(col(outlierColumn), 3).over(window), 3, true)))
      )
      .withColumn("is_all_outlier", col(outlierColumn).or(col("is_local_outlier")))
      // A "return point" is the last observation of a sequence identified as outlier, it mean this observation is valid according to next observation
      // An observation preceding an isolated "return point" is considered as an outlier
      .withColumn("is_return_point",
                  whenNextPointExists(col("is_all_outlier")
                                        .and(not(lead(col("is_all_outlier"), 1).over(window))),
                                      1,
                                      false))
      .withColumn(
        "is_final_outlier",
        col("is_all_outlier")
          .and(not(col("is_return_point")))
          .or(whenNextPointExists(lead(col("is_return_point"), 1).over(window), 1, false))
      )
      .drop(outlierColumn, "is_all_outlier", "is_return_point", "is_local_outlier")
      .withColumnRenamed("is_final_outlier", outlierColumn)
  }
}
