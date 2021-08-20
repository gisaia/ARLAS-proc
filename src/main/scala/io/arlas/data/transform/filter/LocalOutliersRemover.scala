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
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify if an observation is a local outlier based on its neighborhood
  * It is freely adapted from the Hampel filter (use the median values over sliding windows)
  *
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param idColumn Id of the sequence point to analyse
  * @param appliedColumns Features used to detect outliers, each feature is analyse independently
  * @param halfWindowSize Half size of the window used to compute local outliers
  * @param threshold Threshold used on feature innovation to determine whether an observation is an outlier or not
  * @param keepInnovation If True, keep the processed innovation and temporary columns (used for tuning)
  */
class LocalOutliersRemover(dataModel: DataModel,
                           idColumn: String,
                           targetOutlierColumn: String,
                           appliedColumns: List[String],
                           halfWindowSize: Long,
                           threshold: Double,
                           keepInnovation: Boolean = false)
    extends ArlasTransformer(Vector(idColumn, arlasTimestampColumn, dataModel.latColumn, dataModel.lonColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val window = Window
      .partitionBy(idColumn)
      .orderBy(arlasTimestampColumn)
      .rowsBetween(-halfWindowSize, halfWindowSize)

    var filterData = dataset.toDF().withColumn(targetOutlierColumn, lit(false))

    // Filter the outliers once identified
    for (appliedColumn <- appliedColumns) {
      filterData = applyToCol(filterData, appliedColumn, window)
    }
    filterData
  }

  private def applyToCol(df: DataFrame, appliedCol: String, window: WindowSpec): DataFrame = {
    val COLLECT_COLUMN = "collect" + "_" + appliedCol
    val COLLECT_COLUMN_SIZE = "collect_size" + "_" + appliedCol
    val MEDIAN_COLUMN = "median_value" + "_" + appliedCol
    val INNOVATION_COLUMN = "innovation" + "_" + appliedCol

    val filteredDf = df
    // Compute the median over a window
      .withColumn(COLLECT_COLUMN, sort_array(collect_list(appliedCol).over(window)))
      .withColumn(COLLECT_COLUMN_SIZE, size(col(COLLECT_COLUMN)))
      .withColumn(
        MEDIAN_COLUMN,
        when(col(COLLECT_COLUMN_SIZE).equalTo(halfWindowSize * 2 + 1), col(COLLECT_COLUMN)(halfWindowSize))
          .otherwise(col(COLLECT_COLUMN)(halfWindowSize / 2))
      )
      // Compute innovation as the delta between a value and its window median value
      .withColumn(
        INNOVATION_COLUMN,
        col(appliedCol) - col(MEDIAN_COLUMN)
      )
      // If the innovation is over the threshold, the observation is considered as a local outlier
      .withColumn(targetOutlierColumn + "_" + appliedCol, when(col(INNOVATION_COLUMN).gt(threshold), true).otherwise(false))
      // Update the outlier column
      .withColumn(targetOutlierColumn, col(targetOutlierColumn).or(col(targetOutlierColumn + "_" + appliedCol)))

    if (keepInnovation) {
      filteredDf
    } else {
      filteredDf
        .drop(INNOVATION_COLUMN)
        .drop(COLLECT_COLUMN, COLLECT_COLUMN_SIZE, MEDIAN_COLUMN, targetOutlierColumn + "_" + appliedCol)
    }
  }

}
