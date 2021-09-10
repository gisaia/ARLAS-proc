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

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Compute the duration since last observation of the same object
  * @param idColumn Column containing the object identifier
  * @param timestampColumn Column containing the timestamp of observations
  * @param targetDurationColumn Name of the column to store computed duration (s)
  */
class WithDuration(idColumn: String, timestampColumn: String, targetDurationColumn: String)
    extends ArlasTransformer(Vector(idColumn, timestampColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // spark window
    val window = Window
      .partitionBy(idColumn)
      .orderBy(timestampColumn)

    def whenPreviousPointExists(expression: Column, offset: Int = 1, default: Any = null) =
      when(lag(timestampColumn, offset).over(window).isNull, default)
        .otherwise(expression)

    dataset
      .toDF()
      .withColumn( // track_duration_s = ts(start) - ts(end)
                  targetDurationColumn,
                  whenPreviousPointExists(col(timestampColumn) - lag(timestampColumn, 1).over(window)))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(targetDurationColumn, IntegerType, false))
  }
}
