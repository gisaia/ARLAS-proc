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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class WithArlasDeltaTimestamp(dataModel: DataModel,
                              spark: SparkSession,
                              aggregationColumnName: String)
    extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn, aggregationColumnName)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window
      .partitionBy(aggregationColumnName)
      .orderBy(arlasTimestampColumn)

    dataset
      .withColumn(
        arlasDeltaTimestampColumn,
        when(lag(arlasTimestampColumn, 1).over(window).isNull, null)
          .otherwise(col(arlasTimestampColumn) - lag(arlasTimestampColumn, 1).over(window))
      )
      .withColumn(
        arlasPreviousDeltaTimestampColumn,
        when(lag(arlasTimestampColumn, 2).over(window).isNull, null)
          .otherwise(
            lag(arlasTimestampColumn, 1).over(window) - lag(arlasTimestampColumn, 2).over(window))
      )
      .withColumn(
        arlasDeltaTimestampVariationColumn,
        when(col(arlasDeltaTimestampColumn).isNull or col(arlasPreviousDeltaTimestampColumn).isNull, null)
          .otherwise(col(arlasDeltaTimestampColumn) - col(arlasPreviousDeltaTimestampColumn))
        )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasDeltaTimestampColumn, LongType, true))
      .add(StructField(arlasPreviousDeltaTimestampColumn, LongType, true))
      .add(StructField(arlasDeltaTimestampVariationColumn, LongType, true))
  }

}
