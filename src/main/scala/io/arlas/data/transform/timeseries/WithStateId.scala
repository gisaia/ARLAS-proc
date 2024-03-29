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

package io.arlas.data.transform.timeseries

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Compute a state ID in targetIdColumn based on stateColumn.
  * The generated ID looks like <object_id>_<first_timestamp>
  * @param idColumn Column containing the object id
  * @param orderColumn Column containing the field used to order sequences (usually timestamp)
  * @param targetIdColumn Column to store the created ID
  * @param isNewIdColumn should be 'true' for rows that start a new ID.
  * For example, if you expect the rows to have the same state_id for each consecutive row with the same state,
  * you should return a 'true' at each state change.
  */
class WithStateId(idColumn: String, orderColumn: String, targetIdColumn: String, isNewIdColumn: Column)
    extends ArlasTransformer(Vector(orderColumn, idColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window.partitionBy(idColumn).orderBy(orderColumn)

    dataset
      .toDF()
      .withColumn("is_new_id", isNewIdColumn)
      .withColumn("temp_id", when(col("is_new_id").equalTo(true), concat(col(idColumn), lit("#"), col(orderColumn))))
      .withColumn(targetIdColumn, last("temp_id", true).over(window))
      .drop("is_new_id", "temp_id")
  }

  override def transformSchema(schema: StructType): StructType = {
    val newSchema = checkSchema(schema)
    if (!newSchema.fieldNames.contains(targetIdColumn))
      newSchema.add(StructField(targetIdColumn, StringType, true))
    else newSchema
  }

}
