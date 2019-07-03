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
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
* For a ID that is hold by multiple rows, it computes the 'duration of this id'
  * i.a. <oldest row with this id> - <earlier row with this id>
  * @param dataModel
  * @param idColumn
  * @param durationColumn
  */
class WithDurationFromId(dataModel: DataModel, idColumn: String, durationColumn: String)
  extends ArlasTransformer(dataModel, Vector(idColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window.partitionBy(idColumn)
      .orderBy(arlasTimestampColumn)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    dataset.toDF()
      .withColumn(durationColumn, last(arlasTimestampColumn).over(window) - first(arlasTimestampColumn).over(window))
  }

  override def transformSchema(schema: StructType): StructType = {
    super.transformSchema(schema).add(StructField(durationColumn, LongType, true))
  }
}
