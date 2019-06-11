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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

class SeldomValuesByObjectReplacer(dataModel: DataModel, targetCol: String, minPercent: Double, defaultValue: String)
  extends ArlasTransformer(dataModel, Vector(targetCol)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val windowObjectTarget = Window.partitionBy(dataModel.idColumn, targetCol)
    val windowObject = Window.partitionBy(dataModel.idColumn)

    val countByObjectAndTargetCol = "_countByObjectAndTarget"
    val countByObjectCol = "_countByObject"

    dataset.toDF()
      .withColumn(countByObjectAndTargetCol, count(targetCol).over(windowObjectTarget))
      .withColumn(countByObjectCol, count(dataModel.idColumn).over(windowObject))
      .withColumn(
        targetCol,
        when(count(targetCol).over(windowObjectTarget) * 100 / count(dataModel.idColumn).over(windowObject) < minPercent, defaultValue)
          .otherwise(dataset(targetCol)))
      .drop(countByObjectAndTargetCol)
      .drop(countByObjectCol)
  }

  override def transformSchema(schema: StructType): StructType = {

    val transformedSchema = super.transformSchema(schema)
    if (transformedSchema.fields.filter(_.name == targetCol).head.dataType != StringType) {
      throw new DataFrameException(s"The column ${targetCol} set for ${this.getClass.getName} should be String, other type is not yet supported")
    }
    transformedSchema
  }

}
