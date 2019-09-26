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
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

/**
  * Map some values to another values (of the same type).
  * From the doc, supported types are doubles, strings or booleans but in source code
  * we see that all numerical types are converted to double (float, long...)
  * => see https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/sql/DataFrameNaFunctions
  * .html#replace-java.lang.String:A-java.util.Map-
  * Target column may be a new one, or an existing one (i.a. same column).
  * If sourceColumn and valuesMap are not of the same type, this will not break (simply not replace any value)
  * @param dataModel
  * @param sourceColumn
  * @param targetColumn
  * @param valuesMap
  */
class OtherColValuesMapper[T](dataModel: DataModel,
                              sourceColumn: String,
                              targetColumn: String,
                              valuesMap: Map[T, T])
    extends ArlasTransformer(dataModel, Vector(sourceColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    valuesMap match {
      case m if m.isEmpty => dataset.toDF()
      case _ =>
        val df =
          if (sourceColumn == targetColumn) dataset.toDF()
          else dataset.toDF().withColumn(targetColumn, col(sourceColumn))
        df.na.replace(targetColumn, valuesMap)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    val sourceColumnDataType = transformedSchema.fields.filter(_.name == sourceColumn).head.dataType

    if (!transformedSchema.fieldNames.contains(targetColumn))
      transformedSchema.add(StructField(targetColumn, sourceColumnDataType, true))
    else {
      val targetColumnDataType =
        transformedSchema.fields.filter(_.name == targetColumn).head.dataType
      if (targetColumnDataType != sourceColumnDataType) {
        throw new DataFrameException(
          s"Source and target columns should be of the same type, currently source is ${sourceColumnDataType} and target ${targetColumnDataType}")
      }
      transformedSchema
    }
  }

}
