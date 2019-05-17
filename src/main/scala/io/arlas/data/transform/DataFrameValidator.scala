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
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, regexp_replace, to_timestamp}

class DataFrameValidator(dataModel: DataModel) extends ArlasTransformer(dataModel) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF
      .transform(withValidColumnNames())
      .transform(withValidDynamicColumnsType())
  }

  def withValidColumnNames()(df: DataFrame): DataFrame = {
    df.select(df.columns.map { c =>
      df.col(c)
        .as(getValidColumnName(c))
    }: _*)
  }

  def getValidColumnName(columnName: String): String = {
    columnName
      .replaceAll("\\s", "_") // replace whitespaces with '_'
      .replaceAll("[^A-Za-z0-9_]", "") // remove special characters
      .replaceAll("^_", "") // replace strings that start with '_' with empty char
      .toLowerCase()
  }

  def withValidDynamicColumnsType()(df: DataFrame): DataFrame = {

    //Dynamic columns only support double values
    //For those dynamic columns, if it is a string, replace possible coma "," (decimal european format) in decimal with a dot ".", that spark supports
    df.columns.filter(dataModel.dynamicFields.contains(_)).foldLeft(df){
                             (dataframe, column) =>
                               if (df.schema.filter(c => c.name == column && c.dataType == StringType).nonEmpty) {
                                 dataframe
                                   .withColumn(column, regexp_replace(col(column), ",", ".").cast(DoubleType))
                               } else if (df.schema.filter(c => c.name == column && c.dataType == DoubleType).isEmpty) {
                                 dataframe.withColumn(column, col(column).cast(DoubleType))
                               } else dataframe
                                           }

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      checkSchema(schema)
        .map(field => {
          StructField(getValidColumnName(field.name), field.dataType, field.nullable)
        })
        .map(field => {
          if (dataModel.dynamicFields.contains(field.name)) {
            StructField(field.name, DoubleType, field.nullable)
          } else {
            field
          }
        })
    )
  }
}
