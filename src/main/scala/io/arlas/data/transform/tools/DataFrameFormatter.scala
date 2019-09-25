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

package io.arlas.data.transform.tools

import io.arlas.data.model.DataModel
import io.arlas.data.transform.{ArlasTransformer, DataFrameException}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class DataFrameFormatter(dataModel: DataModel, doubleColumns: Vector[String] = Vector.empty)
    extends ArlasTransformer() {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF
      .transform(withValidColumnNames())
      .transform(withNoDuplicates())
      .transform(withRequiredColumns())
      .transform(withValidDoubleColumns())
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

  def withNoDuplicates()(df: DataFrame): DataFrame = {
    df.dropDuplicates(dataModel.idColumn, dataModel.timestampColumn)
  }

  def withRequiredColumns()(df: DataFrame): DataFrame = {
    val colsNotFound =
      (doubleColumns ++ Vector(dataModel.lonColumn,
                               dataModel.latColumn,
                               dataModel.idColumn,
                               dataModel.timestampColumn)).distinct.diff(df.schema.fieldNames)
    if (colsNotFound.length > 0) {
      throw DataFrameException(
        s"The ${colsNotFound.mkString(", ")} columns are not included in the DataFrame with the following columns: ${df.schema.fieldNames
          .mkString(", ")}")
    }
    df
  }

  def withValidDoubleColumns()(df: DataFrame): DataFrame = {
    df.columns
      .filter((doubleColumns ++ Vector(dataModel.lonColumn, dataModel.latColumn))
        .contains(_))
      .foldLeft(df) { (dataframe, column) =>
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
    )
  }

}
