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

package io.arlas.data.utils

import io.arlas.data.model.DataModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType

object DataFrameHelper {

  // DataFrame transformations
  def withValidColumnNames()(df: DataFrame): DataFrame = {
    df.select(df.columns.map { c =>
      df.col(c)
        .as(
          c.replaceAll("\\s", "_") // replace whitespaces with '_'
            .replaceAll("[^A-Za-z0-9_]", "") // remove special characters
            .replaceAll("^_", "") // replace strings that start with '_' with empty char
            .toLowerCase())
    }: _*)
  }

  def withValidDynamicColumnsType(dataModel: DataModel)(
      df: DataFrame): DataFrame = {
    //Dynamic columns only support double values
    val columns = df.columns.map(column => {
      if (dataModel.dynamicFields.contains(column))
        df.col(column).cast(DoubleType)
      else df.col(column)
    })
    df.select(columns: _*)
  }

}

case class DataFrameException(message: String) extends Exception(message)
