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

import io.arlas.data.sql.readFromCsv
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * Convert a country name between different formats
  *
  * @param inputCountryCol Name of the column containing the country name to convert
  * @param outputCountryCol Name of the column to store converted country name
  * @param inputCountryFormat Format of the input country name ("alpha2", "alpha3", "nom", "name", "numérique")
  * @param outputCountryFormat Format of the target country name ("alpha2", "alpha3", "nom", "name", "numérique")
  * @param countryInfoPath Path to the country name file
  * @param spark Spark Session
  */
class WithCountryNameFormatted(inputCountryCol: String,
                               outputCountryCol: String,
                               inputCountryFormat: String = "alpha2",
                               outputCountryFormat: String = "nom",
                               countryInfoPath: String,
                               spark: SparkSession)
// The countryFormat is within: "alpha2", "alpha3", "nom", "name", "numérique"
    extends ArlasTransformer(Vector(inputCountryCol)) {

  def whenDataExists(expression: Column, default: Any = null) =
    when(expression.isNull, default)
      .otherwise(expression)

  val output_country_info_temp_right = "output_country_info_temp_right"
  val input_country_info_temp_right = "input_country_info_temp_right"
  val input_country_info_temp_left = "input_country_info_temp_left"

  val countryInfoDf =
    readFromCsv(spark, ",", true, None, countryInfoPath)
      .withColumnRenamed(outputCountryFormat, output_country_info_temp_right)
      .withColumnRenamed(inputCountryFormat, input_country_info_temp_right)
      .select(input_country_info_temp_right, output_country_info_temp_right)

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset
      .withColumn(input_country_info_temp_left, col(inputCountryCol))
      //join with vessel info
      .join(countryInfoDf, col(input_country_info_temp_left).equalTo(col(input_country_info_temp_right)), "left_outer")
      .withColumn(outputCountryCol, whenDataExists(col(output_country_info_temp_right), col(input_country_info_temp_left)))
      .drop(input_country_info_temp_left, input_country_info_temp_right, output_country_info_temp_right)
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema).add(StructField(outputCountryCol, StringType, true))
}
