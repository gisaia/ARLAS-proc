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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Enrich the vessel with flag nationality from its MMSI first 3 digits
  *
  * @param mmsiColumn Name of the column containing vessel mmsi
  * @param targetFlagColumn Name of the column to store vessel flag
  * @param flagTablePath Unit of the computed speed
  * @param countryFormat The countryFormat is within: "alpha2", "alpha3", "name"
  * @param spark Spark Session
  */
class WithMmsiFlag(mmsiColumn: String, targetFlagColumn: String, flagTablePath: String, countryFormat: String = "name", spark: SparkSession)
    extends ArlasTransformer(Vector(mmsiColumn)) {

  val code_info_temp_right = "code"
  val code_info_temp_left = "input_country_info_temp_left"

  val flagMmsiTableDf =
    spark.read
      .option("header", true)
      .csv(flagTablePath)
      .withColumnRenamed(countryFormat, targetFlagColumn)
      .select(code_info_temp_right, targetFlagColumn)

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .withColumn(code_info_temp_left, col(mmsiColumn).substr(lit(1), lit(3)))
      //join with vessel info
      .join(flagMmsiTableDf, col(code_info_temp_left).equalTo(col(code_info_temp_right)), "left_outer")
      .drop(code_info_temp_right, code_info_temp_left)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(targetFlagColumn, DoubleType, false))
  }
}
