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

package io.arlas.data.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameReader {

  def readFromCsv(spark: SparkSession, source: String, delimiter: String = ","): DataFrame = {
    readFromMulipleCsvs(spark, delimiter, source)
  }

  def readFromMulipleCsvs(spark: SparkSession, delimiter: String, sources: String*): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", delimiter)
      .csv(sources :_*)
  }

  def readFromParquet(spark: SparkSession, source: String) = {
    spark.read.parquet(source)
  }

  def readFromScyllaDB(spark: SparkSession, source: String): DataFrame = {
    val sourceKeyspace = source.split('.')(0)
    val sourceTable = source.split('.')(1)
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceTable, "keyspace" -> sourceKeyspace))
      .load()
  }
}
