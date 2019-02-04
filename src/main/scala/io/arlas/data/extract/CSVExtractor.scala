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

package io.arlas.data.extract

import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.utils.DataFrameHelper._
import io.arlas.data.extract.transformations._
import io.arlas.data.utils.BasicApp
import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVExtractor extends BasicApp {

  override def getName: String = "CSV Extractor"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {
    val PARQUET_BLOCK_SIZE: Int = 32 * 1024 * 1024

    val csvDf = spark.read
      .option("header", "true")
      .csv(runOptions.source)
      .transform(withValidColumnNames())
      .transform(withValidDynamicColumnsType(dataModel))
    //TODO try to support schema infering without time column issues

    csvDf
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))
      .write
      .option("compression", "snappy")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE.toString)
      .mode(SaveMode.Append)
      .partitionBy(arlasPartitionColumn)
      .parquet(runOptions.target)
  }
}