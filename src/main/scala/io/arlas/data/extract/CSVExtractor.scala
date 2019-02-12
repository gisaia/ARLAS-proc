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

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import io.arlas.data.extract.transformations._
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.utils.BasicApp
import io.arlas.data.utils.DataFrameHelper._
import org.apache.spark.sql.functions.{col, lit, min}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

object CSVExtractor extends BasicApp {

  override def getName: String = "CSV Extractor"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {
    val PARQUET_BLOCK_SIZE: Int = 32 * 1024 * 1024

    val csvDf = spark.read
      .option("header", "true")
      .csv(runOptions.source)
      .transform(withValidColumnNames())
      .transform(withValidDynamicColumnsType(dataModel))
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val minimumDate = ZonedDateTime.ofInstant(
      Instant.ofEpochSecond(csvDf.select(min(arlasTimestampColumn)).head().getLong(0)),
      ZoneOffset.UTC)

    unionWithParquetData(spark, runOptions, dataModel, csvDf, minimumDate)
      .where(col(arlasTimestampColumn) >= minimumDate.toEpochSecond)
      .write
      .option("compression", "snappy")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE.toString)
      .mode(SaveMode.Append)
      .partitionBy(arlasPartitionColumn)
      .parquet(runOptions.target)
  }

  def unionWithParquetData(spark: SparkSession,
                           runOptions: RunOptions,
                           dataModel: DataModel,
                           sourceDF: DataFrame,
                           minDate: ZonedDateTime): DataFrame = {

    val startDate = minDate.truncatedTo(ChronoUnit.DAYS).minusSeconds(2 * dataModel.sequenceGap)
    val endDate = minDate.truncatedTo(ChronoUnit.DAYS)
    val startSeconds = startDate.toEpochSecond
    val stopSeconds = endDate.toEpochSecond

    Try(spark.read.parquet(runOptions.target)) match {
      case Success(result_df) => {
        val df = result_df
          .where(
            col(arlasPartitionColumn) >= Integer.valueOf(
              startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
              && col(arlasPartitionColumn) < Integer.valueOf(
                endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
          .where(
            col(arlasTimestampColumn) >= startSeconds && col(arlasTimestampColumn) <= stopSeconds)

        sourceDF
          .withColumn(arlasSequenceIdColumn, lit(null).cast(StringType))
          .unionByName(df)
          .transform(fillSequenceId(dataModel))
      }
      case Failure(f) => {
        sourceDF
          .withColumn(arlasSequenceIdColumn, lit(null).cast(StringType))
          .transform(fillSequenceId(dataModel))
      }
    }
  }
}
