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

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import io.arlas.data.transform.transformations._
import io.arlas.data.extract.transformations._
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.utils.{BasicApp, CassandraApp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import io.arlas.data.transform.transformations.doPipelineTransform

object Transformer extends BasicApp with CassandraApp {

  override def getName: String = "Transformer"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {

    val targetKeyspace = runOptions.target.split('.')(0)
    val targetTable = runOptions.target.split('.')(1)

    createCassandraKeyspaceIfNotExists(spark, targetKeyspace)

    // read raw data with warming period of transformed data if available
    val df = readData(spark, runOptions, dataModel)

    // transform raw data
    val transformedDf: DataFrame = doPipelineTransform(
      df,
      new WithSequenceId(dataModel),
      new WithSequenceResampledTransformer(dataModel, runOptions, spark),
      new WithoutEdgingPeriod(dataModel, runOptions, spark),
      new WithArlasDistanceTransformer(dataModel, runOptions, spark)
    )

    createCassandraTableIfNotExists(transformedDf, dataModel, targetKeyspace, targetTable)

    transformedDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> targetKeyspace, "table" -> targetTable))
      .mode(SaveMode.Append)
      .save()
  }

  def readData(spark: SparkSession, runOptions: RunOptions, dataModel: DataModel): DataFrame = {
    val start = runOptions.start.getOrElse(ZonedDateTime.now(ZoneOffset.UTC).minusHours(1))
    val stop = runOptions.stop.getOrElse(ZonedDateTime.now(ZoneOffset.UTC))
    val startSeconds = start.toEpochSecond
    val stopSeconds = stop.toEpochSecond

    val df: DataFrame = {
      if (runOptions.source.contains("/")) {
        spark.read.parquet(runOptions.source)
      } else {
        val ks = runOptions.source.split('.')(0)
        val ta = runOptions.source.split('.')(1)

        spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> ta, "keyspace" -> ks))
          .load()
      }
    }.where(
        col(arlasPartitionColumn) >= Integer.valueOf(
          start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
          && col(arlasPartitionColumn) <= Integer.valueOf(
            stop.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      .where(col(arlasTimestampColumn) >= startSeconds && col(arlasTimestampColumn) <= stopSeconds)

    df.transform(addWarmUpPeriodData(spark, runOptions, dataModel, startSeconds))
  }

  /*
   * Add previously transformed data from target on a period
   * that lasts 2 times dataModel.sequenceGap just before transformation.
   * It enables to have history on some fields like arlas_sequence_id.
   */
  def addWarmUpPeriodData(spark: SparkSession,
                          runOptions: RunOptions,
                          dataModel: DataModel,
                          transformerStartSeconds: Long)(sourceDF: DataFrame): DataFrame = {

    val targetKeyspace = runOptions.target.split('.')(0)
    val targetTable = runOptions.target.split('.')(1)

    if (isCassandraTableCreated(spark, targetKeyspace, targetTable)) {
      val startDate = ZonedDateTime
        .ofInstant(Instant.ofEpochSecond(transformerStartSeconds), ZoneOffset.UTC)
        .minusSeconds(2 * dataModel.sequenceGap)
      val endDate = ZonedDateTime
        .ofInstant(Instant.ofEpochSecond(transformerStartSeconds), ZoneOffset.UTC)
      val startSeconds = startDate.toEpochSecond
      val stopSeconds = endDate.toEpochSecond

      spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> targetKeyspace, "table" -> targetTable))
        .load()
        .where(
          col(arlasPartitionColumn) >= Integer.valueOf(
            startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
            && col(arlasPartitionColumn) < Integer.valueOf(
              endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
        .where(
          col(arlasTimestampColumn) >= startSeconds && col(arlasTimestampColumn) <= stopSeconds)
        .unionByName(sourceDF
          .withColumn(arlasSequenceIdColumn, lit(null).cast(StringType))
          .withColumn(arlasDistanceColumn, lit(null).cast(StringType)))
    } else {
      sourceDF.withColumn(arlasSequenceIdColumn, lit(null).cast(StringType))
    }
  }
}
