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
import java.time.{ZoneOffset, ZonedDateTime}

import io.arlas.data.extract.transformations._
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.transform.transformations._
import io.arlas.data.utils.{BasicApp, CassandraApp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Transformer extends BasicApp with CassandraApp {

  override def getName: String = "Transformer"

  override def run(spark: SparkSession,
                   dataModel: DataModel,
                   runOptions: RunOptions): Unit = {

    createCassandraKeyspace(spark, runOptions)

    val df = loadData(spark, runOptions)

    // transform raw data
    var transformedDf: DataFrame = doPipelineTransform(
      df,
      new WithSequenceIdTransformer(dataModel),
      new WithSequenceResampledTransformer(dataModel, spark),
      new WithoutEdgingPeriod(dataModel, runOptions, spark)
    )

    createCassandraTable(transformedDf, dataModel)

    // write transformed data
    transformedDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> resultTable))
      .mode(SaveMode.Append)
      .save()
  }

  def loadData(spark: SparkSession, runOptions: RunOptions): DataFrame = {
    val start = runOptions.start.getOrElse(
      ZonedDateTime.now(ZoneOffset.UTC).minusHours(1))
    val stop = runOptions.stop.getOrElse(ZonedDateTime.now(ZoneOffset.UTC))
    val startSeconds = start.toEpochSecond
    val stopSeconds = stop.toEpochSecond

    var df: DataFrame = null
    if (runOptions.source.contains("/")) {
      df = spark.read.parquet(runOptions.source)
    } else {
      val ks = runOptions.source.split('.')(0)
      val ta = runOptions.source.split('.')(1)

      df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> ta, "keyspace" -> ks))
        .load()
    }

    df = df
      .where(
        col(arlasPartitionColumn) >= Integer.valueOf(
          start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
          && col(arlasPartitionColumn) <= Integer.valueOf(
            stop.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      .where(col(arlasTimestampColumn) >= startSeconds && col(
        arlasTimestampColumn) <= stopSeconds)

    df
  }
}
