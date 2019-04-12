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

package io.arlas.data.app.transform

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import io.arlas.data.app.BasicApp
import io.arlas.data.model.{DataModel, Period, RunOptions}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns.{arlasDistanceColumn, arlasTimestampColumn}
import io.arlas.data.transform._
import io.arlas.data.utils.CassandraTool
import org.apache.spark.sql.functions.{lit, min}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Transformer extends BasicApp with CassandraTool {

  override def getName: String = "Transformer"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {

    // read raw data with warming period of transformed data if available
    val df = readData(spark, runOptions, dataModel)

    // transform raw data
    val transformedDf: DataFrame = df.enrichWithArlas(
      new ArlasSequenceIdFiller(dataModel),
      new ArlasSequenceResampler(dataModel, runOptions.period.start, spark),
      new EdgingPeriodRemover(dataModel, runOptions, spark),
      new WithArlasDistance(dataModel, spark)
    )

    transformedDf.writeToScyllaDB(spark, dataModel, runOptions.target)
  }

  def readData(spark: SparkSession, runOptions: RunOptions, dataModel: DataModel): DataFrame = {

    val df: DataFrame = {
      if (runOptions.source.contains("/")) {
        readFromParquet(spark, runOptions.source)
      } else {
        readFromScyllaDB(spark, runOptions.source)
      }
    }.filterOnPeriod(runOptions.period)
      .enrichWithArlas(new WithEmptyArlasSequenceId(dataModel))

    df.transform(addWarmUpPeriodData(spark, runOptions, dataModel))
  }

  /*
   * Add previously transformed data from target on a period
   * that lasts 2 times dataModel.sequenceGap just before transformation.
   * It enables to have history on some fields like arlas_sequence_id.
   */
  def addWarmUpPeriodData(spark: SparkSession, runOptions: RunOptions, dataModel: DataModel)(
      sourceDF: DataFrame): DataFrame = {

    val targetKeyspace = runOptions.target.split('.')(0)
    val targetTable = runOptions.target.split('.')(1)

    if (isCassandraTableCreated(spark, targetKeyspace, targetTable)) {

      val warmUpEnd = runOptions.period.start match {
        case Some(start) =>
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(start.toEpochSecond), ZoneOffset.UTC)
        case _ => {
          // if runOptions.period.start==None , use sourceDF min date
          ZonedDateTime.ofInstant(
            sourceDF.select(min(arlasTimestampColumn)).head.getTimestamp(0).toInstant,
            ZoneOffset.UTC)
        }
      }

      val warmUpStart = warmUpEnd.minusSeconds(2 * dataModel.sequenceGap)
      val warmUpPeriod = Period(Some(warmUpStart), Some(warmUpEnd))

      readFromScyllaDB(spark, runOptions.target)
        .filterOnPeriod(warmUpPeriod)
        .unionByName(sourceDF
          .withColumn(arlasDistanceColumn, lit(null).cast(StringType)))

    } else {
      sourceDF
    }
  }
}
