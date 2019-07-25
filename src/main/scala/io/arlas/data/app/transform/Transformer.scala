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
import io.arlas.data.model.{ArgumentMap, DataModel, Period}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.CassandraTool
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.arlas.data.model.runoptions.{RunOptionsBasic, RunOptionsBasicFactory}

object Transformer extends BasicApp[RunOptionsBasic] with CassandraTool {

  override def getName: String = "Transformer"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptionsBasic): Unit = {

    //value set for the integration tests to pass,
    //TODO tests should be rewritten and visibilityTimeout managed in another way
    val visibilityTimeout: Int = 120

    // read raw data with warming period of transformed data if available
    val df = readData(spark, runOptions, dataModel, visibilityTimeout)

    // transform raw data
    val transformedDf: DataFrame = df
      .asArlasVisibleSequencesFromTimestamp(dataModel, visibilityTimeout)

    transformedDf.writeToScyllaDB(spark, dataModel, runOptions.target)
  }

  def readData(spark: SparkSession, runOptions: RunOptionsBasic, dataModel: DataModel, visibilityTimeout: Int): DataFrame = {

    val df: DataFrame = {
      if (runOptions.source.contains("/")) {
        readFromParquet(spark, runOptions.source)
      } else {
        readFromScyllaDB(spark, runOptions.source)
      }
    }.filterOnPeriod(runOptions.period)
      .withEmptyCol(arlasVisibleSequenceIdColumn)
      .withEmptyCol(arlasVisibilityStateColumn)

    df.transform(addWarmUpPeriodData(spark, runOptions, dataModel, visibilityTimeout))
  }

  /*
   * Add previously transformed data from target on a period
   * that lasts 2 times dataModel.visibilityTimeout just before transformation.
   * It enables to have history on some fields like arlas_visible_sequence_id.
   */
  def addWarmUpPeriodData(spark: SparkSession, runOptions: RunOptionsBasic, dataModel: DataModel, visibilityTimeout: Int)(
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

      val warmUpStart = warmUpEnd.minusSeconds(2 * visibilityTimeout)
      val warmUpPeriod = Period(Some(warmUpStart), Some(warmUpEnd))

      readFromScyllaDB(spark, runOptions.target)
        .filterOnPeriod(warmUpPeriod)
        .unionByName(sourceDF)

    } else {
      sourceDF
    }
  }

  override def getRunOptions(arguments: ArgumentMap): RunOptionsBasic = RunOptionsBasicFactory(arguments)
}
