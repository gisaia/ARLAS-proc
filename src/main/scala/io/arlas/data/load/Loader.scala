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

package io.arlas.data.load

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import io.arlas.data.extract.transformations.{
  arlasPartitionColumn,
  arlasTimestampColumn
}
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.utils.{BasicApp, CassandraApp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Loader extends BasicApp with CassandraApp {
  var id = ""

  override def getName: String = "Loader Application"

  override def run(spark: SparkSession,
                   dataModel: DataModel,
                   runOptions: RunOptions): Unit = {
    spark.sparkContext.setLogLevel("Error")

    val start = runOptions.start.getOrElse(
      ZonedDateTime.now(ZoneOffset.UTC).minusHours(1))
    val stop = runOptions.stop.getOrElse(ZonedDateTime.now(ZoneOffset.UTC))
    val startSeconds = start.toEpochSecond
    val stopSeconds = stop.toEpochSecond

    val source = runOptions.source.split(",")(0)

    var csvResultName = "parquet_csv_load"
    var df: DataFrame = null
    if (source.contains("/")) {
      df = spark.read.parquet(source)
    } else {
      val ks = source.split('.')(0)
      val ta = source.split('.')(1)

      df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> ta, "keyspace" -> ks))
        .load()

      csvResultName = "scylladb_csv_load"
    }

    df = df
      .where(
        col(arlasPartitionColumn) >= Integer.valueOf(
          start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
          && col(arlasPartitionColumn) <= Integer.valueOf(
            stop.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      .where(col(arlasTimestampColumn) >= startSeconds && col(
        arlasTimestampColumn) <= stopSeconds)

    var csvName = s"${runOptions.target}/${csvResultName}_period"

    if (!id.trim.isEmpty) {
      df = df
        .where(col(dataModel.idColumn) === id)

      csvName = s"${runOptions.target}/${csvResultName}_${id}"
    }

    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(csvName)
  }

  override def getArgs(map: Loader.ArgumentMap,
                       list: List[String]): Loader.ArgumentMap = {
    list match {
      case Nil                           => map
      case "--id-value" :: value :: tail => id = value
      case argument :: tail              => println("Unknown argument " + argument)
    }
    super.getArgs(map, list)
  }
}
