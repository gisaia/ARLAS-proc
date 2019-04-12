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

package io.arlas.data.app.load

import io.arlas.data.app.BasicApp
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Loader extends BasicApp {
  var id = ""

  override def getName: String = "Loader Application"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {
    spark.sparkContext.setLogLevel("Error")

    val source = runOptions.source.split(",")(0)

    val csvResultName =
      if (runOptions.source.contains("/")) { "parquet_csv_load" } else { "scylladb_csv_load" }
    val df: DataFrame = {
      if (runOptions.source.contains("/")) {
        readFromParquet(spark, source)
      } else {
        readFromScyllaDB(spark, source)
      }
    }.filterOnPeriod(runOptions.period)

    val csvName = if (!id.trim.isEmpty) { s"${runOptions.target}/${csvResultName}_period" } else {
      s"${runOptions.target}/${csvResultName}_${id}"
    }
    val dfFiltered = if (!id.trim.isEmpty) {
      df.where(col(dataModel.idColumn) === id)
    } else { df }

    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(csvName)
  }

  override def getArgs(map: Loader.ArgumentMap, list: List[String]): Loader.ArgumentMap = {
    list match {
      case Nil                           => map
      case "--id-value" :: value :: tail => id = value
      case argument :: tail              => println("Unknown argument " + argument)
    }
    super.getArgs(map, list)
  }
}
