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

package io.arlas.data.app

import io.arlas.data.model.runoptions.RunOptions
import io.arlas.data.model.{ArgumentMap, DataModel}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait BasicApp[R <: RunOptions] {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def appArguments =
    Seq("id",
        "timestamp",
        "timeformat",
        "lat",
        "lon",
        "speed",
        "distance",
        "dynamic",
        "warmingPeriod",
        "endingPeriod",
        "start",
        "stop",
        "source",
        "target")

  def getName: String

  val usage = s"""
    Usage: ${this.getClass}
              [--id string]
              [--timestamp string]
              [--lat string]
              [--lon string]
              [--speed string]
              [--distance string]
              [--dynamic coma,separated,string]
              [--start YYYY-MM-DDThh:mm:ss+00:00]
              [--stop YYYY-MM-DDThh:mm:ss+00:00]
              [--warmingPeriod long]
              [--endingPeriod long]
              --source string
              --target string
  """

  def run(spark: SparkSession, dataModel: DataModel, runOptions: R): Unit

  def initSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName(getName)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    val (sourcePath, targetPath, startStr, stopStr) =
      (args(0), args(1), args(2), args(3))
    if (args.length == 0) println(usage)
    val arglist = args.toList
    val options = getArgs(Map(), arglist)
    logger.info(s"""App arguments : \n${options}""")

    val dataModel = getDataModel(options)
    val runOptions = getRunOptions(options)
    logger.info(s"""Data model : \n${dataModel}""")
    logger.info(s"""Run options : \n${runOptions}""")

    val spark: SparkSession = initSparkSession

    run(spark, dataModel, runOptions)
  }

  def getArgs(map: ArgumentMap, list: List[String]): ArgumentMap = {
    list match {
      case Nil => map
      case head :: value :: tail if appArguments.map(arg => s"--${arg}").contains(head) =>
        getArgs(map ++ Map(head.replaceFirst("^\\-\\-", "") -> value), tail)
      case argument :: tail =>
        println("Unknown argument " + argument)
        getArgs(map, list.tail)
    }
  }

  def getDataModel(arguments: ArgumentMap): DataModel = {
    DataModel(
      idColumn = arguments.getOrElse("id", "id"),
      timestampColumn = arguments.getOrElse("timestamp", "timestamp"),
      timeFormat = arguments.getOrElse("timeformat", "yyyy-MM-dd'T'HH:mm:ssXXX"),
      latColumn = arguments.getOrElse("lat", "lat"),
      lonColumn = arguments.getOrElse("lon", "lon")
    )
  }

  def getRunOptions(arguments: ArgumentMap): R

}
