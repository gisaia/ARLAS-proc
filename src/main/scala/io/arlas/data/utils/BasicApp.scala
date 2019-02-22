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

package io.arlas.data.utils

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model.{DataModel, RunOptions}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait BasicApp {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def getName: String

  val usage = s"""
    Usage: ${this.getClass}
              [--id string]
              [--timestamp string]
              [--lat string]
              [--lon string]
              [--dynamic coma,separated,string]
              [--gap int]
              [--start YYYY-MM-DDThh:mm:ss+00:00]
              [--stop YYYY-MM-DDThh:mm:ss+00:00]
              --source string
              --target string
  """

  def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit

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

    // For implicit conversions like converting RDDs to DataFrames

    run(spark, dataModel, runOptions)
  }

  type ArgumentMap = Map[String, String]
  def getArgs(map: ArgumentMap, list: List[String]): ArgumentMap = {
    list match {
      case Nil => map
      case "--id" :: value :: tail =>
        getArgs(map ++ Map("id" -> value), tail)
      case "--timestamp" :: value :: tail =>
        getArgs(map ++ Map("timestamp" -> value), tail)
      case "--timeformat" :: value :: tail =>
        getArgs(map ++ Map("timeformat" -> value), tail)
      case "--lat" :: value :: tail =>
        getArgs(map ++ Map("lat" -> value), tail)
      case "--lon" :: value :: tail =>
        getArgs(map ++ Map("lon" -> value), tail)
      case "--dynamic" :: value :: tail =>
        getArgs(map ++ Map("dynamic" -> value), tail)
      case "--gap" :: value :: tail =>
        getArgs(map ++ Map("gap" -> value), tail)
      case "--warmingPeriod" :: value :: tail =>
        getArgs(map ++ Map("warmingPeriod" -> value), tail)
      case "--endingPeriod" :: value :: tail =>
        getArgs(map ++ Map("endingPeriod" -> value), tail)
      case "--start" :: value :: tail =>
        getArgs(map ++ Map("start" -> value), tail)
      case "--stop" :: value :: tail =>
        getArgs(map ++ Map("stop" -> value), tail)
      case "--source" :: value :: tail =>
        getArgs(map ++ Map("source" -> value), tail)
      case "--target" :: value :: tail =>
        getArgs(map ++ Map("target" -> value), tail)
      case argument :: tail =>
        println("Unknown argument " + argument)
        getArgs(map, list.tail)
    }
  }

  def getDataModel(arguments: ArgumentMap): DataModel = {
    DataModel(
      arguments.getOrElse("id", "id"),
      arguments.getOrElse("timestamp", "timestamp"),
      arguments.getOrElse("timeformat", "yyyy-MM-dd'T'HH:mm:ssXXX"),
      arguments.getOrElse("lat", "lat"),
      arguments.getOrElse("lon", "lon"),
      arguments.getOrElse("dynamic", "lat,lon").split(","),
      arguments.getOrElse("gap", "3600").toInt
    )
  }

  def getRunOptions(arguments: ArgumentMap): RunOptions = {
    RunOptions(
      arguments.get("source") match {
        case Some(source) => source
        case None         => throw new RuntimeException("Missing source argument")
      },
      arguments.get("target") match {
        case Some(target) => target
        case None         => throw new RuntimeException("Missing source argument")
      },
      arguments.get("start") match {
        case Some(startStr) =>
          Some(ZonedDateTime.parse(startStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        case None => None
      },
      arguments.get("stop") match {
        case Some(stopStr) =>
          Some(ZonedDateTime.parse(stopStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        case None => None
      },
      arguments.get("warmingPeriod") match {
        case Some(warmingPeriod) => Some(warmingPeriod.toLong)
        case None                => None
      },
      arguments.get("endingPeriod") match {
        case Some(endingPeriod) => Some(endingPeriod.toLong)
        case None               => None
      }
    )
  }
}
