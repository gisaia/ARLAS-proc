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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model.{DataModel, Period, ProcessingConfiguration, RunOptions}
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
              [--timeout int]
              [--start YYYY-MM-DDThh:mm:ss+00:00]
              [--stop YYYY-MM-DDThh:mm:ss+00:00]
              [--hmmModelPath string]
              --source string
              --target string
  """

  def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions, processingConf: ProcessingConfiguration): Unit

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

    run(spark, dataModel, runOptions, getProcessingConfiguration())
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
      case "--speed" :: value :: tail =>
        getArgs(map ++ Map("speed" -> value), tail)
      case "--dynamic" :: value :: tail =>
        getArgs(map ++ Map("dynamic" -> value), tail)
      case "--timeout" :: value :: tail =>
        getArgs(map ++ Map("timeout" -> value), tail)
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
      case "--movingStateModelPath" :: value :: tail =>
        getArgs(map ++ Map("movingStateModelPath" -> value), tail)
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
      lonColumn = arguments.getOrElse("lon", "lon"),
      speedColumn = arguments.getOrElse("speed", "speed"),
      dynamicFields = arguments.getOrElse("dynamic", "lat,lon,speed").split(",")
    )
  }

  def getRunOptions(arguments: ArgumentMap): RunOptions = {
    val start = arguments.get("start") match {
      case Some(startStr) =>
        Some(ZonedDateTime.parse(startStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
      case None => None
    }
    val stop = arguments.get("stop") match {
      case Some(stopStr) =>
        Some(ZonedDateTime.parse(stopStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
      case None => None
    }

    RunOptions(
      arguments.get("source") match {
        case Some(source) => source
        case None         => throw new RuntimeException("Missing source argument")
      },
      arguments.get("target") match {
        case Some(target) => target
        case None         => throw new RuntimeException("Missing source argument")
      },
      Period(start, stop),
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

  def getProcessingConfiguration(): ProcessingConfiguration = {
    new ProcessingConfiguration()
  }
}
