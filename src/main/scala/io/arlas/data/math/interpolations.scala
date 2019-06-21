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

package io.arlas.data.math

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.ml.filter.{BasicOutlierFilter, GolayFilter}
import io.arlas.ml.parameter.{BasicOutlierFilterParameter, GolayFilterParameter}
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator

object interpolations {

  type Sequence = List[Map[String, Any]]

  def splineInterpolateAndResample(dataModel: DataModel,
                                   rawTimeSerie: List[Map[String, Any]],
                                   columns: Array[String]): List[Map[String, Any]] = {

    // sort timeserie and drop duplicates
    val orderedRawTimeSerie = sortSequence(rawTimeSerie, arlasTimestampColumn).distinct

    // create resampled timestamp timeserie
    val tsTimeSerie = orderedRawTimeSerie
      .map(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long].toDouble)
      .toArray
    val minTs =
      (tsTimeSerie.min - tsTimeSerie.min % dataModel.timeSampling + dataModel.timeSampling).toLong
    val maxTs =
      (tsTimeSerie.max - tsTimeSerie.max % dataModel.timeSampling).toLong

    val resampledTimeSerie = List.range(minTs, maxTs, dataModel.timeSampling)

    // get interpolation functions for each column (dynamics and static)
    val interpolator = new SplineInterpolator()
    val interpolators = columns
      .map(column => {
        //TODO remove try block and improve code to be resilient to exceptions
        //TODO support dynamic columns with null cells
        try {
          if (dataModel.dynamicFields.contains(column)) {
            val values = orderedRawTimeSerie
              .map(row => row.getOrElse(column, 0.0d).toString.toDouble)
              .toArray
            val interpolatedFunction =
              interpolator.interpolate(tsTimeSerie, values)
            val function = (ts: Long) => interpolatedFunction.value(ts.toDouble)
            (column -> function)
          } else {
            val value = orderedRawTimeSerie
              .map(row => row.getOrElse(column, null))
              .filter(_ != null)
              .head
            val function = (ts: Long) => value.toString
            (column -> function)
          }
        } catch {
          case e: Throwable => {
            val function = (ts: Long) => null
            (column -> function)
          }
        }

      })
      .toMap

    // create resampled and interpolated row timeserie
    val timeFormatter = DateTimeFormatter.ofPattern(dataModel.timeFormat)
    val partitionFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    resampledTimeSerie
      .map(timestamp => {
        columns
          .map(column =>
            column -> {
              if (column.equals(arlasTimestampColumn)) {
                timestamp
              } else if (column.equals(arlasPartitionColumn)) {
                Integer.valueOf(
                  ZonedDateTime
                    .ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC)
                    .format(partitionFormatter))
              } else if (column.equals(dataModel.timestampColumn)) {
                ZonedDateTime
                  .ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC)
                  .format(timeFormatter)
              } else {
                val interpolator =
                  interpolators.getOrElse(column, (_: Long) => null)
                interpolator(timestamp)
              }
          })
          .toMap
      })
  }

  def golayInterpolateAndResample(dataModel: DataModel,
                                  rawSequence: Sequence,
                                  columns: Array[String],
                                  minSequenceStopEpochSeconds: Long): Sequence = {

    val orderedPointsSequence: Sequence =
      sortSequence(rawSequence, arlasTimestampColumn)

    val tsSequence = orderedPointsSequence
      .map(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long].toDouble)
      .toArray
    val minTs =
      (tsSequence.min - tsSequence.min % dataModel.timeSampling + dataModel.timeSampling).toLong
    val maxTs =
      (tsSequence.max - tsSequence.max % dataModel.timeSampling).toLong

    // ensure that sequence is at least part of the job period
    //TODO apply this filtering before the interpolation
    if (maxTs <= minSequenceStopEpochSeconds) {
      List()

    } else {

      val (localPoints: Sequence, firstLat: Double, firstLon: Double) = GeographicUtils
        .sequenceToLocalCoordinates(orderedPointsSequence, dataModel.latColumn, dataModel.lonColumn)

      def getRandomRawValue(column: String): Any = {
        rawSequence
          .map(row => row.getOrElse(column, null))
          .filter(_ != null)
          .head
      }

      val localPointsFilteredAndResampled: Sequence =
        columns
          //filter the dynamic values
          .filter(dataModel.dynamicFields.contains(_))
          .flatMap { col =>
            {
              val cleanLocalPoints = BasicOutlierFilter.filter(
                localPoints
                  .map(p =>
                         (p(arlasTimestampColumn).asInstanceOf[Long]
                          -> p(col).asInstanceOf[Double]))
                  .toMap,
                new BasicOutlierFilterParameter(
                  dataModel.basicOutlierFilterModel.median,
                  dataModel.basicOutlierFilterModel.innovationWindowSize)
                )

              val res = GolayFilter
                .filter(cleanLocalPoints,
                        new GolayFilterParameter(dataModel.timeSampling,
                                                 dataModel.golayFilterModel.window,
                                                 dataModel.golayFilterModel.windowMinSize))
                .map(r => {
                  Map(
                    r._1 -> Map(
                      col -> r._2.value,
                      "d" + col -> r._2.dValue,
                      "dd" + col -> r._2.ddValue
                      ))
                })
              res
            }.reduce(_ ++ _)
                   }
          //transform the List[Map[timestamp, Map[key, values]]] with multiple times the same timestamp (one for each
          // dynamic column) to a usual List[Map[key amound which timestamp, values]]
          .groupBy(_._1)
          .map { valuesByTimestamp =>
          {
            valuesByTimestamp._2
              .flatMap(_._2)
              .toMap ++ columns
              //add the static sequence values
              .filterNot(dataModel.dynamicFields.contains(_))
              .map {
                     case dataModel.timestampColumn =>
                       (dataModel.timestampColumn -> ZonedDateTime
                         .ofInstant(Instant.ofEpochSecond(valuesByTimestamp._1), ZoneOffset.UTC)
                         .format(DateTimeFormatter.ofPattern(dataModel.timeFormat)))
                     case x => (x -> getRandomRawValue(x))
                   }
              .toMap
          }
               }
          .toList
          .asInstanceOf[Sequence]

      GeographicUtils.sequenceFromLocalCoordinates(localPointsFilteredAndResampled,
                                                   dataModel,
                                                   firstLat,
                                                   firstLon)
    }
  }

  private def sortSequence(sequence: Sequence, sortColumn: String): Sequence = {

    sequence
      .sortBy(row => row.getOrElse(sortColumn, 0l).asInstanceOf[Long])
  }

}
