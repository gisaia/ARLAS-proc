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
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator

object interpolations {

  def splineInterpolateAndResample(dataModel: DataModel,
                                   timeSampling: Long,
                                   rawTimeSerie: List[Map[String, Any]],
                                   columns: Array[String]): List[Map[String, Any]] = {

    // sort timeserie and drop duplicates
    val orderedRawTimeSerie = rawTimeSerie
      .sortBy(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long])
      .distinct

    // create resampled timestamp timeserie
    val tsTimeSerie = orderedRawTimeSerie
      .map(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long].toDouble)
      .toArray
    val minTs =
      (tsTimeSerie.min - tsTimeSerie.min % timeSampling + timeSampling).toLong
    val maxTs =
      (tsTimeSerie.max - tsTimeSerie.max % timeSampling).toLong

    val resampledTimeSerie = List.range(minTs, maxTs, timeSampling)

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
}
