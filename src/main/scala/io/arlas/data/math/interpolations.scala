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
                                   rawSequence: List[Map[String, Any]],
                                   columns: Array[String]): List[Map[String, Any]] = {

    // sort sequence and drop duplicates
    val orderedRawSequence = rawSequence
      .sortBy(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long])
      .distinct

    // create resampled timestamp sequence
    val tsSequence = orderedRawSequence
      .map(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long].toDouble)
      .toArray
    val minTs =
      (tsSequence.min - tsSequence.min % dataModel.timeSampling + dataModel.timeSampling).toLong
    val maxTs =
      (tsSequence.max - tsSequence.max % dataModel.timeSampling).toLong

    val resampledSequence = List.range(minTs, maxTs, dataModel.timeSampling)

    // get interpolation functions for each column (dynamics and static)
    val interpolator = new SplineInterpolator()
    val interpolators = columns
      .map(column => {
        //TODO remove try block and improve code to be resilient to exceptions
        //TODO support dynamic columns with null cells
        try {
          if (dataModel.dynamicFields.contains(column)) {
            val values = orderedRawSequence
              .map(row => row.getOrElse(column, 0.0d).toString.toDouble)
              .toArray
            val interpolatedFunction =
              interpolator.interpolate(tsSequence, values)
            val function = (ts: Long) => interpolatedFunction.value(ts.toDouble)
            (column -> function)
          } else {
            val value = orderedRawSequence
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

    // create resampled and interpolated row sequence
    val timeFormatter = DateTimeFormatter.ofPattern(dataModel.timeFormat)
    val partitionFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    resampledSequence
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
