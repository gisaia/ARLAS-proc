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

package io.arlas.data.model.runoptions

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model.{ArgumentMap, Period}

abstract class RunOptionsAbstractFactory[R <: RunOptions](arguments: ArgumentMap) {

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

  val source =
    arguments.get("source") match {
      case Some(source) => source
      case None         => throw new RuntimeException("Missing source argument")
    }

  val target =
    arguments.get("target") match {
      case Some(target) => target
      case None         => throw new RuntimeException("Missing source argument")
    }

  val period =
    Period(start, stop)

  val warmingPeriod =
    arguments.get("warmingPeriod") match {
      case Some(warmingPeriod) => Some(warmingPeriod.toLong)
      case None                => None
    }

  val endingPeriod =
    arguments.get("endingPeriod") match {
      case Some(endingPeriod) => Some(endingPeriod.toLong)
      case None               => None
    }

  def create(): R
}
