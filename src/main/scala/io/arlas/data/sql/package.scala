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

package io.arlas.data

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.model._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, struct}

import scala.collection.mutable

package object sql extends DataFrameReader {

  def getPeriod(start: String, stop: String): Period = {
    Period(Some(ZonedDateTime.parse(start, DateTimeFormatter.ISO_OFFSET_DATE_TIME)),
           Some(ZonedDateTime.parse(stop, DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
  }

  implicit class ArlasDataFrame(df: DataFrame) extends WritableDataFrame(df) {

    def filterOnPeriod(period: Period): DataFrame = {
      df.transform(filterOnStart(period.start))
        .transform(filterOnStop(period.stop))
    }

    def filterOnStart(start: Option[ZonedDateTime])(df: DataFrame): DataFrame = {
      start match {
        case Some(start: ZonedDateTime) => {
          df.where(
            col(arlasPartitionColumn) >= Integer.valueOf(
              start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
              && col(arlasTimestampColumn) >= start.toEpochSecond)
        }
        case _ => df
      }
    }

    def filterOnStop(stop: Option[ZonedDateTime])(df: DataFrame): DataFrame = {
      stop match {
        case Some(stop: ZonedDateTime) => {
          df.where(
            col(arlasPartitionColumn) <= Integer.valueOf(
              stop.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
              && col(arlasTimestampColumn) <= stop.toEpochSecond)
        }
        case _ => df
      }
    }
  }

  sealed trait ColumnGroupingElement
  implicit class ImplicitColumnName(val v: String) extends ColumnGroupingElement
  implicit class ImplicitColumnObj(val c: Column) extends ColumnGroupingElement
  class ColumnGroup(elems: Tuple2[String, ColumnGroupingElement]*)
      extends mutable.HashMap[String, ColumnGroupingElement]
      with ColumnGroupingElement {
    this ++= elems
  }

  object ImplicitColumnName {
    def unapply(arg: ImplicitColumnName): Option[String] = Some(arg.v)
  }

  object ImplicitColumnObj {
    def unapply(arg: ImplicitColumnObj): Option[Column] = Some(arg.c)
  }

  object ColumnGroup {
    def apply(entries: (String, ColumnGroupingElement)*): ColumnGroup = new ColumnGroup(entries: _*)
  }

}
