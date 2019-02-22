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

package io.arlas.data.extract

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import io.arlas.data.model.DataModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType}

object transformations {

  val arlasTimestampColumn = "arlas_timestamp"
  val arlasPartitionColumn = "arlas_partition"

  def getUdf(timeFormat: String): UserDefinedFunction = udf { date: String =>
    val timeFormatter = DateTimeFormatter.ofPattern(timeFormat)
    date match {
      case null => None
      case d => {
        try Some(ZonedDateTime.parse(date, timeFormatter).toEpochSecond)
        catch {
          case dtpe: DateTimeParseException => {
            try Some(
              ZonedDateTime
                .parse(date, timeFormatter.withZone(ZoneOffset.UTC))
                .toEpochSecond)
            catch {
              case _: Exception => None
            }
          }
          case _: Exception => None
        }
      }
    }
  }

  def withArlasTimestamp(dataModel: DataModel)(df: DataFrame): DataFrame = {

    val timestampConversion = getUdf(dataModel.timeFormat)
    df.withColumn(arlasTimestampColumn,
                  timestampConversion(col(dataModel.timestampColumn)))
  }

  def withArlasPartition(dataModel: DataModel)(df: DataFrame): DataFrame = {
    df.withColumn(
      arlasPartitionColumn,
      date_format(to_date(col(dataModel.timestampColumn), dataModel.timeFormat),
                  "yyyyMMdd").cast(IntegerType))
  }

}
