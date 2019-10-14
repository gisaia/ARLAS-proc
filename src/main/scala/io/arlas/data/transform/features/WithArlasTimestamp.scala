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

package io.arlas.data.transform.features

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{ZoneOffset, ZonedDateTime}
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class WithArlasTimestamp(dataModel: DataModel)
    extends ArlasTransformer(Vector(dataModel.timestampColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val timestampConversion = getUdf(dataModel.timeFormat)
    dataset.withColumn(arlasTimestampColumn, timestampConversion(col(dataModel.timestampColumn)))
  }

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

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema).add(StructField(arlasTimestampColumn, LongType, false))
  }
}
