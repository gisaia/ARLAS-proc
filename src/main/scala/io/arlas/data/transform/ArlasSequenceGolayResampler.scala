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
package io.arlas.data.transform

import java.time.{ZoneOffset, ZonedDateTime}
import io.arlas.data.model.DataModel
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.math.interpolations.golayInterpolateAndResample

class ArlasSequenceGolayResampler(dataModel: DataModel,
                                  spark: SparkSession,
                                  start: Option[ZonedDateTime],
                                  partitionColumn: String
                                  )
  extends ArlasTransformer(
    dataModel,
    Vector(arlasTimestampColumn, partitionColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._

    val columns = dataset.columns
    val minSequenceStopEpochSeconds =
      start.getOrElse(ZonedDateTime.now(ZoneOffset.UTC).minusHours(1)).toEpochSecond
    val outputColumns = dataset.columns :+ ("d" + dataModel.latColumn) :+ ("dd" + dataModel.latColumn) :+
                        ("d" + dataModel.lonColumn) :+ ("dd" + dataModel.lonColumn)

    val schema = dataset.schema
      .add("d" + dataModel.latColumn, DoubleType)
      .add("dd" + dataModel.latColumn, DoubleType)
      .add("d" + dataModel.lonColumn, DoubleType)
      .add("dd" + dataModel.lonColumn, DoubleType)

    //added toDf()
    val interpolatedRows = dataset
      .toDF()
      .map(row =>
             (row.getString(row.fieldIndex(partitionColumn)), List(row.getValuesMap(columns))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
                 case (_, sequence) =>
                   golayInterpolateAndResample(dataModel, sequence, columns, minSequenceStopEpochSeconds)
               }
      .map(entry => Row.fromSeq(outputColumns.map(entry.getOrElse(_, null)).toSeq))

    spark.sqlContext
      .createDataFrame(interpolatedRows, schema)
  }

}
