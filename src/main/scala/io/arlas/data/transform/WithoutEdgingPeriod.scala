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

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.extract.transformations._
import io.arlas.data.model.{DataModel, RunOptions}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class WithoutEdgingPeriod(dataModel: DataModel, runOptions: RunOptions, spark: SparkSession)
    extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn, arlasPartitionColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._

    val columns = dataset.columns
    val schema = dataset.schema

    val filterByWarming = dataset
      .toDF()
      .map(row =>
        (row.getString(row.fieldIndex(arlasSequenceIdColumn)), List(row.getValuesMap(columns))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
        case (_, sequence) => filterEdgingPeriod(dataModel, sequence, columns)
      }
      .map(entry => Row.fromSeq(columns.map(entry.getOrElse(_, null)).toSeq))

    spark.sqlContext.createDataFrame(filterByWarming, schema)

  }

  def filterEdgingPeriod(dataModel: DataModel,
                         rawSequence: List[Map[String, Any]],
                         columns: Array[String]): List[Map[String, Any]] = {

    val orderedRawSequence =
      rawSequence.sortBy(row => row.getOrElse(arlasTimestampColumn, 0l).asInstanceOf[Long])

    val sequenceStartDate = orderedRawSequence.head
      .getOrElse(arlasTimestampColumn, 0l)
      .asInstanceOf[Long]
    val sequenceEndDate = orderedRawSequence.last
      .getOrElse(arlasTimestampColumn, 0l)
      .asInstanceOf[Long]

    val sequenceWarmingPeriodFilter = ZonedDateTime
      .ofInstant(Instant.ofEpochSecond(sequenceStartDate), ZoneOffset.UTC)
      .plusSeconds(runOptions.warmingPeriod.getOrElse(0))
    val sequenceEndPeriodFilter = ZonedDateTime
      .ofInstant(Instant.ofEpochSecond(sequenceEndDate), ZoneOffset.UTC)
      .minusSeconds(runOptions.endingPeriod.getOrElse(0))

    orderedRawSequence
      .filter(
        x =>
          x.getOrElse(arlasTimestampColumn, 0l)
            .asInstanceOf[Long] >= sequenceWarmingPeriodFilter.toEpochSecond &&
            x.getOrElse(arlasTimestampColumn, 0l)
              .asInstanceOf[Long] <= sequenceEndPeriodFilter.toEpochSecond)
  }
}
