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

package io.arlas.data.load

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.BasicApp
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

object ESLoader extends BasicApp {
  override def getName: String = "Elasticsearch loader application"
  val arlasElasticsearchIdColumn = "arlas_es_id"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {

    val start = runOptions.start.getOrElse(ZonedDateTime.now(ZoneOffset.UTC).minusHours(1))
    val stop = runOptions.stop.getOrElse(ZonedDateTime.now(ZoneOffset.UTC))
    val startSeconds = start.toEpochSecond
    val stopSeconds = stop.toEpochSecond

    val source = runOptions.source.split(",")(0)

    var df: DataFrame = null
    if (source.contains("/")) {
      df = spark.read.parquet(source)
    } else {
      val ks = source.split('.')(0)
      val ta = source.split('.')(1)

      df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> ta, "keyspace" -> ks))
        .load()
    }

    df = df
      .withColumn(arlasElasticsearchIdColumn,
                  concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
      .where(
        col(arlasPartitionColumn) >= Integer.valueOf(
          start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
          && col(arlasPartitionColumn) <= Integer.valueOf(
            stop.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      .where(col(arlasTimestampColumn) >= startSeconds && col(arlasTimestampColumn) <= stopSeconds)

    df.saveToEs(runOptions.target, Map("es.mapping.id" -> arlasElasticsearchIdColumn))
  }
}
