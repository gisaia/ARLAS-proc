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

package io.arlas.data.sql

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.CassandraTool
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.spark.sql._

class WritableDataFrame(df: DataFrame) extends TransformableDataFrame(df) with CassandraTool {

  val PARQUET_BLOCK_SIZE: Int = 32 * 1024 * 1024
  val arlasElasticsearchIdColumn = "arlas_es_id"

  def writeToParquet(spark: SparkSession, target: String): Unit = {
    df.write
      .option("compression", "snappy")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE.toString)
      .mode(SaveMode.Append)
      .partitionBy(arlasPartitionColumn)
      .parquet(target)
  }

  def writeToElasticsearch(spark: SparkSession, dataModel: DataModel, target: String): Unit = {
    df.withColumn(arlasElasticsearchIdColumn,
                  concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
      .saveToEs(target, Map("es.mapping.id" -> arlasElasticsearchIdColumn))
  }

  def writeToScyllaDB(spark: SparkSession, dataModel: DataModel, target: String): Unit = {
    val targetKeyspace = target.split('.')(0)
    val targetTable = target.split('.')(1)

    createCassandraKeyspaceIfNotExists(spark, targetKeyspace)
    createCassandraTableIfNotExists(df, dataModel, targetKeyspace, targetTable)

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> targetKeyspace, "table" -> targetTable))
      .mode(SaveMode.Append)
      .save()
  }
}
