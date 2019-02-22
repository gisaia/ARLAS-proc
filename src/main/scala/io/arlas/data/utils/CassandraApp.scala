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

package io.arlas.data.utils

import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.model.{DataModel, RunOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CassandraApp {
  var keySpace: String = "ks"
  var resultTable: String = "result_table"

  def createCassandraKeyspace(spark: SparkSession, runOptions: RunOptions): Unit = {
    keySpace = runOptions.target.split('.')(0)
    resultTable = runOptions.target.split('.')(1)

    val connector = CassandraConnector.apply(spark.sparkContext.getConf)
    val session = connector.openSession
    try {
      session.execute(
        String.format(
          "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}",
          keySpace))
    } finally if (session != null) session.close()
  }

  def createCassandraTable(df: DataFrame, dataModel: DataModel): Unit = {
    try {
      df.createCassandraTable(
        keySpace,
        resultTable,
        partitionKeyColumns = Some(Seq(arlasPartitionColumn)),
        clusteringKeyColumns = Some(Seq(dataModel.idColumn, arlasTimestampColumn))
      )
    } catch {
      case aee: AlreadyExistsException => {
        print("Already existed table")
      }
    }
  }
}
