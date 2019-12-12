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
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CassandraTool {

  def createCassandraKeyspaceIfNotExists(spark: SparkSession, keyspace: String): Unit = {
    CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}")
    }
  }

  def createCassandraTableIfNotExists(df: DataFrame, dataModel: DataModel, keyspace: String, table: String): Unit = {
    try {
      df.createCassandraTable(
        keyspace,
        table,
        partitionKeyColumns = Some(Seq(arlasPartitionColumn)),
        clusteringKeyColumns = Some(Seq(dataModel.idColumn, arlasTimestampColumn))
      )
    } catch {
      case aee: AlreadyExistsException => {
        //already existing
      }
    }
  }

  def isCassandraTableCreated(spark: SparkSession, keyspace: String, table: String): Boolean = {
    CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
      val ks = session.getCluster.getMetadata().getKeyspace(keyspace)
      if (ks != null) { ks.getTable(table) != null } else false
    }
  }
}
