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
      session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}", keySpace))
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
    }
    catch {
      case aee: AlreadyExistsException => {
        print("Already existed table")
      }
    }
  }
}
