package io.arlas.data.sql

import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.load.ESLoader.arlasElasticsearchIdColumn
import io.arlas.data.model.DataModel
import io.arlas.data.utils.CassandraApp
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.spark.sql._

class WritableDataFrame(df: DataFrame) extends TransformableDataFrame(df) with CassandraApp {

  val PARQUET_BLOCK_SIZE: Int = 32 * 1024 * 1024

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
