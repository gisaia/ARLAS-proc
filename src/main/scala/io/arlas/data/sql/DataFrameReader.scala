package io.arlas.data.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameReader {

  def readFromCsv(spark: SparkSession, source: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(source)
  }

  def readFromParquet(spark: SparkSession, source: String) = {
    spark.read.parquet(source)
  }

  def readFromScyllaDB(spark: SparkSession, source: String): DataFrame = {
    val sourceKeyspace = source.split('.')(0)
    val sourceTable = source.split('.')(1)
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceTable, "keyspace" -> sourceKeyspace))
      .load()
  }
}
