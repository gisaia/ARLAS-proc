package io.arlas.data

import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Arlas Spark Test")
      .config("spark.driver.memory","512m")
      .getOrCreate()
  }
}
