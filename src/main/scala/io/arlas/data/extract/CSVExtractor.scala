package io.arlas.data.extract

import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.utils.DataFrameHelper._
import io.arlas.data.extract.transformations._
import io.arlas.data.utils.BasicApp
import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVExtractor extends BasicApp {

  override def getName: String = "CSV Extractor"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {
    val PARQUET_BLOCK_SIZE: Int = 32 * 1024 * 1024

    val csvDf = spark.read
      .option("header", "true")
      .csv(runOptions.source)
      .transform(withValidColumnNames())
      .transform(withValidDynamicColumnsType(dataModel))
    //TODO try to support schema infering without time column issues

    csvDf
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))
      .write
      .option("compression", "snappy")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE.toString)
      .mode(SaveMode.Append)
      .partitionBy(arlasPartitionColumn)
      .parquet(runOptions.target)
  }
}