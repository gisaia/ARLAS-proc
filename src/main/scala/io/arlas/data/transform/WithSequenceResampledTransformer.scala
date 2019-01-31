package io.arlas.data.transform

import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.math.interpolations.splineInterpolateAndResample
import io.arlas.data.model.DataModel
import io.arlas.data.transform.transformations.arlasSequenceIdColumn
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class WithSequenceResampledTransformer(dataModel: DataModel, spark: SparkSession) extends ArlasTransformer(
  dataModel,
  Vector(arlasTimestampColumn, arlasPartitionColumn, arlasSequenceIdColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._

    val columns = dataset.columns
    val schema = dataset.schema

    //added toDf()
    val interpolatedRows = dataset.toDF().map(row => (row.getString(row.fieldIndex(arlasSequenceIdColumn)),List(row.getValuesMap(columns)))).rdd
      .reduceByKey(_ ++ _)
      .flatMap{ case(_, sequence) => splineInterpolateAndResample(dataModel, sequence, columns) }
      .map(entry => Row.fromSeq(columns.map(entry.getOrElse(_,null)).toSeq))

    spark.sqlContext.createDataFrame(interpolatedRows, schema)
  }

}
