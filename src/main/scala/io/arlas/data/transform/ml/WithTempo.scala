package io.arlas.data.transform.ml

import io.arlas.data.model.{DataModel, MLModelLocal}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Identify tempo from duration by using a HMM model
  *
  * @param dataModel     Data model containing names of structuring columns (id, lat, lon, time)
  * @param durationColumn Name of the column containing the fragment duration (s)
  * @param targetTempoColumn Name of the column to store the detected emission tempo
  * @param spark Spark Session
  * @param tempoModelPath Path to the HMM model used to identify tempo
  * @param hmmWindowSize Size of the max length of hmm sequence
  * @param tempoIrregular Path to the HMM model used to identify tempo
  */
class WithTempo(dataModel: DataModel,
                durationColumn: String,
                targetTempoColumn: String,
                spark: SparkSession,
                tempoModelPath: String,
                hmmWindowSize: Int = 5000,
                tempoIrregular: String = "tempo_irregular")
    extends HmmProcessor(
      durationColumn,
      MLModelLocal(spark, tempoModelPath),
      dataModel.idColumn,
      targetTempoColumn,
      hmmWindowSize
    ) {
  override def transform(dataset: Dataset[_]): DataFrame = {
    super
      .transform(dataset)
      // Fill missing prediction with tempoIrregular values
      .withColumn(targetTempoColumn,
                  when(col(targetTempoColumn).equalTo(null), lit(tempoIrregular))
                    .otherwise(col(targetTempoColumn)))
  }
}
