package io.arlas.data.transform

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame

object transformations {

  val arlasSequenceIdColumn = "arlas_sequence_id"

  /**
    * Create a pipeline and apply within the Transforms to the dataframe
    * @param df
    * @param transformers
    * @return
    */
  def doPipelineTransform(df: DataFrame, transformers: ArlasTransformer*) = {
    val pipeline = new Pipeline()
    pipeline.setStages(transformers.toArray)
    pipeline.fit(df).transform(df)
  }
}
