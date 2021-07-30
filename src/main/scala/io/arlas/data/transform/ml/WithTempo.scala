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

package io.arlas.data.transform.ml

import io.arlas.data.model.{MLModelLocal}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Identify emission tempo of observation from duration by using a HMM model
  * The emitting object usually record their location regularly in time.
  * The several modes of emission (ex: 10s, 2min, 30min) can be identify within the data thank to a pre-trained HMM model
  *
  * @param idColumn  Name of the column containing object identifier
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
