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

import io.arlas.data.model.MLModelLocal
import io.arlas.data.transform.ArlasMovingStates
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Identify if fragments correspond to still or move state with a HMM model based on speed
  *
  * @param spark Spark Session
  * @param idColumn Column containing the id used to identify sequence
  * @param gapStateColumn Column containing the gap state of the fragment
  * @param speedColumn Column containing the speed used for HMM
  * @param targetMovingState Column to store the moving state
  * @param stillMoveModelPath Path of the still move hmm model
  * @param hmmWindowSize Max size of sequence for HMM
  */
class WithMovingState(spark: SparkSession,
                      idColumn: String,
                      gapStateColumn: String,
                      speedColumn: String,
                      targetMovingState: String,
                      stillMoveModelPath: String,
                      hmmWindowSize: Int = 5000)
    extends HmmProcessor(
      speedColumn,
      MLModelLocal(spark, stillMoveModelPath),
      idColumn,
      targetMovingState,
      hmmWindowSize
    ) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    super
      .transform(dataset)
      .withColumn(
        targetMovingState,
        when(
          col(gapStateColumn)
            .equalTo(ArlasMovingStates.GAP),
          lit(ArlasMovingStates.GAP)
        ).otherwise(col(targetMovingState))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    super
      .transformSchema(schema)
      .add(StructField(targetMovingState, ArrayType(StringType, true), false))
  }
}
