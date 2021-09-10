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

package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Tag fragments of a same aggregation group with same arlasTrackSampleId
  * so that their durations' sum approximates sampling duration when possible.
  *
  * @param dataModel the input Datamodel
  * @param aggregationColumnName aggregation column to group fragments
  * @param sampling target sampling duration for fragments
  */
class WithFragmentSampleId(dataModel: DataModel, aggregationColumnName: String, sampling: Long)
    extends ArlasTransformer(Vector(arlasTrackTimestampCenter, arlasTrackDuration, aggregationColumnName)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window
      .partitionBy(aggregationColumnName)
      .orderBy(arlasTrackTimestampCenter)

    dataset.withColumn(
      arlasTrackSampleId,
      concat(
        col(aggregationColumnName),
        lit("_"),
        // sample index = duration sum of preceding and current rows / sampling (common case)
        //                - current row's duration / sampling (to fix fragments with duration > sampling case)
        floor((sum(arlasTrackDuration).over(window) - lit(1)) / lit(sampling))
          - floor((col(arlasTrackDuration) - lit(1)) / lit(sampling))
      )
    )

  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasTrackSampleId, StringType, true))
  }
}
