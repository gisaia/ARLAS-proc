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

package io.arlas.data.sql

import io.arlas.data.extract.transformations.{
  withArlasPartition,
  withArlasTimestamp,
  withEmptyArlasSequenceId
}
import io.arlas.data.model.{DataModel, Period}
import io.arlas.data.transform.{ArlasTransformer, WithSequenceId, WithSequenceResampledTransformer}
import io.arlas.data.utils.DataFrameHelper.{withValidColumnNames, withValidDynamicColumnsType}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}

class TransformableDataFrame(df: DataFrame) {

  def asArlasBasicData(dataModel: DataModel): DataFrame = {
    df.transform(withValidColumnNames())
      .transform(withValidDynamicColumnsType(dataModel))
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))
  }

  def asArlasResampledData(spark: SparkSession,
                           dataModel: DataModel,
                           period: Option[Period]): DataFrame = {
    period match {
      case Some(p: Period) => {
        doPipelineTransform(
          df.transform(withEmptyArlasSequenceId(dataModel)),
          new WithSequenceId(dataModel),
          new WithSequenceResampledTransformer(dataModel, Some(p.start), spark)
        )
      }
      case _ => {
        doPipelineTransform(df.transform(withEmptyArlasSequenceId(dataModel)),
                            new WithSequenceId(dataModel),
                            new WithSequenceResampledTransformer(dataModel, None, spark))
      }
    }
  }

  def enrichWithArlas(transformers: ArlasTransformer*): DataFrame = {
    doPipelineTransform(df, transformers: _*)
  }

  def doPipelineTransform(df: DataFrame, transformers: ArlasTransformer*): DataFrame = {
    val pipeline = new Pipeline()
    pipeline.setStages(transformers.toArray)
    pipeline.fit(df).transform(df)
  }
}
