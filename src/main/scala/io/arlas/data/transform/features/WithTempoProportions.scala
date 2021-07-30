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

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Initialize tempo proportion of the fragment (1 for predicted, 0 for others) -> Unique tempo
  *
  * @param tempoColumn Name of the predicted tempo column
  * @param targetIsMultiColumn Unit of the computed speed
  * @param proportionColumnMap Map linking tempo proportion column name to tempo values.
  *                            Ex: Map(tempoIrregularProportion -> tempoIrregular)
  */
class WithTempoProportions(tempoColumn: String, targetIsMultiColumn: String, proportionColumnMap: Map[String, String])
    extends ArlasTransformer(Vector(tempoColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    proportionColumnMap
      .foldLeft(
        dataset
          .toDF()) {
        case (df: DataFrame, cols: (String, String)) =>
          df.withColumn(cols._1,
                        when(col(tempoColumn).equalTo(cols._2), lit(1.0))
                          .otherwise(lit(0.0)))
      }
      .withColumn(targetIsMultiColumn, lit(false))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(targetIsMultiColumn, BooleanType, false))
    // TODO: Add proportions to Schema from map
  }
}
