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
import io.arlas.data.transform.VisibilityChange._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class WithFragmentVisibilityFromTempo(dataModel: DataModel, spark: SparkSession, irregularTempo: String)
    extends ArlasTransformer(Vector(arlasTimestampColumn, arlasTempoColumn, dataModel.idColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window.partitionBy(dataModel.idColumn).orderBy(arlasTimestampColumn)

    val previousVisibilityProportion = lag(arlasTrackVisibilityProportion, 1).over(window)
    val nextVisibilityProportion = lead(arlasTrackVisibilityProportion, 1).over(window)

    dataset
    // irregular tempo => visibility proportion = 0
      .withColumn(arlasTrackVisibilityProportion, when(col(arlasTempoColumn).equalTo(irregularTempo), lit(0.0d)).otherwise(lit(1.0d)))

      /*
       * APPEAR = first visible fragment after an invisible fragment
       * DISAPPEAR = last visible fragment before an invisible fragment
       * APPEAR_DISAPPEAR = visible fragment between 2 invisible fragments
       * null = other fragments
       */
      .withColumn(
        arlasTrackVisibilityChange,
        when(col(arlasTrackVisibilityProportion).equalTo(0), null)
          .otherwise(
            when(previousVisibilityProportion.equalTo(0) && nextVisibilityProportion.equalTo(0), APPEAR_DISAPPEAR)
              .when(previousVisibilityProportion.equalTo(0), APPEAR)
              .when(nextVisibilityProportion.equalTo(0), DISAPPEAR)
              .otherwise(null))
      )
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasTrackVisibilityProportion, DoubleType, false))
      .add(StructField(arlasTrackVisibilityChange, StringType, true))
  }

}
