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

package io.arlas.data.transform

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import io.arlas.data.sql._

class WithArlasVisibilityStateFromTempo(dataModel: DataModel,
                                        spark: SparkSession,
                                        appearTempo: String)
  extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = if (!dataset.columns.contains(arlasVisibilityStateColumn))
      dataset.toDF().withEmptyCol(arlasVisibilityStateColumn)
    else dataset.toDF()

    val window = Window.partitionBy(dataModel.idColumn).orderBy(arlasTimestampColumn)
    val nextTempo = lead(arlasTempoColumn, 1).over(window)
    df.withColumn(
      arlasVisibilityStateColumn,
      when(col(arlasTempoColumn).equalTo(appearTempo), lit(ArlasVisibilityStates.APPEAR.toString))
        .otherwise(
          when(nextTempo.equalTo(appearTempo).and(col(arlasTempoColumn).notEqual(appearTempo)), ArlasVisibilityStates.DISAPPEAR.toString)
            .otherwise(lit(ArlasVisibilityStates.VISIBLE.toString))))
  }

  override def transformSchema(schema: StructType): StructType = {

    Some(checkSchema(schema)).map(sch => {
      if (!sch.fieldNames.contains(arlasVisibilityStateColumn)) {
        sch.add(StructField(arlasVisibilityStateColumn, StringType, true))
      } else sch
    }).get
  }

}
