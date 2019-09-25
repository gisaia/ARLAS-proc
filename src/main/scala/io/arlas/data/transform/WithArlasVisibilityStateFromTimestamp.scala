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

import io.arlas.data.sql._
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/*
 * Split groups into sequences separated by gaps larger than visibilityTimeout.
 * Add arlas_visibility_state column (available values in ArlasVisibilityStates)
 */
@Deprecated
//TODO remove as soon as tests don't rely on it anymore
class WithArlasVisibilityStateFromTimestamp(dataModel: DataModel, visibilityTimeout: Int)
    extends ArlasTransformer(Vector(arlasTimestampColumn, arlasPartitionColumn, dataModel.idColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val datasetDF = dataset.toDF()
    val df =
      if (!datasetDF.columns.contains(arlasVisibilityStateColumn))
        datasetDF.withEmptyCol(arlasVisibilityStateColumn)
      else datasetDF

    // prerequisites visibility timeout computations
    val window = Window.partitionBy(dataModel.idColumn).orderBy(arlasTimestampColumn)
    val previousGap = col(arlasTimestampColumn) - lag(arlasTimestampColumn, 1).over(window)
    val nextGap = lead(arlasTimestampColumn, 1).over(window) - col(arlasTimestampColumn)

    // sequence information computation
    val sequenceId = when(col("previousGap").isNull || col("previousGap") > visibilityTimeout,
                          concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
    val visibilityState =
      when(col("previousGap").isNull || col("previousGap") > visibilityTimeout,
           ArlasVisibilityStates.APPEAR.toString)
        .when(col("nextGap").isNull || col("nextGap") > visibilityTimeout,
              ArlasVisibilityStates.DISAPPEAR.toString)
        .otherwise(ArlasVisibilityStates.VISIBLE.toString)

    // dataframe enrichment
    df.withColumn("previousGap", previousGap)
      .withColumn("nextGap", nextGap)
      .withColumn(
        arlasVisibilityStateColumn,
        when(
          // state initialisation
          (col(arlasVisibilityStateColumn).isNull ||
            //overwrite VISIBLE when should be DISAPPEAR
            ((col("nextGap").isNotNull) || col("previousGap").isNotNull)),
          visibilityState
        ).otherwise(col(arlasVisibilityStateColumn))
      )
      .drop("previousGap", "nextGap")
  }

  override def transformSchema(schema: StructType): StructType = {
    if (!schema.fieldNames.contains(arlasVisibilityStateColumn))
      schema.add(StructField(arlasVisibilityStateColumn, StringType, true))
    else schema
  }

}
