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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/*
 * Group data by id and split groups into sequences separated by gaps larger than visibilityTimeout.
 *
 * Add 2 columns to enrich geo points information :
 * - arlas_visibility_state = APPEAR|VISIBLE|DISAPPEAR
 *     + APPEAR = first point of a visible sequence
 *     + DISAPPEAR = last point of a visible sequence
 *     + VISIBLE = other points of the sequence
 * - arlas_visible_sequence_id = id#timestampOfTheFirstPointOfTheSequence
 */
class WithArlasVisibleSequence(dataModel: DataModel)
    extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn, arlasPartitionColumn)) {

  val newColumns = List(arlasVisibleSequenceIdColumn, arlasVisibilityStateColumn)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = newColumns.foldLeft(dataset.toDF) { (newDF, columnName) =>
      {
        if (!newDF.columns.contains(columnName))
          newDF.transform(withEmptyNullableStringColumn(columnName))
        else newDF
      }
    }

    // prequisites visibility timeout computations
    val window = Window.partitionBy(dataModel.idColumn).orderBy(arlasTimestampColumn)
    val previousGap = col(arlasTimestampColumn) - lag(arlasTimestampColumn, 1).over(window)
    val nextGap = lead(arlasTimestampColumn, 1).over(window) - col(arlasTimestampColumn)

    // sequence information computation
    val sequenceId = when(
      col("previousGap").isNull || col("previousGap") > dataModel.visibilityTimeout,
      concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
    val visibilityState =
      when(col("previousGap").isNull || col("previousGap") > dataModel.visibilityTimeout, "APPEAR")
        .when(col("nextGap").isNull || col("nextGap") > dataModel.visibilityTimeout, "DISAPPEAR")
        .otherwise("VISIBLE")

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
      .withColumn("row_sequence_id",
                  when(col(arlasVisibleSequenceIdColumn).isNull, sequenceId)
                    .otherwise(col(arlasVisibleSequenceIdColumn)))
      .withColumn(arlasVisibleSequenceIdColumn,
                  last("row_sequence_id", ignoreNulls = true)
                    .over(window.rowsBetween(Window.unboundedPreceding, 0)))
      .drop("row_sequence_id", "previousGap", "nextGap")
  }

  override def transformSchema(schema: StructType): StructType = {
    newColumns.foldLeft(checkSchema(schema)) { (newSchema, columnName) =>
      {
        if (!newSchema.fieldNames.contains(columnName))
          newSchema.add(StructField(columnName, StringType, true))
        else newSchema
      }
    }
  }

}

object WithArlasVisibleSequence {

  def withEmptyVisibileSequenceId()(df: DataFrame): DataFrame = {
    df.withColumn(arlasVisibleSequenceIdColumn, lit(null).cast(StringType))
  }

  def withEmptyVisibilityState()(df: DataFrame): DataFrame = {
    df.withColumn(arlasVisibilityStateColumn, lit(null).cast(StringType))
  }
}
