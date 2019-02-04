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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{JavaParams, ParamMap, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.model.DataModel
import io.arlas.data.transform.transformations.arlasSequenceIdColumn
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class WithSequenceIdTransformer(dataModel: DataModel) extends ArlasTransformer(
  dataModel,
  Vector(arlasTimestampColumn, arlasPartitionColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val window = Window.partitionBy(dataModel.idColumn).orderBy(arlasTimestampColumn)
    val gap = col(arlasTimestampColumn) - lag(arlasTimestampColumn,1).over(window)
    val sequenceId = when(col("gap").isNull || col("gap") > dataModel.sequenceGap, concat(col(dataModel.idColumn),lit("#"),col(arlasTimestampColumn)))

    dataset
      .withColumn("gap", gap)
      .withColumn("row_sequence_id", sequenceId)
      .withColumn( arlasSequenceIdColumn,
                   last("row_sequence_id", ignoreNulls = true).over(window.rowsBetween(Window.unboundedPreceding, 0)))
      .drop("row_sequence_id", "gap")
  }

  override def transformSchema(schema: StructType): StructType = {
    super.transformSchema(schema)

    schema.add(StructField(arlasSequenceIdColumn, LongType, false))
  }

}
