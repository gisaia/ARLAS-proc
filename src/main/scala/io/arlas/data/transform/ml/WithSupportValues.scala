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

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Duplicate values by adding "support point" when the fragment duration is high
  * It allows to take add weight to these values when using hmm classifier
  * @param supportColumn  Name of the column containing values used as input for hmm model
  * @param targetSupportedColumn Name of the new column containing supported values
  * @param supportValueDeltaTime Duration (s) between two created "support points"
  * @param supportValueMaxNumberInGap Maximum number of "support points" (duplicated value) created within a fragment
  * @param durationColumn Name of the column containing fragment duration
  */
class WithSupportValues(supportColumn: String,
                        targetSupportedColumn: String,
                        supportValueDeltaTime: Int = 120,
                        supportValueMaxNumberInGap: Int = 10,
                        durationColumn: String)
    extends ArlasTransformer(Vector(supportColumn, durationColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val schema =
      dataset.schema.add(StructField(targetSupportedColumn, ArrayType(DoubleType, true), false))

    val encoder = RowEncoder(schema)
    dataset
      .toDF()
      .map((row: Row) => {

        val value = row.getAs[Double](supportColumn)
        val gapDuration = row.getAs[Long](durationColumn)
        if (gapDuration > supportValueDeltaTime * supportValueMaxNumberInGap) {
          val nbValues =
            scala.math.min(supportValueMaxNumberInGap, (gapDuration / supportValueDeltaTime).toInt)
          Row.fromSeq(row.toSeq :+ Seq.fill(nbValues)(value))
        } else {
          Row.fromSeq(row.toSeq :+ Seq(value))
        }
      })(encoder)
  }

  override def transformSchema(schema: StructType): StructType = {
    super
      .transformSchema(schema)
      .add(StructField(targetSupportedColumn, ArrayType(DoubleType, true), false))
  }
}
