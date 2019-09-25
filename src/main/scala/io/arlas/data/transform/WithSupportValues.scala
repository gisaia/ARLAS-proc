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
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class WithSupportValues(
    dataModel: DataModel,
    spark: SparkSession,
    supportColumn: String,
    supportValueDeltaTime: Int,
    supportValueMaxNumberInGap: Int,
    irregularTempo: String
) extends ArlasTransformer(
      Vector(arlasTrackDuration, dataModel.distanceColumn, supportColumn, arlasTempoColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val schema =
      dataset.schema.add(StructField(supportColumn + "_array", ArrayType(DoubleType, true), false))

    val encoder = RowEncoder(schema)
    dataset
      .toDF()
      .map((row: Row) => {

        val value = row.getAs[Double](supportColumn)
        val tempo = row.getAs[String](arlasTempoColumn)

        if (tempo.equals(irregularTempo)) {
          val gapDuration = row.getAs[Long](arlasTrackDuration)
          val gapDistance = row.getAs[Double](dataModel.distanceColumn)
          val gapSpeed = gapDistance / gapDuration
          val nbValues =
            scala.math.min(supportValueMaxNumberInGap, (gapDuration / supportValueDeltaTime).toInt)
          Row.fromSeq(row.toSeq :+ Seq.fill(nbValues)(gapSpeed))
        } else {
          Row.fromSeq(row.toSeq :+ Seq(value))
        }
      })(encoder)
  }

  override def transformSchema(schema: StructType): StructType = {
    super
      .transformSchema(schema)
      .add(StructField(supportColumn + "_array", ArrayType(DoubleType, true), false))
  }
}
