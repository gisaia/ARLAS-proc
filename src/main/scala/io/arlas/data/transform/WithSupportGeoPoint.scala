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

import org.apache.spark.sql.functions._
import io.arlas.data.model.DataModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{BooleanType, DoubleType, StructField, StructType}

class WithSupportGeoPoint(dataModel: DataModel, spark: SparkSession, realDistanceCol: String, supportPointLength: Int,
                          colsToPropagate: Seq[String], windowSize: Int = 10, meanSpeedMultiplier: Double = 1.0)
  extends ArlasTransformer(dataModel, Vector(arlasTimestampColumn, arlasDeltaTimestampColumn, arlasVisibilityStateColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._
    val datasetWithKeep = dataset.withColumn("keep", lit(true))
    val columns = datasetWithKeep.columns
    val encoder = RowEncoder(datasetWithKeep.schema)
    datasetWithKeep.toDF()
      .withColumn("_cast_distance", col(realDistanceCol).cast(DoubleType))
      .flatMap((row: Row) => {
        var rows = Seq(row)

        val deltaTs = row.getAs[Long](arlasDeltaTimestampColumn)

       if (deltaTs != null) {
         val nbPeriods = deltaTs / supportPointLength
         val halfWindowSize = math.min(nbPeriods, windowSize) / 2
         if (halfWindowSize > 0) {

           val realDistance = row.getAs[Double]("_cast_distance")
           val meanSpeed = 1.0 * realDistance * meanSpeedMultiplier / deltaTs

           val currentTs = row.getAs[Long](arlasTimestampColumn)
           val previousTs = currentTs - deltaTs
           val shiftedCurrentTs = currentTs - 1
           val shiftedPreviousTs = previousTs + 1

           val leftWindow = Vector.range(shiftedPreviousTs, shiftedPreviousTs + halfWindowSize * supportPointLength, supportPointLength)
           val rightWindow = Vector.range(shiftedCurrentTs, shiftedCurrentTs - halfWindowSize * supportPointLength, -supportPointLength).reverse
           val window = leftWindow ++ rightWindow

           rows ++= window.zipWithIndex.map {
                                              case (ts, index) => {
                                                val values = columns.foldLeft(row.toSeq)((seq, col) => {

                                                  val speedColumn = dataModel.speedColumn

                                                  seq.updated(row.fieldIndex(col), col match {
                                                    case `speedColumn` => meanSpeed
                                                    case `arlasTimestampColumn` => ts
                                                    case "keep" => index == 0 || index == window.size - 1
                                                    case `arlasVisibilityStateColumn` => ArlasVisibilityStates.INVISIBLE.toString
                                                    case c if (!colsToPropagate.contains(c)) => null
                                                    case _ => row.get(row.fieldIndex(col))
                                                  })
                                                })
                                                Row.fromSeq(values)
                                              }
                                            }
         }
       }
        rows
      })(encoder)
      .drop("_cast_distance")
  }

  override def transformSchema(schema: StructType): StructType = {
    super.transformSchema(schema).add(StructField("keep", BooleanType))
  }
}
