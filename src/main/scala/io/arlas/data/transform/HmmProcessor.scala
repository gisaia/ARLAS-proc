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

import io.arlas.data.model.{DataModel, MLModel}
import org.apache.spark.sql.functions._
import io.arlas.ml.classification.Hmm
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.ml.parameter.State
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ArrayBuffer

class HmmProcessor(dataModel: DataModel,
                   spark                 : SparkSession,
                   sourceColumn          : String,
                   hmmModel              : MLModel,
                   partitionColumn       : String,
                   resultColumn          : String )
    extends ArlasTransformer(dataModel, Vector(partitionColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  val UNKNOWN_RESULT       = "Unknown"
  val SOURCE_DOUBLE_COLUMN = "_sourceDoubleColumn"
  val COLLECT_COLUMN       = "_collectColumn"
  val HMM_PREDICT_COLUMN   = "_hmmPredictColumn"
  val UNIQUE_ID_COLUMN     = "_uniqueIDColumn"

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema).add(StructField(resultColumn, StringType, true))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val columns = dataset.columns

    val hmmModelContent = hmmModel.getModelString()

    if (!dataset.columns.contains(sourceColumn)) {
      logger.error(s"Missing required column ${sourceColumn} to compute HMM")
      dataset.withColumn(resultColumn, lit(UNKNOWN_RESULT))

    } else if (hmmModelContent.isFailure) {
      logger.error(s"HMM model not found")
      dataset.withColumn(resultColumn, lit(UNKNOWN_RESULT))

    } else {
      interpolateRows(dataset, hmmModelContent.get)
    }
  }

  val hmmPredictUDF = udf((sourceValues: Seq[Double], hmmModelContent: String) => {
    Hmm.predictStatesSequence(sourceValues, hmmModelContent) match {
      case Success(statesSequence: Seq[State]) => statesSequence.map(_.state)
      case Failure(excp) =>
        logger.error("HMM failed", excp)
        sourceValues.map(_ => UNKNOWN_RESULT)
    }
  })

  def interpolateRows(dataset: Dataset[_], hmmModelContent: String) = {

    val columns = dataset.columns
    val hmmSchema = StructType(Seq(
      StructField(UNIQUE_ID_COLUMN, StringType),
      StructField(resultColumn, StringType)
    ))

    val window = Window.partitionBy(partitionColumn).orderBy(arlasTimestampColumn)

    val hmmDF = dataset
      .withColumn(SOURCE_DOUBLE_COLUMN, when(col(sourceColumn).isNull, lit(0.0)).otherwise(col(sourceColumn).cast(DoubleType)))
      .withColumn(COLLECT_COLUMN, collect_list(SOURCE_DOUBLE_COLUMN).over(window))//use window to ensure timestamp sorting
      .groupBy(partitionColumn)
      .agg(last(COLLECT_COLUMN).as(COLLECT_COLUMN))
    //at this point, if <COLLECT_COLUMN> is an id, rows are like "id1, Seq(0.1, 1.0, 2.0, 3.0)"
    //the second column is the ordered list of source values to use for prediction
      .withColumn(HMM_PREDICT_COLUMN, hmmPredictUDF(col(COLLECT_COLUMN), lit(hmmModelContent)))
    //now rows are like "id1, Seq(PREDICTION1, PREDICTION2, PREDICTION3, PREDICTION4)"
      .flatMap((r: Row) => {
        r.getAs[ArrayBuffer[String]](HMM_PREDICT_COLUMN).zipWithIndex.map {
                                                            case (result: String, index: Int) => {
                                                              Row.fromSeq(Seq(
                                                                r.getAs[String](partitionColumn) + "_" + (index + 1),
                                                                result
                                                              ))
                                                            }
                                                          }
      //finally we splitted the rows predictions into multiple rows, the first column being a kind of index
      //like UNIQUE_ID_COLUMN = <partitionColumn>_<row number for its partitionColumn>
      // e.g. "id1_1, PREDICTION1", "id1_2, PREDICTION2", "id1_3, PREDICTION4", "id1_1, PREDICTION4"

      })(RowEncoder(hmmSchema))

    //we also add the UNIQUE_ID_COLUMN to initial data and join on it
    dataset
      .withColumn(UNIQUE_ID_COLUMN, concat(col(partitionColumn), lit("_"), row_number().over(window)))
      .join(hmmDF, UNIQUE_ID_COLUMN)
      .drop(UNIQUE_ID_COLUMN)
  }
}
