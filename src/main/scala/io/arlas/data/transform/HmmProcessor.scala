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
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import scala.util.{Failure, Success}
import io.arlas.ml.parameter.State
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ArrayBuffer

class HmmProcessor(dataModel: DataModel,
                   spark                 : SparkSession,
                   sourceColumn          : String,
                   hmmModel              : MLModel,
                   partitionColumn       : String,
                   resultColumn          : String)

  extends ArlasTransformer(dataModel, Vector(partitionColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  val UNKNOWN_RESULT                 = "Unknown"
  val SOURCE_DOUBLE_COLUMN           = "_sourceDoubleColumn"
  val COLLECT_COLUMN                 = "_collectColumn"
  val HMM_PREDICT_COLUMN             = "_hmmPredictColumn"
  val UNIQUE_ID_COLUMN               = "_uniqueIDColumn"
  val WINDOW_ID_COLUMN               = "_windowIdColumn"
  val ROW_NUMBER_ON_PARTITION_COLUMN = "_rowNumberOnPartitionColumn"

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

    val hmmSchema = StructType(Seq(
      StructField(UNIQUE_ID_COLUMN, StringType),
      StructField(resultColumn, StringType)
      ))

    val keyWindow = Window.partitionBy(WINDOW_ID_COLUMN).orderBy(arlasTimestampColumn)
    val partitionWindow = Window.partitionBy(partitionColumn).orderBy(arlasTimestampColumn)

    //eg. if windowSize is 2 and you have 3 rows with partitionColumn="id", then rows are like
    //row1: ROW_NUMBER_ON_PARTITION_COLUMN = 1, KEY_COLUMN = id1_1
    //row2: ROW_NUMBER_ON_PARTITION_COLUMN = 2, KEY_COLUMN = id1_1
    //row3: ROW_NUMBER_ON_PARTITION_COLUMN = 3, KEY_COLUMN = id1_2
    val initDF = dataset
      .withColumn(ROW_NUMBER_ON_PARTITION_COLUMN, row_number().over(partitionWindow))
      .withColumn(WINDOW_ID_COLUMN, concat(col(partitionColumn), lit("_"), floor(col(ROW_NUMBER_ON_PARTITION_COLUMN) / dataModel.hmmWindowSize)))

    //temporary DF with 2 fields: a unique id (= <partitionColumn>_<index>) and the prediction
    val hmmDF = initDF
      .withColumn(SOURCE_DOUBLE_COLUMN, when(col(sourceColumn).isNull, lit(0.0)).otherwise(col(sourceColumn).cast(DoubleType)))
      .withColumn(COLLECT_COLUMN, collect_list(SOURCE_DOUBLE_COLUMN).over(keyWindow))//use window to ensure timestamp sorting
      .groupBy(WINDOW_ID_COLUMN)
      .agg(last(COLLECT_COLUMN).as(COLLECT_COLUMN))
      //at this point, rows are like "id1_1, Seq(0.1, 1.0)", "id1_2, Seq(2.0)"
      //the second column is the ordered list of source values to use for prediction, for the window
      .withColumn(HMM_PREDICT_COLUMN, hmmPredictUDF(col(COLLECT_COLUMN), lit(hmmModelContent)))
      //now rows are like "id1_1, Seq(PREDICTION1, PREDICTION2)", "id1_2, Seq(PREDICTION3)"
      .flatMap((r: Row) => {
        r.getAs[ArrayBuffer[String]](HMM_PREDICT_COLUMN).zipWithIndex.map {
                                                                            case (result: String, index: Int) => {
                                                                              Row.fromSeq(Seq(
                                                                                r.getAs[String](WINDOW_ID_COLUMN) + "_" + (index + 1),
                                                                                result
                                                                                ))
                                                                            }
                                                                          }
      //finally we splitted the rows predictions into multiple rows, the first column being a kind of index
      //like UNIQUE_ID_COLUMN = <window_id>_<row number for its dinow>
      // e.g. "id1_1_1, PREDICTION1", "id1_1_2, PREDICTION2", "id1_2_1, PREDICTION3"
    })(RowEncoder(hmmSchema))

    //we also add the UNIQUE_ID_COLUMN to initial data and join on it
    initDF
      .withColumn(UNIQUE_ID_COLUMN, concat(col(WINDOW_ID_COLUMN), lit("_"), row_number().over(keyWindow)))
      .join(hmmDF, UNIQUE_ID_COLUMN)
      .drop(UNIQUE_ID_COLUMN, ROW_NUMBER_ON_PARTITION_COLUMN, WINDOW_ID_COLUMN)
  }
}
