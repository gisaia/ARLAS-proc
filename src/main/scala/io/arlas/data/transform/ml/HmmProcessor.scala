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

import io.arlas.data.model.MLModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import io.arlas.ml.classification.Hmm
import io.arlas.ml.parameter.State
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

/**
  * Apply an HMM (Hidden Markov Model) to predict states from the sequence of a variable input
  *
  * @param sourceColumn Column containing values used as input of the HMM
  * @param hmmModel MLModel object containing the hmm model (state observation and transition probabilities)
  * @param partitionColumn Column used to group data to create the timeseries (ex: objectId)
  * @param resultColumn Column to store the hmm predicted states
  * @param hmmWindowSize Max size of the hmm time series (split if data are longer than the threshold)
  */
class HmmProcessor(sourceColumn: String, hmmModel: MLModel, partitionColumn: String, resultColumn: String, hmmWindowSize: Int = 5000)
    extends ArlasTransformer(Vector(partitionColumn, arlasTimestampColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  val UNKNOWN_RESULT = "Unknown"
  val SOURCE_DOUBLE_COLUMN = "_sourceDoubleColumn"
  val COLLECT_COLUMN = "_collectColumn"
  val HMM_PREDICT_COLUMN = "_hmmPredictColumn"
  val UNIQUE_ID_COLUMN = "_uniqueIDColumn"
  val WINDOW_ID_COLUMN = "_windowIdColumn"
  val ROW_NUMBER_ON_PARTITION_COLUMN = "_rowNumberOnPartitionColumn"

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema).add(StructField(resultColumn, StringType, true))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val hmmModelContent = hmmModel.getModelString()

    if (!dataset.columns.contains(sourceColumn)) {
      throw new Exception(s"Missing required column ${sourceColumn} to compute HMM")

    } else if (hmmModelContent.isFailure) {
      throw new Exception(s"HMM model not found: " + hmmModelContent.failed.get.getMessage)

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

    val hmmSchema = StructType(
      Seq(
        StructField(UNIQUE_ID_COLUMN, StringType),
        StructField(resultColumn, StringType)
      ))

    val keyWindow = Window.partitionBy(WINDOW_ID_COLUMN).orderBy(arlasTimestampColumn)
    val partitionWindow = Window.partitionBy(partitionColumn).orderBy(arlasTimestampColumn)

    //eg. if windowSize is 2 and you have 3 rows with partitionColumn="id", then rows are like
    //row1: ROW_NUMBER_ON_PARTITION_COLUMN = 1, KEY_COLUMN = id1_1
    //row2: ROW_NUMBER_ON_PARTITION_COLUMN = 2, KEY_COLUMN = id1_1
    //row3: ROW_NUMBER_ON_PARTITION_COLUMN = 3, KEY_COLUMN = id1_2
    val windowedPartitionedDF = dataset
      .withColumn(ROW_NUMBER_ON_PARTITION_COLUMN, row_number().over(partitionWindow))
      .withColumn(WINDOW_ID_COLUMN, concat(col(partitionColumn), lit("_"), floor(col(ROW_NUMBER_ON_PARTITION_COLUMN) / hmmWindowSize)))
      .withColumn(UNIQUE_ID_COLUMN, concat(col(WINDOW_ID_COLUMN), lit("_"), row_number().over(keyWindow)))

    //cast (and eventually explode) source column to DoubleType
    val sourceDataType =
      windowedPartitionedDF.schema.fields.filter(_.name.equals(sourceColumn)).head.dataType
    val formatedSourceDF =
      if (sourceDataType.equals(ArrayType(DoubleType, true))
          || sourceDataType.equals(ArrayType(DoubleType, false)))
        //create one row per values in sourceColumn
        //these rows will have the same UNIQUE_ID_COLUMN
        windowedPartitionedDF.withColumn(SOURCE_DOUBLE_COLUMN, explode(col(sourceColumn)))
      else
        windowedPartitionedDF.withColumn(SOURCE_DOUBLE_COLUMN,
                                         when(col(sourceColumn).isNull, lit(0.0d))
                                           .otherwise(col(sourceColumn).cast(DoubleType)))

    //temporary DF with 2 fields: a unique id (= <partitionColumn>_<index>) and the prediction
    val window = Window.partitionBy(UNIQUE_ID_COLUMN).orderBy(desc("count"))
    val hmmDF = formatedSourceDF
      .withColumn(COLLECT_COLUMN, collect_list(SOURCE_DOUBLE_COLUMN).over(keyWindow)) //use window to ensure timestamp sorting
      .withColumn(UNIQUE_ID_COLUMN, collect_list(UNIQUE_ID_COLUMN).over(keyWindow)) //use window to ensure timestamp sorting
      .groupBy(WINDOW_ID_COLUMN)
      .agg(last(COLLECT_COLUMN).as(COLLECT_COLUMN), last(UNIQUE_ID_COLUMN).as(UNIQUE_ID_COLUMN))
      //at this point, rows are like "id1_1, Seq(0.1, 1.0), Seq(id1_1_1,id1_1_2)", "id1_2, Seq(2.0), Seq(id1_2_1)"
      //the second and the third column is the ordered list of source values and corresponding unique IDs
      //to use for prediction, for the window
      .withColumn(HMM_PREDICT_COLUMN, hmmPredictUDF(col(COLLECT_COLUMN), lit(hmmModelContent)))
      //now rows are like "id1_1, Seq(PREDICTION1, PREDICTION2), Seq(id1_1_1,id1_1_2)", "id1_2, Seq(PREDICTION3), Seq(id1_2_1)"
      .flatMap((r: Row) => {
        r.getAs[ArrayBuffer[String]](HMM_PREDICT_COLUMN)
          .zip(r.getAs[ArrayBuffer[String]](UNIQUE_ID_COLUMN))
          .map {
            case (result: String, uniqueId: String) => {
              Row.fromSeq(
                Seq(
                  uniqueId,
                  result
                ))
            }
          }
        //finally we gather UNIQUE_ID_COLUMN with predictions
        // e.g. "id1_1_1, PREDICTION1", "id1_1_2, PREDICTION2", "id1_2_1, PREDICTION3"
        //if sourceColumn is a Seq of values, there are several predictions for a given UNIQUE_ID_COLUMN
      })(RowEncoder(hmmSchema))
      //gather predictions by UNIQUE_ID_COLUMN and select the best prediction (with the most occurrences)
      .groupBy(UNIQUE_ID_COLUMN, resultColumn)
      .count()
      .withColumn("order", row_number().over(window))
      .where(col("order").equalTo(lit(1)))
      .drop("order", "count")

    //we also add the UNIQUE_ID_COLUMN to initial data and join on it
    windowedPartitionedDF
      .join(hmmDF, Seq(UNIQUE_ID_COLUMN), "left") // Sometimes not all the data coincide, use left avoid to drop data
      .drop(UNIQUE_ID_COLUMN, ROW_NUMBER_ON_PARTITION_COLUMN, WINDOW_ID_COLUMN)
  }
}
