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
import io.arlas.ml.classification.Hmm
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success}
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.functions._

class HmmProcessor(dataModel: DataModel,
                   spark                 : SparkSession,
                   sourceColumn          : String,
                   hmmModel              : MLModel,
                   partitionColumn       : String,
                   resultColumn          : String )
    extends ArlasTransformer(dataModel, Vector(partitionColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  val UNKNOWN_RESULT       = "Unknown"
  val SOURCE_DOUBLE_COLUMN = "_tempColumn"
  val UNIQUEID_COLUMN      = "_uniqueIdColumn"

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

  def interpolateRows(dataset: Dataset[_], hmmModelContent: String) = {

    import spark.implicits._

    val hmmSchema = StructType(Seq(
      StructField(UNIQUEID_COLUMN, StringType),
      StructField(resultColumn, StringType)
      ))

    val idDF = dataset
      .withColumn(UNIQUEID_COLUMN, concat(col(dataModel.idColumn), col(arlasTimestampColumn)))

    val hmmRDD = idDF
      .withColumn(SOURCE_DOUBLE_COLUMN, col(sourceColumn).cast(DoubleType))
      .select(col(UNIQUEID_COLUMN), col(partitionColumn), col(SOURCE_DOUBLE_COLUMN))
      .map(row =>
             (row.getString(row.fieldIndex(partitionColumn)), List(row.getValuesMap(Seq(UNIQUEID_COLUMN, SOURCE_DOUBLE_COLUMN)))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
                 case (_, timeserie: List[Map[String, Any]]) => {
                   Hmm.predictStatesSequence(timeserie.map(_.getOrElse(SOURCE_DOUBLE_COLUMN, 0d)),
                                             hmmModelContent) match {
                     case Failure(exception) =>
                       logger.error("HMM failed", exception)
//                       //do not block the processing, add a default value
                       timeserie.map(ts => Map(
                         UNIQUEID_COLUMN -> ts.getOrElse(UNIQUEID_COLUMN, null),
                         resultColumn -> UNKNOWN_RESULT))
                     case Success(statesSequence) =>
                       timeserie.zip(statesSequence).map {
                                                           case (ts, state) => Map(
                                                             UNIQUEID_COLUMN -> ts.getOrElse(UNIQUEID_COLUMN, null),
                                                             resultColumn -> state.state)
                                                         }
                   }
                 }
               }
      .map((entry: Map[String, Any]) =>
             Row.fromSeq(Seq(UNIQUEID_COLUMN, resultColumn).map(entry.getOrElse(_, null))))

    val hmmDF: DataFrame = spark.createDataFrame(hmmRDD, hmmSchema)
    idDF.join(hmmDF, UNIQUEID_COLUMN)
  }
}

