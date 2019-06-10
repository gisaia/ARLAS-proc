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
import org.apache.spark.sql.functions._
import io.arlas.ml.classification.Hmm
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class HmmProcessor(dataModel: DataModel,
                   spark                 : SparkSession,
                   sourceColumn: String,
                   hmmModelPath          : String,
                   partitionColumn       : String,
                   resultColumn          : String )
    extends ArlasTransformer(dataModel, Vector(partitionColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  val UNKNOWN_RESULT = "Unknown"
  val TEMP_COLUMN = "_tempColumn"

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema).add(StructField(resultColumn, StringType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val columns = dataset.columns
    val hmmModelContent = Try(spark.sparkContext.textFile(hmmModelPath, 1).toLocalIterator.mkString)

    if (!dataset.columns.contains(sourceColumn) || hmmModelContent.isFailure) {
      dataset.withColumn(resultColumn, lit(UNKNOWN_RESULT))
    } else {
      interpolateRows(dataset, hmmModelContent.get)
    }
  }

  def interpolateRows(dataset: Dataset[_], hmmModelContent: String) = {

    import spark.implicits._
    val columns = dataset.columns

    val interpolatedRows = dataset
      .withColumn(TEMP_COLUMN, col(sourceColumn).cast(DoubleType))
      .toDF()
      .map(row =>
             (row.getString(row.fieldIndex(partitionColumn)), List(row.getValuesMap(columns :+ TEMP_COLUMN))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
                 case (_, timeserie: List[Map[String, Any]]) => {
                   Hmm.predictStatesSequence(timeserie.map(_.getOrElse(TEMP_COLUMN, 0d)),
                                             hmmModelContent) match {
                     case Failure(exception) =>
                       logger.error("HMM failed", exception)
                       //do not block the processing, add a default value
                       timeserie.map(_ + (resultColumn -> UNKNOWN_RESULT))
                     case Success(statesSequence) =>
                       timeserie.zip(statesSequence).map {
                                                           case (ts, state) => ts + (resultColumn -> state.state)
                                                         }
                   }
                 }
               }
      .map((entry: Map[String, Any]) =>
             Row.fromSeq((columns :+ resultColumn).map(entry.getOrElse(_, null)).toSeq))

    spark.sqlContext.createDataFrame(interpolatedRows, transformSchema(dataset.schema))
  }

}
