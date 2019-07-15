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

import io.arlas.data.math.interpolations.splineInterpolateAndResample
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class ArlasResampler(
                      dataModel: DataModel,
                      aggregationColumnName: String,
                      spark: SparkSession,
                      timeSampling: Long)
    extends ArlasTransformer(
      dataModel,
      Vector(arlasTimestampColumn, arlasPartitionColumn, aggregationColumnName)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._

    val columns = dataset.columns
    val schema = dataset.schema

    val interpolatedRows = dataset
      .toDF()
      .map(row =>
        (row.getString(row.fieldIndex(aggregationColumnName)), List(row.getValuesMap(columns))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
        case (_, timeserie) => {
          splineInterpolateAndResample(dataModel, timeSampling, timeserie, columns)
        }
      }
      .map(entry => Row.fromSeq(columns.map(entry.getOrElse(_, null)).toSeq))

    spark.sqlContext.createDataFrame(interpolatedRows, schema)
  }

}
