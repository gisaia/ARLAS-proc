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

import org.apache.spark.sql.{DataFrame, Row}
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class WithArlasDeltaTimestampTest extends ArlasTest {

  val deltaTimestampSchema = StructType(
    List(
      StructField("id", StringType, true),
      StructField(arlasTimestampColumn, LongType, false),
      StructField(arlasVisibleSequenceIdColumn, StringType, true),
      StructField(arlasDeltaTimestampColumn, LongType, true),
      StructField(arlasPreviousDeltaTimestampColumn, LongType, true),
      StructField(arlasDeltaTimestampVariationColumn, LongType, true)))

  val deltaTimestampData: Seq[Row] = visibleSequencesDF.select("id", arlasTimestampColumn, arlasVisibleSequenceIdColumn).collect()
    .groupBy(_.getAs[String](arlasVisibleSequenceIdColumn))
    .flatMap(f => {
      val data = f._2
      (Seq(Row.fromSeq(data(0).toSeq ++ Array[Any](
          null,
          null,
          null))) :+
       Row.fromSeq(data(1).toSeq ++ Array[Any](
          data(1).getAs[Long](arlasTimestampColumn) - data(0).getAs[Long](arlasTimestampColumn),
          null,
          null))) ++
      data.sliding(3).map(window => Row.fromSeq(window(2).toSeq ++ Array[Any](
          window(2).getAs[Long](arlasTimestampColumn) - window(1).getAs[Long](arlasTimestampColumn),
          window(1).getAs[Long](arlasTimestampColumn) - window(0).getAs[Long](arlasTimestampColumn),
          window(2).getAs[Long](arlasTimestampColumn) - window(1).getAs[Long](arlasTimestampColumn) - (window(1).getAs[Long](arlasTimestampColumn) - window(0).getAs[Long](arlasTimestampColumn))
        )))
    }).toSeq

  val deltaTimestampDF = spark.createDataFrame(
    spark.sparkContext.parallelize(deltaTimestampData),
    deltaTimestampSchema
    )

  "WithArlasDeltaTimestamp transformation" should "fill the arlasDeltaTimestamp against dataframe's timeseries" in {

    val sourceDF = visibleSequencesDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new WithArlasDeltaTimestamp(dataModel, spark, arlasVisibleSequenceIdColumn))
      .select("id", arlasTimestampColumn, arlasVisibleSequenceIdColumn, arlasDeltaTimestampColumn, arlasPreviousDeltaTimestampColumn, arlasDeltaTimestampVariationColumn)

    val expectedDF = deltaTimestampDF
    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
