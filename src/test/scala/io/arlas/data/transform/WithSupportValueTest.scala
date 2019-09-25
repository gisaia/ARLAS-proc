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
import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types._

class WithSupportValueTest extends ArlasTest {

  import spark.implicits._

  val testDataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX")
  val distanceColumn = "distance"

  val testData = Seq(
    ("ObjectA", 0l, 0d, 0d, 10l, 4.0d, 0.1d, "tempo_other"),
    ("ObjectA", 0l, 0d, 0d, 10l, 4.0d, 0.2d, "tempo_irregular"),
    ("ObjectA", 0l, 0d, 0d, 100l, 10.0d, 0.3d, "tempo_other"),
    ("ObjectA", 0l, 0d, 0d, 100l, 10.0d, 0.4d, "tempo_irregular")
  )

  val expectedData = Seq(
    ("ObjectA", 0l, 0d, 0d, 10l, 4.0d, 0.1d, "tempo_other", Seq(0.1d)),
    ("ObjectA", 0l, 0d, 0d, 10l, 4.0d, 0.2d, "tempo_irregular", Seq(0.4d, 0.4d)),
    ("ObjectA", 0l, 0d, 0d, 100l, 10.0d, 0.3d, "tempo_other", Seq(0.3d)),
    ("ObjectA", 0l, 0d, 0d, 100l, 10.0d, 0.4d, "tempo_irregular", Seq.fill(8)(0.1d))
  )

  val testSchema = StructType(
    List(
      StructField(dataModel.idColumn, StringType, true),
      StructField(dataModel.timestampColumn, LongType, true),
      StructField(dataModel.latColumn, DoubleType, true),
      StructField(dataModel.lonColumn, DoubleType, true),
      StructField(arlasTrackDuration, LongType, true),
      StructField(distanceColumn, DoubleType, true),
      StructField(speedColumn, DoubleType, true),
      StructField(arlasTempoColumn, StringType, true)
    ))

  val expectedSchema = testSchema.add(
    StructField(speedColumn + "_array", ArrayType(DoubleType, true), false)
  )

  val testDF = spark.createDataFrame(testData.toDF().rdd, testSchema)
  val expectedDF = spark
    .createDataFrame(expectedData.toDF().rdd, expectedSchema)

  "WithSupportValues " should "add a column with a list of support column's value" in {

    val transformedDF = testDF
      .enrichWithArlas(
        new WithSupportValues(speedColumn, 5, 8, "tempo_irregular", distanceColumn)
      )

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
