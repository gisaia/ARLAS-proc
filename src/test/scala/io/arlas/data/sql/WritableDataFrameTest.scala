/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.sql

import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import io.arlas.data.transform.{ArlasTest, DataFrameException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

import scala.collection.immutable.ListMap

class WritableDataFrameTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 0l, 0.123, "toulouse"),
      Seq("id1", 30l, 1.234, "blagnac")
    ),
    ListMap(
      "id" -> (StringType, true),
      arlasTimestampColumn -> (LongType, true),
      "speed" -> (DoubleType, true),
      "city" -> (StringType, true)
    )
  )

  "withColumnsNested" should "create structures recursively" in {

    val expectedDF =
      testDF.withColumn("user", struct(struct(col("city").as("address_city")).as("address")))
    val transformedDF =
      testDF.withColumnsNested(
        Map(
          "user" ->
            ColumnGroup("address" ->
              ColumnGroup("address_city" -> "city"))))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withColumnsNested" should "be able to use columns objects" in {

    val speedCityColumn = concat(col("speed"), lit("-"), col("city"))
    val castedSpeed = col("speed").cast(StringType)
    val expectedDF = testDF
      .withColumn("cast", struct(speedCityColumn.as("speed_city"), castedSpeed.as("casted_speed")))

    val transformedDF =
      testDF.withColumnsNested(
        Map("cast" ->
          ColumnGroup("speed_city" -> speedCityColumn, "casted_speed" -> castedSpeed)))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withColumnsNested" should "fail if a column already exists with one of the basic structures names" in {

    val thrown = intercept[DataFrameException] {
      testDF.withColumnsNested(
        Map(
          "city" -> ColumnGroup("new_city" -> "city")
        ))
    }
    assert(thrown.getMessage === "ColumnGroup city cannot be created because a column already exists with this name")
  }

}
