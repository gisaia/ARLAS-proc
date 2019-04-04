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
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class DataFrameValidatorTest
    extends FlatSpec
    with Matchers
    with TestSparkSession
    with DataFrameTester {

  import spark.implicits._

  val schema = StructType(
    List(
      StructField("id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true)
    )
  )

  val testDataDF = spark.createDataFrame(
    testData.toDF.rdd,
    schema
  )

  "DataFrameValidator " should " fix invalid column names" in {

    val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", sequenceGap = 120)

    val sourceDF = testDataDF
      .withColumn("white space", lit(0).cast(IntegerType))
      .withColumn("special:*$char/;?", lit(1).cast(IntegerType))
      .withColumn("_start_with_underscore", lit(2).cast(IntegerType))

    val expectedDF = testDataDF
      .withColumn("white_space", lit(0).cast(IntegerType))
      .withColumn("specialchar", lit(1).cast(IntegerType))
      .withColumn("start_with_underscore", lit(2).cast(IntegerType))

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new DataFrameValidator(dataModel))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "DataFrameValidator " should " cast dynamic column to DoubleType if necessary" in {

    val dataModel = DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", sequenceGap = 120)

    val sourceDF = testDataDF
      .withColumnRenamed("lat", "oldlat")
      .withColumnRenamed("lon", "oldlon")
      .withColumn("lat", col("oldlat").cast(StringType))
      .withColumn("lon", col("oldlon").cast(FloatType))
      .drop("oldlat", "oldlon")

    val expectedDF = testDataDF
      .withColumnRenamed("lon", "oldlon")
      .withColumn("newlon", col("oldlon").cast(FloatType))
      .withColumn("lon", col("newlon").cast(DoubleType))
      .drop("oldlon", "newlon")

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new DataFrameValidator(dataModel))

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}
