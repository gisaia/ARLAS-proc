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

import io.arlas.data.sql._
import io.arlas.data.transform.tools.DataFrameFormatter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class DataFrameFormatterTest extends ArlasTest {

  "DataFrameFormatter " should " fix invalid column names" in {

    val sourceDF = rawDF
      .withColumn("white space", lit(0).cast(IntegerType))
      .withColumn("special:*$char/;?", lit(1).cast(IntegerType))
      .withColumn("_start_with_underscore", lit(2).cast(IntegerType))

    val expectedDF = rawDF
      .withColumn("white_space", lit(0).cast(IntegerType))
      .withColumn("specialchar", lit(1).cast(IntegerType))
      .withColumn("start_with_underscore", lit(2).cast(IntegerType))

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(new DataFrameFormatter(dataModel))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "DataFrameFormatter " should " fail with missing DataModel columns" in {

    val sourceDF = rawDF
      .drop(dataModel.latColumn)

    val thrown = intercept[DataFrameException] {
      sourceDF
        .enrichWithArlas(new DataFrameFormatter(dataModel))
    }
    assert(
      thrown.getMessage === "The lat columns are not included in the DataFrame with the following columns: id, timestamp, lon, speed")
  }

  "DataFrameFormatter " should " fail with missing double columns" in {

    val thrown = intercept[DataFrameException] {
      rawDF
        .enrichWithArlas(new DataFrameFormatter(dataModel, Vector("notExistingCol")))
    }
    assert(
      thrown.getMessage === "The notExistingCol columns are not included in the DataFrame with the following columns: id, timestamp, lat, lon, speed")
  }

  "DataFrameFormatter " should " cast double columns" in {

    val sourceDF = rawDF
      .withColumn("stringdouble", lit("000.5"))
      .withColumn("stringeuropeandouble", lit("000,5"))

    val expectedDF = rawDF
    //using when/otherwise to make column nullable
      .withColumn("stringdouble", when(lit(true), lit(0.5)).otherwise(null))
      .withColumn("stringeuropeandouble", when(lit(true), lit(0.5)).otherwise(null))

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(
        new DataFrameFormatter(dataModel, Vector("stringdouble", "stringeuropeandouble")))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
