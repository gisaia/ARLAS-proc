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

package io.arlas.data.transform.features

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.{ArlasMockServer, ArlasTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

import scala.collection.immutable.ListMap

class WithGeoDataTest extends ArlasTest with ArlasMockServer {

  val testSchemaFields = ListMap(
    "id" -> (StringType, true),
    "lat" -> (DoubleType, true),
    "lon" -> (DoubleType, true),
    "expected_city" -> (StringType, true),
    "expected_county" -> (StringType, true),
    "expected_state" -> (StringType, true),
    "expected_country" -> (StringType, true),
    "expected_country_code" -> (StringType, true),
    "expected_postcode" -> (StringType, true)
  )

  val testDF =
    createDataFrameWithTypes(
      spark,
      List(
        Seq("id1", 43.636883, 1.373484, "Blagnac", "Toulouse", "Occitanie", "France", "fr", "31700"),
        Seq("id2", 44.636883, 2.373484, "Conques-en-Rouergue", "Rodez", "Occitanie", "France", "fr", "12320"),
        Seq("id3", 41.270568, 6.670123, null, null, null, null, null, null)
      ),
      testSchemaFields
    )

  val baseDF =
    testDF.drop("expected_city", "expected_county", "expected_state", "expected_country", "expected_country_code", "expected_postcode")

  val addressColumnsPrefix = "address_"
  val cityColumn = addressColumnsPrefix + "city"
  val countyColumn = addressColumnsPrefix + "county"
  val stateColumn = addressColumnsPrefix + "state"
  val countryColumn = addressColumnsPrefix + "country"
  val countryCodeColumn = addressColumnsPrefix + "country_code"
  val postcodeColumn = addressColumnsPrefix + "postcode"

  "WithGeoData" should "get GeoData" in {

    val expectedDF = testDF
      .withColumnRenamed("expected_city", cityColumn)
      .withColumnRenamed("expected_county", countyColumn)
      .withColumnRenamed("expected_state", stateColumn)
      .withColumnRenamed("expected_country", countryColumn)
      .withColumnRenamed("expected_country_code", countryCodeColumn)
      .withColumnRenamed("expected_postcode", postcodeColumn)

    val transformedDF = baseDF.enrichWithArlas(new WithGeoData("lat", "lon", addressColumnsPrefix))
    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithGeoData" should "get GeoData when a 'null' condition is true" in {

    val expectedDF = testDF
      .withColumn(cityColumn, when(col("id").equalTo("id2"), col("expected_city")).otherwise(lit(null)))
      .withColumn(countyColumn, when(col("id").equalTo("id2"), col("expected_county")).otherwise(lit(null)))
      .withColumn(stateColumn, when(col("id").equalTo("id2"), col("expected_state")).otherwise(lit(null)))
      .withColumn(countryColumn, when(col("id").equalTo("id2"), col("expected_country")).otherwise(lit(null)))
      .withColumn(countryCodeColumn, when(col("id").equalTo("id2"), col("expected_country_code")).otherwise(lit(null)))
      .withColumn(postcodeColumn, when(col("id").equalTo("id2"), col("expected_postcode")).otherwise(lit(null)))
      .drop("expected_city", "expected_county", "expected_state", "expected_country", "expected_country_code", "expected_postcode")

    //set "id (row with id2) = null" and get address with condition "id = null"
    val transformedDF =
      baseDF
        .withColumn("id", when(col("id").equalTo("id2"), lit(null)).otherwise(col("id")))
        .withColumn("do_get_address", col("id").isNull)
        .enrichWithArlas(new WithGeoData("lat", "lon", addressColumnsPrefix, Some("do_get_address")))
        .withColumn("id", when(col("id") isNull, lit("id2")).otherwise(col("id")))
        .drop("do_get_address")
    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithGeoData" should "get GeoData when a 'equals value' condition is true" in {

    val expectedDF = testDF
      .withColumn(cityColumn, when(col("id").equalTo("id2"), col("expected_city")).otherwise(lit(null)))
      .withColumn(countyColumn, when(col("id").equalTo("id2"), col("expected_county")).otherwise(lit(null)))
      .withColumn(stateColumn, when(col("id").equalTo("id2"), col("expected_state")).otherwise(lit(null)))
      .withColumn(countryColumn, when(col("id").equalTo("id2"), col("expected_country")).otherwise(lit(null)))
      .withColumn(countryCodeColumn, when(col("id").equalTo("id2"), col("expected_country_code")).otherwise(lit(null)))
      .withColumn(postcodeColumn, when(col("id").equalTo("id2"), col("expected_postcode")).otherwise(lit(null)))
      .drop("expected_city", "expected_county", "expected_state", "expected_country", "expected_country_code", "expected_postcode")

    val transformedDF =
      baseDF
        .withColumn("do_get_address", col("lat").equalTo("44.636883"))
        .enrichWithArlas(new WithGeoData("lat", "lon", addressColumnsPrefix, Some("do_get_address")))
        .drop("do_get_address")
    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithGeoData" should "get GeoData but not replace existing address when condition is not verified" in {

    val expectedDF = testDF
      .withColumnRenamed("expected_city", cityColumn)
      .withColumnRenamed("expected_county", countyColumn)
      .withColumnRenamed("expected_state", stateColumn)
      .withColumnRenamed("expected_country", countryColumn)
      .withColumnRenamed("expected_country_code", countryCodeColumn)
      .withColumnRenamed("expected_postcode", postcodeColumn)

    //before transformation, only id2 has an address. Then we get geo data for id1
    val transformedDF = testDF
      .withColumn("do_get_address", col("id").equalTo("id1"))
      .withColumn(cityColumn, when(col("id").equalTo("id2"), col("expected_city")).otherwise(lit(null)))
      .withColumn(countyColumn, when(col("id").equalTo("id2"), col("expected_county")).otherwise(lit(null)))
      .withColumn(stateColumn, when(col("id").equalTo("id2"), col("expected_state")).otherwise(lit(null)))
      .withColumn(countryColumn, when(col("id").equalTo("id2"), col("expected_country")).otherwise(lit(null)))
      .withColumn(countryCodeColumn, when(col("id").equalTo("id2"), col("expected_country_code")).otherwise(lit(null)))
      .withColumn(postcodeColumn, when(col("id").equalTo("id2"), col("expected_postcode")).otherwise(lit(null)))
      .drop("expected_city", "expected_county", "expected_state", "expected_country", "expected_country_code", "expected_postcode")
      .enrichWithArlas(new WithGeoData("lat", "lon", addressColumnsPrefix, Some("do_get_address")))
      .drop("do_get_address")
    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithGeoData" should "not stringify 0.000932 to 9.32E^4 and fail with bad coordinates" in {

    val expectedDF = createDataFrameWithTypes(
      spark,
      List(Seq("id1", 43.636883, 0.000932, "Tasque", "Mirande", "Occitanie", "France", "fr", "32160")),
      testSchemaFields
    )

    val transformedDF = expectedDF
      .drop("expected_city", "expected_county", "expected_state", "expected_country", "expected_country_code", "expected_postcode")
      .enrichWithArlas(new WithGeoData("lat", "lon", "expected_"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
