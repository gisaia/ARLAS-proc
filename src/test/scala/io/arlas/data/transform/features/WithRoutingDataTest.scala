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
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.{ArlasMockServer, ArlasTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

import scala.collection.immutable.ListMap

class WithRoutingDataTest extends ArlasTest with ArlasMockServer {

  val testDF =
    createDataFrameWithTypes(
      spark,
      List(
        Seq(
          "id1",
          "LINESTRING (11.782009 42.099761, 11.781858 42.099615, 11.782359 42.100029) ",
          "LINESTRING (11.782513 42.099937, 11.7826 42.100037, 11.782598 42.100161, 11.7824 42.100382, 11.782237 42.100637, 11.782015 42.100842, 11.781965 42.100969, 11.782027 42.101033, 11.782347 42.101166, 11.782383 42.101266, 11.781642 42.102489, 11.781433 42.102688, 11.781474 42.102779, 11.781537 42.102852, 11.781626 42.102842, 11.781731 42.102857, 11.781864 42.102808, 11.782034 42.102651, 11.782209 42.102432, 11.78286 42.101487, 11.784622 42.098796, 11.784834 42.098425, 11.784804 42.098278, 11.784703 42.09823, 11.784566 42.098231, 11.7845 42.098255, 11.784415 42.098335, 11.784057 42.098898, 11.783827 42.099103, 11.783544 42.09955, 11.783415 42.099719, 11.783011 42.099493, 11.78276 42.099245, 11.78265 42.099288, 11.782521 42.099285, 11.782377 42.099257, 11.782521 42.099285, 11.78265 42.099288, 11.78276 42.099245, 11.783011 42.099493, 11.783415 42.099719, 11.782692 42.100954, 11.782266 42.100767, 11.782237 42.100637, 11.7824 42.100382, 11.782598 42.100161, 11.7826 42.100037, 11.782524 42.099949)",
          1695.685,
          260l
        ),
        Seq(
          "id2",
          "LINESTRING (11.782009 42.099761, 11.781858 42.099615, 11.782359 42.100029) ",
          "LINESTRING (11.782513 42.099937, 11.7826 42.100037, 11.782598 42.100161, 11.7824 42.100382, 11.782237 42.100637, 11.782015 42.100842, 11.781965 42.100969, 11.782027 42.101033, 11.782347 42.101166, 11.782383 42.101266, 11.781642 42.102489, 11.781433 42.102688, 11.781474 42.102779, 11.781537 42.102852, 11.781626 42.102842, 11.781731 42.102857, 11.781864 42.102808, 11.782034 42.102651, 11.782209 42.102432, 11.78286 42.101487, 11.784622 42.098796, 11.784834 42.098425, 11.784804 42.098278, 11.784703 42.09823, 11.784566 42.098231, 11.7845 42.098255, 11.784415 42.098335, 11.784057 42.098898, 11.783827 42.099103, 11.783544 42.09955, 11.783415 42.099719, 11.783011 42.099493, 11.78276 42.099245, 11.78265 42.099288, 11.782521 42.099285, 11.782377 42.099257, 11.782521 42.099285, 11.78265 42.099288, 11.78276 42.099245, 11.783011 42.099493, 11.783415 42.099719, 11.782692 42.100954, 11.782266 42.100767, 11.782237 42.100637, 11.7824 42.100382, 11.782598 42.100161, 11.7826 42.100037, 11.782524 42.099949)",
          1695.685,
          260l
        )
      ),
      ListMap(
        "id" -> (StringType, true),
        "trail" -> (StringType, true),
        "expected_refined_trail" -> (StringType, true),
        "expected_refined_distance" -> (DoubleType, true),
        "expected_refined_duration" -> (LongType, true)
      )
    )

  val baseDF =
    testDF.drop("expected_refined_trail", "expected_refined_distance", "expected_refined_duration")

  "WithRefinedTrail" should "get refined data" in {

    val expectedDF = testDF
      .withColumnRenamed("expected_refined_trail", arlasTrackRoutingTrailRefined)
      .withColumnRenamed("expected_refined_distance", arlasTrackRoutingDistance)
      .withColumnRenamed("expected_refined_duration", arlasTrackRoutingDuration)

    val transformedDF =
      baseDF.enrichWithArlas(new WithRoutingData("http://localhost:8080/route?%s&vehicle=car&locale=en&calc_points=true&instructions=false&points_encoded=false&type=json",
        "trail"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithRefinedTrail" should "get refined data based on condition" in {

    val isId1 = col("id").equalTo("id1")

    val transformedDF = baseDF
      .withColumn("condition", isId1)
      .enrichWithArlas(new WithRoutingData("http://localhost:8080/route?%s&vehicle=car&locale=en&calc_points=true&instructions=false&points_encoded=false&type=json",
        "trail", Some("condition")))
      .drop("condition")

    val expectedDF = testDF
      .withColumn(arlasTrackRoutingTrailRefined, when(isId1, col("expected_refined_trail")).otherwise(col("trail")))
      .withColumn(arlasTrackRoutingDistance, when(isId1, col("expected_refined_distance")))
      .withColumn(arlasTrackRoutingDuration, when(isId1, col("expected_refined_duration")))
      .drop("expected_refined_trail", "expected_refined_distance", "expected_refined_duration")

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithRefinedTrail" should "return origin trail with invalid trail" in {

    val invalidTrail = when(lit(true), lit("LINESTRING (42.099761 11.782009 , 42.099615 11.781858, 42.100029 11.782359)"))
    val transformedDF = baseDF
      .withColumn("trail",
                  //reverted lat and lon
                  invalidTrail)
      .enrichWithArlas(new WithRoutingData("http://localhost:8080/route?%s&vehicle=car&locale=en&calc_points=true&instructions=false&points_encoded=false&type=json",
        "trail"))
      .drop("trail")

    val expectedDF = testDF
      .drop("expected_refined_trail", "expected_refined_distance", "expected_refined_duration")
      .withColumn(arlasTrackRoutingTrailRefined, invalidTrail)
      .withColumn(arlasTrackRoutingDistance, lit(null).cast(DoubleType))
      .withColumn(arlasTrackRoutingDuration, lit(null).cast(LongType))
      .drop("trail")

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
