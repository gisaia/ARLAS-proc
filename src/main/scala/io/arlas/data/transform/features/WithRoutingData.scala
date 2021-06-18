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

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.arlas.data.app.ArlasProcConfig
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.{GeoTool, RestTool}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Extract routing data from a trail.
  * The transformers add 3 columns to the dataframe, from an external web-service:
  * - trail refined (i.a. redesigned trail, that matches "real" roads)
  * - distance covered over the refined trail
  * - approximate duration spent over the refined trail
  * @param trailColumn input trail column
  * @param conditionColumn if provided, only rows whose value of conditionColumn is "true" are processed
  */
class WithRoutingData(trailColumn: String, conditionColumn: Option[String] = None) extends ArlasTransformer(Vector(trailColumn)) {

  @transient lazy val MAPPER = new ObjectMapper().registerModule(DefaultScalaModule)

  val tmpRoutingColumn = "tmp_routing"
  val tmpTrailRefinedColumn = "trail_refined"
  val tmpDistanceColumn = "distance"
  val tmpDurationColumn = "duration"

  val getTrailRefinedUDF = udf((trail: String) => {

    RestTool
      .get(ArlasProcConfig.getRefineTrailUrl(trail))
      .map(response => {
        val refinedData = MAPPER.readValue(response, classOf[Route])
        Option(refinedData)
          .map(
            res =>
              RoutingResult(
                GeoTool.listOfCoordsToLineString(res.paths.head.points.coordinates.map(c => (c(0), c(1)))),
                Some(res.paths.head.distance),
                Some(res.paths.head.time)
            ))
          .getOrElse(RoutingResult(Some(trail)))
      })
      .getOrElse(RoutingResult(Some(trail)))
  })

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset
      .withColumn(tmpRoutingColumn, explode(array(whenConditionOtherwise(getTrailRefinedUDF(col(trailColumn))))))
      .withColumn(arlasTrackRoutingTrailRefined,
                  whenConditionOtherwise(col(tmpRoutingColumn + "." + tmpTrailRefinedColumn), col(trailColumn)))
      .withColumn(arlasTrackRoutingDistance, whenConditionOtherwise(col(tmpRoutingColumn + "." + tmpDistanceColumn)))
      .withColumn(
        //duration in seconds
        arlasTrackRoutingDuration,
        whenConditionOtherwise(
          round(col(tmpRoutingColumn + "." + tmpDurationColumn) / 1000)
            .cast(LongType))
      )
      .drop(tmpRoutingColumn)
  }

  override def transformSchema(schema: StructType): StructType = {
    super
      .transformSchema(schema)
      .add(StructField(arlasTrackRoutingTrailRefined, StringType, true))
      .add(StructField(arlasTrackRoutingDistance, DoubleType, true))
      .add(StructField(arlasTrackRoutingDuration, LongType, true))
  }

  def whenConditionOtherwise(expr: Column, otherwise: Column = lit(null)) =
    if (conditionColumn.isDefined)
      when(col(conditionColumn.get).equalTo(lit(true)), expr).otherwise(otherwise)
    else expr

}

case class RoutingResult(trail_refined: Option[String] = None, distance: Option[Double] = None, duration: Option[Long] = None)

@JsonIgnoreProperties(ignoreUnknown = true) case class Route(@JsonProperty paths: Array[Path])
@JsonIgnoreProperties(ignoreUnknown = true) case class Path(
    @JsonProperty points: Points,
    @JsonProperty distance: Double,
    @JsonProperty time: Long
)
@JsonIgnoreProperties(ignoreUnknown = true) case class Points(@JsonProperty coordinates: Array[Array[Double]])
