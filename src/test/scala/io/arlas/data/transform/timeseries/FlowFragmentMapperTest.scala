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

package io.arlas.data.transform.timeseries

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKTWriter

class FlowFragmentMapperTest extends ArlasTest {

  val averagedColumn = "speed"

  val expectedSchema = arlasTestDF.schema
    .add(StructField(arlasTrackId, StringType, true))
    .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
    .add(StructField(arlasTrackTrail, StringType, true))
    .add(StructField(arlasTrackDuration, LongType, true))
    .add(StructField(arlasTrackTimestampStart, LongType, true))
    .add(StructField(arlasTrackTimestampEnd, LongType, false))
    .add(StructField(arlasTrackTimestampCenter, LongType, true))
    .add(StructField(arlasTrackLocationLat, DoubleType, true))
    .add(StructField(arlasTrackLocationLon, DoubleType, true))
    .add(StructField(arlasTrackPrefix + averagedColumn, DoubleType, true))

  val expectedData = arlasTestDF
    .collect()
    .groupBy(_.getAs[String](dataModel.idColumn))
    .flatMap {
      case (id, rows) => {
        rows
          .sliding(2)
          .map(window => {
            val start = new Coordinate(window(0).getAs[Double](dataModel.lonColumn),
                                       window(0).getAs[Double](dataModel.latColumn))
            val end = new Coordinate(window(1).getAs[Double](dataModel.lonColumn),
                                     window(1).getAs[Double](dataModel.latColumn))
            val trail =
              if (start.equals2D(end)) new GeometryFactory().createPoint(start)
              else new GeometryFactory().createLineString(Array(start, end))

            Row.fromSeq(window(1).toSeq ++ Array[Any](
              s"""${id}#${window(0).getAs[Long](arlasTimestampColumn)}_${window(1).getAs[Long](
                arlasTimestampColumn)}""",
              2,
              new WKTWriter().write(trail),
              window(1).getAs[Long](arlasTimestampColumn) - window(0).getAs[Long](
                arlasTimestampColumn),
              window(0).getAs[Long](arlasTimestampColumn),
              window(1).getAs[Long](arlasTimestampColumn),
              (window(0).getAs[Long](arlasTimestampColumn) + window(1).getAs[Long](
                arlasTimestampColumn)) / 2,
              trail.getCentroid.getCoordinate.y,
              trail.getCentroid.getCoordinate.x,
              (window(0).getAs[Double](averagedColumn) + window(1)
                .getAs[Double](averagedColumn)) / 2.0
            ))
          })
      }
    }
    .toSeq

  val expectedDF = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedData),
    expectedSchema
  )

  "FlowFragmentMapper transformation" should "fill the arlas_track* columns against dataframe's timeseries" in {

    val transformedDF: DataFrame = arlasTestDF
      .enrichWithArlas(
        new FlowFragmentMapper(dataModel, spark, dataModel.idColumn, List(averagedColumn)))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
