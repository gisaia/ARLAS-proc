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
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKTWriter

class FlowFragmentMapperTest extends ArlasTest {

  val averagedColumn = "speed"
  val standardDeviationEllipsisNbPoints = 12

  val expectedSchema = arlasTestDF.schema
    .add(StructField(arlasTrackId, StringType, true))
    .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
    .add(StructField(arlasTrackTrail, StringType, true))
    .add(StructField(arlasTrackDuration, LongType, true))
    .add(StructField(arlasTrackTimestampStart, LongType, true))
    .add(StructField(arlasTrackTimestampEnd, LongType, true))
    .add(StructField(arlasTrackTimestampCenter, LongType, true))
    .add(StructField(arlasTrackLocationLat, DoubleType, true))
    .add(StructField(arlasTrackLocationLon, DoubleType, true))
    .add(StructField(arlasTrackLocationPrecisionValueLat, DoubleType, true))
    .add(StructField(arlasTrackLocationPrecisionValueLon, DoubleType, true))
    .add(StructField(arlasTrackLocationPrecisionGeometry, StringType, true))
    .add(StructField(arlasTrackDistanceGpsTravelled, DoubleType, true))
    .add(StructField(arlasTrackDistanceGpsStraigthLine, DoubleType, true))
    .add(StructField(arlasTrackDistanceGpsStraigthness, DoubleType, true))
    .add(StructField(arlasTrackDynamicsGpsSpeedKmh, DoubleType, true))
    .add(StructField(arlasTrackDynamicsGpsBearing, DoubleType, true))
    .add(StructField(arlasTrackPrefix + averagedColumn, DoubleType, true))

  val expectedData = arlasTestDF
    .collect()
    .groupBy(_.getAs[String](dataModel.idColumn))
    .flatMap {
      case (id, rows) => {
        rows
          .sliding(2)
          .map(window => {

            val prevLat = window(0).getAs[Double](dataModel.latColumn)
            val prevLon = window(0).getAs[Double](dataModel.lonColumn)
            val curLat = window(1).getAs[Double](dataModel.latColumn)
            val curLon = window(1).getAs[Double](dataModel.lonColumn)
            val prevTimestamp = window(0).getAs[Long](arlasTimestampColumn)
            val curTimestamp = window(1).getAs[Long](arlasTimestampColumn)

            val latMean = (curLat + prevLat) / 2
            val lonMean = (curLon + prevLon) / 2
            val prevLatDiff = Math.pow(prevLat - latMean, 2)
            val curLatDiff = Math.pow(curLat - latMean, 2)
            val prevLonDiff = Math.pow(prevLon - lonMean, 2)
            val curLonDiff = Math.pow(curLon - lonMean, 2)
            val latVariance = (prevLatDiff + curLatDiff) / 2
            val lonVariance = (prevLonDiff + curLonDiff) / 2
            val latStd = Math.sqrt(latVariance)
            val lonStd = Math.sqrt(lonVariance)

            val start = new Coordinate(prevLon, prevLat)
            val end = new Coordinate(curLon, curLat)
            val trail =
              if (start.equals2D(end)) new GeometryFactory().createPoint(start)
              else new GeometryFactory().createLineString(Array(start, end))

            val geodesicCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
            geodesicCalculator.setStartingGeographicPoint(prevLon, prevLat)
            geodesicCalculator.setDestinationGeographicPoint(curLon, curLat)

            val duration = curTimestamp - prevTimestamp

            val azimuth = geodesicCalculator.getAzimuth
            val bearing = ((azimuth % 360) + 360) % 360

            Row.fromSeq(window(1).toSeq ++ Array[Any](
              s"""${id}#${prevTimestamp}_${curTimestamp}""",
              2,
              new WKTWriter().write(trail),
              duration,
              prevTimestamp,
              curTimestamp,
              (prevTimestamp + curTimestamp) / 2,
              latMean,
              lonMean,
              latStd,
              lonStd,
              GeoTool
                .getStandardDeviationEllipsis(
                  latMean,
                  lonMean,
                  latStd,
                  lonStd,
                  standardDeviationEllipsisNbPoints
                )
                .get,
              geodesicCalculator.getOrthodromicDistance(),
              geodesicCalculator.getOrthodromicDistance(),
              1.0,
              geodesicCalculator.getOrthodromicDistance() / duration / 1000 * 3600,
              bearing,
              (window(0).getAs[Double](averagedColumn) + window(1)
                .getAs[Double](averagedColumn)) / 2.0
            ))
          })
      }
    }
    .toSeq

  val expectedDF = spark
    .createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )

  "FlowFragmentMapper transformation" should "fill the arlas_track* columns against dataframe's timeseries" in {

    val transformedDF: DataFrame = arlasTestDF
      .enrichWithArlas(
        new FlowFragmentMapper(dataModel,
                               spark,
                               dataModel.idColumn,
                               List(averagedColumn),
                               standardDeviationEllipsisNbPoints))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
