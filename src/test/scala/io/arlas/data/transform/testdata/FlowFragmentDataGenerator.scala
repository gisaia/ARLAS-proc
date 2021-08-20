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

package io.arlas.data.transform.testdata
import java.text.SimpleDateFormat

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTestHelper.{mean, stdDev, _}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKTWriter

class FlowFragmentDataGenerator(
    spark: SparkSession,
    baseDF: DataFrame,
    dataModel: DataModel,
    averagedColumns: List[String],
    standardDeviationEllipsisNbPoints: Int
) extends TestDataGenerator {

  override def get(): DataFrame = {

    val flowFragmentData: Seq[Row] = getData
    val flowFragmentSchema: StructType = getSchema

    spark
      .createDataFrame(
        spark.sparkContext.parallelize(flowFragmentData),
        flowFragmentSchema
      )
  }

  private def getData = {
    val flowFragmentData = baseDF
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
              val timestampStart = window(0).getAs[Long](arlasTimestampColumn)
              val timestampEnd = window(1).getAs[Long](arlasTimestampColumn)
              val timestampCenter = (timestampStart + timestampEnd) / 2
              val dateFormat = new SimpleDateFormat(arlasPartitionFormat)
              val arlasPartition = dateFormat.format(timestampCenter * 1000).toInt

              val latMean =
                scaleDouble(mean(Seq(prevLat, curLat)), GeoTool.LOCATION_DIGITS)
              val lonMean =
                scaleDouble(mean(Seq(prevLon, curLon)), GeoTool.LOCATION_DIGITS)
              val latStd = stdDev(Seq(prevLat, curLat))
              val lonStd = stdDev(Seq(prevLon, curLon))

              val start = new Coordinate(prevLon, prevLat)
              val end = new Coordinate(curLon, curLat)
              val trail =
                if (start.equals2D(end)) new GeometryFactory().createPoint(start)
                else new GeometryFactory().createLineString(Array(start, end))

              val geodesicCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
              geodesicCalculator.setStartingGeographicPoint(prevLon, prevLat)
              geodesicCalculator.setDestinationGeographicPoint(curLon, curLat)

              val duration = timestampEnd - timestampStart

              val azimuth = geodesicCalculator.getAzimuth
              val bearing = ((azimuth % 360) + 360) % 360

              val averagedValues = averagedColumns.map(averagedColumn =>
                (window(0).getAs[Double](averagedColumn) + window(1)
                  .getAs[Double](averagedColumn)) / 2.0)

              val rowWithTimestamp = window(1).toSeq.zipWithIndex.map {
                case (v, i) =>
                  if (i == window(1).fieldIndex(arlasTimestampColumn)) timestampCenter else v
              }

              Row.fromSeq(rowWithTimestamp ++ Array[Any](
                s"""${id}#${timestampStart}_${timestampEnd}""",
                2,
                new WKTWriter().write(trail),
                duration,
                timestampStart,
                timestampEnd,
                timestampCenter,
                arlasPartition,
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
                bearing
              ) ++ averagedValues)
            })
        }
      }
      .toSeq
    flowFragmentData
  }

  private def getSchema = {

    val schema = baseDF.schema
      .add(StructField(arlasTrackId, StringType, true))
      .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
      .add(StructField(arlasTrackTrail, StringType, true))
      .add(StructField(arlasTrackDuration, LongType, true))
      .add(StructField(arlasTrackTimestampStart, LongType, true))
      .add(StructField(arlasTrackTimestampEnd, LongType, true))
      .add(StructField(arlasTrackTimestampCenter, LongType, true))
      .add(StructField(arlasPartitionColumn, IntegerType, true))
      .add(StructField(arlasTrackLocationLat, DoubleType, true))
      .add(StructField(arlasTrackLocationLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasTrackDistanceGpsTravelled, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthLine, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthness, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsSpeed, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsBearing, DoubleType, true))

    averagedColumns.foldLeft(schema) { (s, c) =>
      s.add(StructField(arlasTrackPrefix + c, DoubleType, true))
    }
  }

}
