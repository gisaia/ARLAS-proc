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
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTWriter

class FlowFragmentMapper(dataModel: DataModel,
                         spark: SparkSession,
                         aggregationColumnName: String,
                         averageColumns: List[String] = Nil)
    extends ArlasTransformer(
      dataModel,
      Vector(arlasTimestampColumn, aggregationColumnName) ++ averageColumns.toVector) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    def getTrailGeometry(lat: Double, lon: Double, prevLat: Double, prevLon: Double): Geometry = {
      val start = new Coordinate(lon, lat)
      val end = new Coordinate(prevLon, prevLat)
      if (start.equals2D(end)) {
        new GeometryFactory().createPoint(start)
      } else {
        new GeometryFactory().createLineString(Array(start, end))
      }
    }

    // Track geometry WKT (LineString)
    def getTrail(lat: Double, lon: Double, prevLat: Double, prevLon: Double): Option[String] = {
      Some((new WKTWriter()).write(getTrailGeometry(lat, lon, prevLat, prevLon)))
    }
    spark.udf.register("getTrail", getTrail _)

    // Track centroid latitude
    def getCentroidLat(lat: Double,
                       lon: Double,
                       prevLat: Double,
                       prevLon: Double): Option[Double] = {
      Some(getTrailGeometry(lat, lon, prevLat, prevLon).getCentroid.getCoordinate.y)
    }
    spark.udf.register("getCentroidLat", getCentroidLat _)

    // Track centroid longitude
    def getCentroidLon(lat: Double,
                       lon: Double,
                       prevLat: Double,
                       prevLon: Double): Option[Double] = {
      Some(getTrailGeometry(lat, lon, prevLat, prevLon).getCentroid.getCoordinate.x)
    }
    spark.udf.register("getCentroidLon", getCentroidLon _)

    // spark window
    val window = Window
      .partitionBy(aggregationColumnName)
      .orderBy(arlasTimestampColumn)

    def whenPreviousPointExists(expression: Column) =
      when(lag(arlasTimestampColumn, 1).over(window).isNull, null)
        .otherwise(expression)

    // Basic track columns addition
    val trackDF = dataset
      .withColumn( // track_id = ID#ts(start)_ts(end)
        arlasTrackId,
        whenPreviousPointExists(
          concat(col(aggregationColumnName),
                 lit("#"),
                 lag(arlasTimestampColumn, 1)
                   .over(window),
                 lit("_"),
                 lit(col(arlasTimestampColumn))))
      )
      .withColumn( // track_nb_geopoints = 2
                  arlasTrackNbGeopoints,
                  when(lag(arlasTimestampColumn, 1).over(window).isNull, null)
                    .otherwise(lit(2)))
      .withColumn( // track_trail = LINESTRING(coord(start),coord(end))
        arlasTrackTrail,
        whenPreviousPointExists(
          callUDF("getTrail",
                  lag(dataModel.latColumn, 1).over(window),
                  lag(dataModel.lonColumn, 1).over(window),
                  col(dataModel.latColumn),
                  col(dataModel.lonColumn)))
      )
      .withColumn( // track_duration_s = ts(start) - ts(end)
        arlasTrackDuration,
        whenPreviousPointExists(
          col(arlasTimestampColumn) - lag(arlasTimestampColumn, 1).over(window))
      )
      .withColumn( // track_timestamps_start = ts(start)
                  arlasTrackTimestampStart,
                  whenPreviousPointExists(lag(arlasTimestampColumn, 1).over(window)))
      .withColumn( // track_timestamps_end = ts(end)
                  arlasTrackTimestampEnd,
                  col(arlasTimestampColumn))
      .withColumn( // track_timestamps_center = ts(start) + ts(end) / 2
        arlasTrackTimestampCenter,
        whenPreviousPointExists(
          ((col(arlasTimestampColumn) + lag(arlasTimestampColumn, 1).over(window)) / lit(2))
            .cast(LongType))
      )
      .withColumn( // track_location_lat = trail centroid latitude
        arlasTrackLocationLat,
        whenPreviousPointExists(
          callUDF("getCentroidLat",
                  lag(dataModel.latColumn, 1).over(window),
                  lag(dataModel.lonColumn, 1).over(window),
                  col(dataModel.latColumn),
                  col(dataModel.lonColumn)))
      )
      .withColumn( // track_location_lat = trail centroid latitude
        arlasTrackLocationLon,
        whenPreviousPointExists(
          callUDF("getCentroidLon",
                  lag(dataModel.latColumn, 1).over(window),
                  lag(dataModel.lonColumn, 1).over(window),
                  col(dataModel.latColumn),
                  col(dataModel.lonColumn)))
      )

    // Averaged track columns addition
    val trackDFWithAveragedColumns = averageColumns.foldLeft(trackDF) { (dataframe, columnName) =>
      {
        dataframe.withColumn(
          arlasTrackPrefix + columnName,
          whenPreviousPointExists(
            (lag(columnName, 1).over(window).cast(DoubleType) + col(columnName)
              .cast(DoubleType)) / lit(2).cast(DoubleType))
        )
      }
    }

    trackDFWithAveragedColumns.filter(col(arlasTrackId).isNotNull) // remove first points that are not considered as fragment
  }

  override def transformSchema(schema: StructType): StructType = {
    val s = checkSchema(schema)
      .add(StructField(arlasTrackId, StringType, true))
      .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
      .add(StructField(arlasTrackTrail, StringType, true))
      .add(StructField(arlasTrackDuration, LongType, true))
      .add(StructField(arlasTrackTimestampStart, LongType, true))
      .add(StructField(arlasTrackTimestampEnd, LongType, false))
      .add(StructField(arlasTrackTimestampCenter, LongType, true))
      .add(StructField(arlasTrackLocationLat, DoubleType, true))
      .add(StructField(arlasTrackLocationLon, DoubleType, true))
    averageColumns.foldLeft(s) { (currentSchema, columnName) =>
      {
        currentSchema.add(StructField(arlasTrackPrefix + columnName, DoubleType, true))
      }
    }
  }
}
