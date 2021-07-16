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

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

class FlowFragmentMapper(dataModel: DataModel,
                         spark: SparkSession,
                         aggregationColumnName: String,
                         averageColumns: List[String] = Nil,
                         standardDeviationEllipsisNbPoints: Int = 12)
    extends ArlasTransformer(
      Vector(arlasTimestampColumn, aggregationColumnName, dataModel.latColumn, dataModel.lonColumn) ++ averageColumns.toVector) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    registerUDF()

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
        whenPreviousPointExists(col(arlasTimestampColumn) - lag(arlasTimestampColumn, 1).over(window))
      )
      .withColumn( // track_timestamps_start = ts(start)
                  arlasTrackTimestampStart,
                  whenPreviousPointExists(lag(arlasTimestampColumn, 1).over(window)))
      .withColumn( // track_timestamps_end = ts(end)
                  arlasTrackTimestampEnd,
                  when(lit(true), col(arlasTimestampColumn))) //when(lit(true), ...) makes column nullable
      .withColumn( // track_timestamps_center = mean(timestamp start, timestamp end)
        arlasTrackTimestampCenter,
        whenPreviousPointExists(mean(arlasTimestampColumn).over(window.rowsBetween(-1, 0)).cast(LongType))
      )
      .withColumn(arlasPartitionColumn, // compute new arlas partition value
                  from_unixtime(col(arlasTrackTimestampCenter), arlasPartitionFormat).cast(IntegerType))

//    coalesce force field in schema to be not null
      .withColumn(arlasTimestampColumn, coalesce(col(arlasTrackTimestampCenter), lit(0)))
      .withColumn( // track_location_lat = mean(latitude start, latitude end)
        arlasTrackLocationLat,
        whenPreviousPointExists(round(mean(dataModel.latColumn).over(window.rowsBetween(-1, 0)), GeoTool.LOCATION_DIGITS))
      )
      .withColumn( // track_location_lon = mean(longitude start, longitude end)
        arlasTrackLocationLon,
        whenPreviousPointExists(round(mean(dataModel.lonColumn).over(window.rowsBetween(-1, 0)), GeoTool.LOCATION_DIGITS))
      )
      .withColumn( // track_location_precision_value_lat = standard deviation of latitude
        arlasTrackLocationPrecisionValueLat,
        whenPreviousPointExists(stddev_pop(dataModel.latColumn).over(window.rowsBetween(-1, 0)))
      )
      .withColumn( // track_location_precision_value_lon = standard deviation of longitude
        arlasTrackLocationPrecisionValueLon,
        whenPreviousPointExists(stddev_pop(dataModel.lonColumn).over(window.rowsBetween(-1, 0)))
      )
      .withColumn( // track_location_precision_geometry = ellipsis of standard deviation around the geocenter
        arlasTrackLocationPrecisionGeometry,
        whenPreviousPointExists(callUDF(
          "getStandardDeviationEllipsis",
          col(arlasTrackLocationLat),
          col(arlasTrackLocationLon),
          col(arlasTrackLocationPrecisionValueLat),
          col(arlasTrackLocationPrecisionValueLon)
        ))
      )
      .withColumn( //track_distance_travelled_m = distance between previous and current point
        arlasTrackDistanceGpsTravelled,
        whenPreviousPointExists(callUDF(
          "getDistanceTravelled",
          lag(dataModel.latColumn, 1).over(window),
          lag(dataModel.lonColumn, 1).over(window),
          col(dataModel.latColumn),
          col(dataModel.lonColumn)
        ))
      )
      .withColumn( // track_distance_straigth_line_m = track_distance_travelled_m
        arlasTrackDistanceGpsStraigthLine,
        whenPreviousPointExists(col(arlasTrackDistanceGpsTravelled))
      )
      .withColumn( // track_distance_straigthness = 1
                  arlasTrackDistanceGpsStraigthness,
                  whenPreviousPointExists(lit(1.0)))
      .withColumn( // track_dynamics_gps_speed_kmh = track_distance_travelled_m / arlas_track_duration_s / 1000 * 3600
        arlasTrackDynamicsGpsSpeedKmh,
        whenPreviousPointExists((col(arlasTrackDistanceGpsTravelled) / col(arlasTrackDuration)) / lit(1000) * lit(3600))
      )
      .withColumn( // track_dynamics_gps_bearing = getGPSBearing previous_point current_point
        arlasTrackDynamicsGpsBearing,
        whenPreviousPointExists(
          callUDF(
            "getGPSBearing",
            lag(dataModel.latColumn, 1).over(window),
            lag(dataModel.lonColumn, 1).over(window),
            col(dataModel.latColumn),
            col(dataModel.lonColumn)
          ))
      )

    // Averaged track columns addition
    val trackDFWithAveragedColumns = averageColumns.foldLeft(trackDF) { (dataframe, columnName) =>
      {
        dataframe.withColumn(
          arlasTrackPrefix + columnName,
          whenPreviousPointExists(mean(columnName).over(window.rowsBetween(-1, 0)))
        )
      }
    }

    trackDFWithAveragedColumns.filter(col(arlasTrackId).isNotNull) // remove first points that are not considered as fragment
  }

  private def registerUDF() = {

    spark.udf.register("getTrail", GeoTool.getTrailBetween _)
    spark.udf.register("getDistanceTravelled", GeoTool.getDistanceBetween _)
    spark.udf.register("getGPSBearing", GeoTool.getBearingBetween _)

    def getStandardDeviationEllipsis(latCenter: Double, lonCenter: Double, latStd: Double, lonStd: Double) = {
      GeoTool.getStandardDeviationEllipsis(latCenter, lonCenter, latStd, lonStd, standardDeviationEllipsisNbPoints)
    }
    spark.udf.register("getStandardDeviationEllipsis", getStandardDeviationEllipsis _)

  }

  override def transformSchema(schema: StructType): StructType = {
    val s = checkSchema(schema)
      .add(StructField(arlasTrackId, StringType, true))
      .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
      .add(StructField(arlasTrackTrail, StringType, true))
      .add(StructField(arlasTrackDuration, LongType, true))
      .add(StructField(arlasTrackTimestampStart, LongType, true))
      .add(StructField(arlasTrackTimestampEnd, LongType, true))
      .add(StructField(arlasTrackTimestampCenter, LongType, true))
      .add(StructField(arlasTrackLocationLat, DoubleType, true))
      .add(StructField(arlasTrackLocationLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasTrackDistanceGpsTravelled, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthLine, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthness, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsSpeedKmh, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsBearing, DoubleType, true))
    averageColumns.foldLeft(s) { (currentSchema, columnName) =>
      {
        currentSchema.add(StructField(arlasTrackPrefix + columnName, DoubleType, true))
      }
    }
  }
}
