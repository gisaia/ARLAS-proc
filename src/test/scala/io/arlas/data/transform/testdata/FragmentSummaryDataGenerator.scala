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
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTempoColumn, _}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.SortedSet

class FragmentSummaryDataGenerator(spark: SparkSession,
                                   baseDF: DataFrame,
                                   dataModel: DataModel,
                                   speedColumn: String,
                                   tempoProportionsColumns: Map[String, String],
                                   tempoIrregular: String,
                                   standardDeviationEllipsisNbPoints: Int,
                                   // the column on which the rows are grouped
                                   aggregationColumn: String,
                                   // only rows whose _1 == _2  will be aggregated
                                   aggregationCondition: (String, String),
                                   // Define additional aggregations:
                                   // for any aggregated rows, Map[String, Any] is a Map of the fragment summary aggregations
                                   // with (aggregated column name -> value)
                                   // and Array[Row] are the source aggregated rows.
                                   // New columns can be added, or existing columns can be updated
                                   additionalAggregations: (Map[String, Any], Array[Row]) => Map[String, Any] = (values, _) => values,
                                   // schema of columns added in `additionalAggregations`
                                   additionalAggregationsNewColumns: Seq[StructField] = Nil,
                                   // callback executed after rows are aggregated.
                                   // Seq[Row] is the list of aggregated rows, StructType their related schema.
                                   // It returns the updated rows with the updated schema.
                                   afterTransform: (Seq[Row], StructType) => (Seq[Row], StructType) = (rows, schema) => (rows, schema))
    extends TestDataGenerator {

  override def get(): DataFrame = {

    // in the schema after all aggregations have been applied, fields are sorted by their name (in order to not manage insertion order)
    val afterAggregationsSchema = StructType(
      SortedSet(
        // arlasTempoColumn and arlasTrackTempoEmissionIsMulti are added or updated by Fragment Summary (depending on whether they already existed or not)
        (baseDF.schema.fields ++ additionalAggregationsNewColumns :+ StructField(arlasTempoColumn, StringType, true) :+
          StructField(arlasTrackTempoEmissionIsMulti, BooleanType, true)): _*)(Ordering.by(_.name)).toList)

    //aggregate rows, and keep not aggregated rows (optionnally adding null values for new fields)
    val aggregationData = baseDF
      .collect()
      .groupBy(_.getAs[String](aggregationColumn))
      .flatMap {
        case (_, rows) => {

          if (rows(0).getAs[String](aggregationCondition._1) != aggregationCondition._2)
            rowsWithNullForNewFields(rows, baseDF.schema, afterAggregationsSchema)
          else {
            rowsAggregated(rows, afterAggregationsSchema)
          }
        }
      }
      .toSeq

    //apply final callback after all aggregations
    val (data, afterTransformSchema) = afterTransform(aggregationData, afterAggregationsSchema)

    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        afterTransformSchema
      ).withColumn(arlasTrackDynamicsGpsSpeed, col(arlasTrackDistanceGpsTravelled) / col(arlasTrackDuration))
  }

  private def rowsWithNullForNewFields(rows: Array[Row], previousSchema: StructType, newSchema: StructType) = {

    val newFields = newSchema.fieldNames.dropWhile(fn => previousSchema.fieldNames.contains(fn))

    rows.map(r => {

      val valuesByFieldName = r.schema
        .map(field => {
          (field.name, r.getAs[Any](field.name))
        })

      val newValuesByFieldName = newFields.foldLeft(valuesByFieldName) {
        case (s, c) => if (r.schema.fields.map(_.name).contains(c)) s else s :+ (c, null)
      }

      new GenericRowWithSchema(newValuesByFieldName.sortBy(_._1).map(_._2).toArray, newSchema)
    })
  }

  private def rowsAggregated(rows: Array[Row], afterAggregationsSchema: StructType) = {

    val sortedRows = rows.sortBy(_.getAs[Long](arlasTrackTimestampStart))

    def getWindowDoubles(colName: String) = sortedRows.map(_.getAs[Double](colName))
    def getWindowLongs(colName: String) = sortedRows.map(_.getAs[Long](colName))
    def getWindowStrings(colName: String) = sortedRows.map(_.getAs[String](colName))
    def getWindowInts(colName: String) = sortedRows.map(_.getAs[Int](colName))

    val distanceGpsStraightLine = GeoTool
      .getStraightLineDistanceFromTrails(getWindowStrings(arlasTrackTrail))
      .getOrElse(-1.0)

    val distanceGpsTravelled = getWindowDoubles(arlasTrackDistanceGpsTravelled).sum

    val distanceGpsStraightness =
      if (distanceGpsTravelled != 0) distanceGpsStraightLine / distanceGpsTravelled
      else null

    val nbPoints = getWindowInts(arlasTrackNbGeopoints).sum - sortedRows
      .count(p => true) + 1

    val timestampStart = getWindowLongs(arlasTrackTimestampStart).head
    val timestampEnd = getWindowLongs(arlasTrackTimestampEnd).last
    val timestampCenter = mean(Seq(timestampStart, timestampEnd)).toLong
    val dateFormat = new SimpleDateFormat(arlasPartitionFormat)
    val arlasPartition = dateFormat.format(timestampCenter * 1000l).toInt
    val duration = getWindowLongs(arlasTrackDuration).sum

    val precisionValueLat =
      scaleDouble(stdDev(getWindowDoubles(arlasTrackLocationLat)), GeoTool.LOCATION_PRECISION_DIGITS)
    val precisionValueLon =
      scaleDouble(stdDev(getWindowDoubles(arlasTrackLocationLon)), GeoTool.LOCATION_PRECISION_DIGITS)
    val locationLat =
      scaleDouble(mean(getWindowDoubles(arlasTrackLocationLat)), GeoTool.LOCATION_DIGITS)
    val locationLon =
      scaleDouble(mean(getWindowDoubles(arlasTrackLocationLon)), GeoTool.LOCATION_DIGITS)
    val distanceSensorTravelled =
      getWindowDoubles(arlasTrackDistanceSensorTravelled).sum

    val objectId = sortedRows.head.getAs[String](dataModel.idColumn)
    val trackId = objectId + "#" + timestampStart + "_" + timestampEnd
    val precisionGeometry =
      GeoTool
        .getStandardDeviationEllipsis(locationLat, locationLon, precisionValueLat, precisionValueLon, standardDeviationEllipsisNbPoints)
        .get

    val tempoProportions = sortedRows
      .flatMap(w => {
        tempoProportionsColumns
          .map {
            case (tpc, tc) =>
              (tc, w.getAs[Double](tpc) * w.getAs[Long](arlasTrackDuration))
          }
      })
      .groupBy(_._1)
      .map(v => (v._1, v._2.map(_._2).sum / duration))

    val mainTempo =
      if (tempoProportions.filter(_._2 > 0.0d).size == 1 && tempoProportions.filter(_._2 > 0.0d).head._1 == tempoIrregular)
        tempoIrregular
      else
        tempoProportions
          .filterNot(_._1 == tempoIrregular)
          .toList
          .sortBy(t => (-t._2, t._1))
          .head
          ._1

    val isTempoEmissionMulti = tempoProportions
      .filterNot(_._1 == tempoIrregular)
      .filter(_._2 > 0.1)
      .size > 1

    val weightAveragedSpeed = sortedRows
      .map(w => w.getAs[Double](speedColumn) * w.getAs[Long](arlasTrackDuration))
      .sum / duration

    val fragmentSummaryAggregationData = Map(
      dataModel.idColumn -> sortedRows.head.getAs[String](dataModel.idColumn),
      dataModel.timestampColumn -> null,
      dataModel.latColumn -> null,
      dataModel.lonColumn -> null,
      speedColumn -> weightAveragedSpeed,
      arlasPartitionColumn -> arlasPartition,
      arlasTimestampColumn -> timestampCenter,
      arlasTrackId -> trackId,
      arlasTrackNbGeopoints -> nbPoints,
      arlasTrackTrail -> null,
      arlasTrackDuration -> duration,
      arlasTrackTimestampStart -> timestampStart,
      arlasTrackTimestampEnd -> timestampEnd,
      arlasTrackTimestampCenter -> timestampCenter,
      arlasTrackLocationLat -> locationLat,
      arlasTrackLocationLon -> locationLon,
      arlasTrackEndLocationLat -> locationLat,
      arlasTrackEndLocationLon -> locationLon,
      arlasTrackLocationPrecisionValueLat -> precisionValueLat,
      arlasTrackLocationPrecisionValueLon -> precisionValueLon,
      arlasTrackLocationPrecisionGeometry -> precisionGeometry,
      arlasTrackDistanceGpsTravelled -> distanceGpsTravelled,
      arlasTrackDistanceGpsStraigthLine -> distanceGpsStraightLine,
      arlasTrackDistanceGpsStraigthness -> distanceGpsStraightness,
      arlasTrackDynamicsGpsSpeed -> null,
      arlasTrackDynamicsGpsBearing -> null,
      arlasTrackPrefix + speedColumn -> null,
      arlasTrackDistanceSensorTravelled -> distanceSensorTravelled,
      arlasMovingStateColumn -> sortedRows.head.getAs[String](arlasMovingStateColumn),
      arlasCourseOrStopColumn -> sortedRows.head.getAs[String](arlasCourseOrStopColumn),
      arlasCourseStateColumn -> sortedRows.head.getAs[String](arlasCourseStateColumn),
      arlasMotionIdColumn -> null,
      arlasMotionDurationColumn -> null,
      arlasCourseIdColumn -> sortedRows.head.getAs[String](arlasCourseIdColumn),
      arlasCourseDurationColumn -> sortedRows.head.getAs[Long](arlasCourseDurationColumn),
      arlasTempoColumn -> mainTempo,
      arlasTrackTempoEmissionIsMulti -> isTempoEmissionMulti
    ) ++ Map(
      tempoProportionsColumns
        .map(t => (t._1, tempoProportions.getOrElse(t._2, -1.0)))
        .toSeq: _*)

    val withAdditionalAggregationData = additionalAggregations(fragmentSummaryAggregationData, sortedRows)

    Seq(
      new GenericRowWithSchema(
        withAdditionalAggregationData.toList.sortBy(_._1).map(_._2).toArray,
        afterAggregationsSchema
      ))
  }

}
