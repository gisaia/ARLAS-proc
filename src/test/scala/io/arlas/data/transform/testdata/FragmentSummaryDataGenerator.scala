package io.arlas.data.transform.testdata
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTestHelper.{mean, stdDev}
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTempoColumn, _}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import io.arlas.data.transform.ArlasTestHelper._

import scala.collection.immutable.ListMap

class FragmentSummaryDataGenerator(
    spark: SparkSession,
    baseDF: DataFrame,
    dataModel: DataModel,
    speedColumn: String,
    tempoProportionsColumns: Map[String, String],
    tempoIrregular: String,
    standardDeviationEllipsisNbPoints: Int,
    aggregationColumn: String,
    aggregationCondition: (String, String),
    aggregationColumns: Seq[StructField],
    afterTransformColumns: Seq[StructField],
    additionalAggregations: (ListMap[String, Any], Array[Row]) => ListMap[String, Any],
    afterTransform: (Seq[Row], StructType) => Seq[Row] = (rows, schema) => rows)
    extends TestDataGenerator {

  override def get(): DataFrame = {

    val aggregationData = baseDF
      .collect()
      .groupBy(_.getAs[String](aggregationColumn))
      .flatMap {
        case (id, rows) => {

          if (rows(0).getAs[String](aggregationCondition._1)
                != aggregationCondition._2)
            rowsWithNullForNewFields(rows)
          else {
            rowsAggregated(rows)
          }
        }
      }
      .toSeq

    val schema = getSchemaWithColumns(baseDF, aggregationColumns ++ afterTransformColumns)
    val data = afterTransform(aggregationData, schema)

    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )
  }

  private def rowsWithNullForNewFields(rows: Array[Row]) = {
    rows.map(r =>
      new GenericRowWithSchema({
        aggregationColumns.foldLeft(r.toSeq) {
          case (s, _) => s :+ null
        }
      }.toArray, getSchemaWithColumns(baseDF, aggregationColumns)))
  }

  private def rowsAggregated(rows: Array[Row]) = {

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
    val duration = getWindowLongs(arlasTrackDuration).sum

    val precisionValueLat =
      scaleDouble(stdDev(getWindowDoubles(arlasTrackLocationLat)),
                  GeoTool.LOCATION_PRECISION_DIGITS)
    val precisionValueLon =
      scaleDouble(stdDev(getWindowDoubles(arlasTrackLocationLon)),
                  GeoTool.LOCATION_PRECISION_DIGITS)
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
        .getStandardDeviationEllipsis(locationLat,
                                      locationLon,
                                      precisionValueLat,
                                      precisionValueLon,
                                      standardDeviationEllipsisNbPoints)
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
      if (tempoProportions.size == 1 && tempoProportions.head._1 == tempoIrregular)
        tempoIrregular
      else
        tempoProportions
          .filterNot(_._1 == tempoIrregular)
          .toList
          .sortBy(-_._2)
          .head
          ._1

    val isTempoEmissionMulti = tempoProportions
      .filterNot(_._1 == tempoIrregular)
      .filter(_._2 > 0.1)
      .size > 1

    val weightAveragedSpeed = sortedRows
      .map(w => w.getAs[Double](speedColumn) * w.getAs[Long](arlasTrackDuration))
      .sum / duration

    val fragmentSummaryData = ListMap(
      dataModel.idColumn -> sortedRows.head.getAs[String](dataModel.idColumn),
      dataModel.timestampColumn -> null,
      dataModel.latColumn -> null,
      dataModel.lonColumn -> null,
      speedColumn -> weightAveragedSpeed,
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
      arlasTrackLocationPrecisionValueLat -> precisionValueLat,
      arlasTrackLocationPrecisionValueLon -> precisionValueLon,
      arlasTrackLocationPrecisionGeometry -> precisionGeometry,
      arlasTrackDistanceGpsTravelled -> distanceGpsTravelled,
      arlasTrackDistanceGpsStraigthLine -> distanceGpsStraightLine,
      arlasTrackDistanceGpsStraigthness -> distanceGpsStraightness,
      arlasTrackDynamicsGpsSpeedKmh -> null,
      arlasTrackDynamicsGpsBearing -> null,
      arlasTrackPrefix + speedColumn -> null,
      arlasTrackDistanceSensorTravelled -> distanceSensorTravelled,
      arlasMovingStateColumn -> sortedRows.head.getAs[String](arlasMovingStateColumn)
    ) ++ ListMap(
      tempoProportionsColumns
        .map(t => (t._1, tempoProportions.getOrElse(t._2, -1.0)))
        .toSeq: _*) ++ ListMap(
      arlasCourseOrStopColumn -> sortedRows.head.getAs[String](arlasCourseOrStopColumn),
      arlasCourseStateColumn -> sortedRows.head.getAs[String](arlasCourseStateColumn),
      arlasMotionIdColumn -> null,
      arlasMotionDurationColumn -> null,
      arlasCourseIdColumn -> sortedRows.head.getAs[String](arlasCourseIdColumn),
      arlasCourseDurationColumn -> sortedRows.head.getAs[Long](arlasCourseDurationColumn),
      arlasTempoColumn -> mainTempo,
      arlasTrackTempoEmissionIsMulti -> isTempoEmissionMulti
    )

    val withAdditionalAggregationData = additionalAggregations(fragmentSummaryData, sortedRows)

    Seq(
      new GenericRowWithSchema(withAdditionalAggregationData.values.toArray,
                               getSchemaWithColumns(baseDF, aggregationColumns)))
  }

  def getSchemaWithColumns(baseDF: DataFrame, columns: Seq[StructField]) =
    columns.foldLeft(baseDF.schema) {
      case (schema, c) => if (schema.fieldNames.contains(c.name)) schema else schema.add(c)
    }

}
