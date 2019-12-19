package io.arlas.data.transform.timeseries

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * Compute the columns related to a mission
  * The principle of a mission is:
  * - from a start row, compute the distance between departure and arrival
  * - consider the next row, and compute the distance between first row's departure and second row's arrival
  * - if this distance is greater than first distance, do the same with the third row
  * - otherwise the mission is over. As a consequence start a new mission, and apply the same algorithm again...
  * @param spark
  * @param dataModel
  */
class WithTraversingMission(spark: SparkSession, dataModel: DataModel, sensorDistanceCol: String, gpsDistanceCol: String)
    extends ArlasTransformer(
      Vector(
        sensorDistanceCol,
        gpsDistanceCol,
        arlasTrackTimestampStart,
        arlasTrackTimestampEnd,
        arlasDepartureLocationLat,
        arlasDepartureLocationLon,
        arlasDepartureAddressState,
        arlasDepartureAddressPostcode,
        arlasDepartureAddressCounty,
        arlasDepartureAddressCountry,
        arlasDepartureAddressCountryCode,
        arlasDepartureAddressCity,
        arlasArrivalLocationLat,
        arlasArrivalLocationLon,
        arlasArrivalAddressState,
        arlasArrivalAddressPostcode,
        arlasArrivalAddressCounty,
        arlasArrivalAddressCountry,
        arlasArrivalAddressCountryCode,
        arlasArrivalAddressCity,
        arlasCourseDurationColumn
      )) {

  //some defaults to return if a key isn't found into a map. However from the transformer's required columns we are sure they are there
  val UNKNOWN_STRING = "unknown"
  val UNKNOWN_LONG = 0l
  val UNKNOWN_DOUBLE = 0.0

  override def transform(dataset: Dataset[_]): DataFrame = {

    import spark.implicits._
    val columns = dataset.columns
    val newSchema = transformSchema(dataset.schema)
    val newColumns = newSchema.fields.map(_.name)

    val interpolatedRows: RDD[Row] = dataset
      .toDF()
      .map(row => (row.getString(row.fieldIndex(dataModel.idColumn)), List(row.getValuesMap(columns))))
      .rdd
      .reduceByKey(_ ++ _)
      .flatMap {
        case (_, timeserie: List[Map[String, Any]]) => {
          val sortedTimeserie =
            timeserie.sortBy(_.getOrElse(arlasTrackTimestampStart, UNKNOWN_LONG))

          //first compute the initial distance between first row's departure and arrival
          val startRow = sortedTimeserie.head
          var missionStartLat = startRow.getOrElse(arlasDepartureLocationLat, UNKNOWN_DOUBLE)
          var missionStartLon = startRow.getOrElse(arlasDepartureLocationLon, UNKNOWN_DOUBLE)

          //ids of rows that start a mission
          val missionsIndices: ListBuffer[Int] = ListBuffer(0)
          val missionDistances: ListBuffer[Double] = ListBuffer()
          var recMissionDistance = GeoTool
            .getDistanceBetween(missionStartLat,
                                missionStartLon,
                                startRow.getOrElse(arlasArrivalLocationLat, UNKNOWN_DOUBLE),
                                startRow.getOrElse(arlasArrivalLocationLon, UNKNOWN_DOUBLE))
            .getOrElse(UNKNOWN_DOUBLE)

          //check each next row. Each time a new mission is started, the row index is added to a List
          sortedTimeserie
            .drop(0)
            .zipWithIndex
            .foreach {
              case (timeserie, index) => {
                val endLat = timeserie.getOrElse(arlasArrivalLocationLat, UNKNOWN_DOUBLE)
                val endLon = timeserie.getOrElse(arlasArrivalLocationLon, UNKNOWN_DOUBLE)

                val curDistance: Option[Double] = GeoTool
                  .getDistanceBetween(missionStartLat, missionStartLon, endLat, endLon)

                if (curDistance.getOrElse(UNKNOWN_DOUBLE) < recMissionDistance) {
                  missionsIndices.append(index)
                  missionDistances.append(recMissionDistance)
                  missionStartLat = timeserie.getOrElse(arlasDepartureLocationLat, UNKNOWN_DOUBLE)
                  missionStartLon = timeserie.getOrElse(arlasDepartureLocationLon, UNKNOWN_DOUBLE)
                  recMissionDistance = GeoTool
                    .getDistanceBetween(
                      missionStartLat,
                      missionStartLon,
                      timeserie.getOrElse(arlasArrivalLocationLat, UNKNOWN_DOUBLE),
                      timeserie.getOrElse(arlasArrivalLocationLon, UNKNOWN_DOUBLE)
                    )
                    .getOrElse(UNKNOWN_DOUBLE)
                } else {
                  recMissionDistance = curDistance.getOrElse(UNKNOWN_DOUBLE)
                }
              }
            }

          //append a fake index, this will help use to browse the last element of mission indices list (because using a 2 sliding)
          missionsIndices.append(timeserie.size)
          //append distance computed at last point, which is the ending point of the last mission
          missionDistances.append(recMissionDistance)

          //browse the mission list. With mission indices, it gives us the first and last mission row.
          //There we can be build the mission data and add it to each row
          missionsIndices
            .sliding(2)
            .zipWithIndex
            .flatMap {
              case (ListBuffer(a, b), index) => {
                val missionTimeseries = sortedTimeserie.slice(a, b)
                val firstRow = missionTimeseries.head
                val lastRow = missionTimeseries.last
                val missionStartTs = firstRow.getOrElse(arlasTrackTimestampStart, UNKNOWN_LONG)
                val missionEndTs = lastRow.getOrElse(arlasTrackTimestampEnd, UNKNOWN_LONG)
                val missionGpsDistance =
                  missionTimeseries.map(_.getOrElse(gpsDistanceCol, UNKNOWN_DOUBLE)).sum
                val objectId = firstRow.getOrElse(dataModel.idColumn, UNKNOWN_STRING)

                val missionData = Map(
                  arlasMissionId -> s"${objectId}#${missionStartTs}_${missionEndTs}",
                  arlasMissionDuration -> missionTimeseries
                    .map(_.getOrElse(arlasCourseDurationColumn, UNKNOWN_LONG))
                    .sum,
                  arlasMissionDistanceSensorTravelled -> missionTimeseries
                    .map(_.getOrElse(sensorDistanceCol, UNKNOWN_DOUBLE))
                    .sum,
                  arlasMissionDistanceGpsTravelled -> missionGpsDistance,
                  arlasMissionDistanceGpsStraigthline -> missionDistances(index),
                  arlasMissionDistanceGpsStraigthness -> missionDistances(index) / missionGpsDistance,
                  arlasMissionDepartureLocationLat -> firstRow.getOrElse(arlasDepartureLocationLat, UNKNOWN_DOUBLE),
                  arlasMissionDepartureLocationLon -> firstRow.getOrElse(arlasDepartureLocationLon, UNKNOWN_DOUBLE),
                  arlasMissionDepartureTimestamp -> firstRow.getOrElse(arlasTrackTimestampStart, UNKNOWN_LONG),
                  arlasMissionDepartureAddressCountryCode -> firstRow
                    .getOrElse(arlasDepartureAddressCountryCode, UNKNOWN_STRING),
                  arlasMissionDepartureAddressCountry -> firstRow
                    .getOrElse(arlasDepartureAddressCountry, UNKNOWN_STRING),
                  arlasMissionDepartureAddressCity -> firstRow.getOrElse(arlasDepartureAddressCity, UNKNOWN_STRING),
                  arlasMissionDepartureAddressCounty -> firstRow
                    .getOrElse(arlasDepartureAddressCounty, UNKNOWN_STRING),
                  arlasMissionDepartureAddressState -> firstRow
                    .getOrElse(arlasDepartureAddressState, UNKNOWN_STRING),
                  arlasMissionDepartureAddressPostcode -> firstRow
                    .getOrElse(arlasDepartureAddressPostcode, UNKNOWN_STRING),
                  arlasMissionArrivalLocationLat -> lastRow.getOrElse(arlasArrivalLocationLat, UNKNOWN_DOUBLE),
                  arlasMissionArrivalLocationLon -> lastRow.getOrElse(arlasArrivalLocationLon, UNKNOWN_DOUBLE),
                  arlasMissionArrivalLocationTimestamp -> lastRow.getOrElse(arlasTrackTimestampEnd, UNKNOWN_LONG),
                  arlasMissionArrivalAddressCountryCode -> lastRow
                    .getOrElse(arlasArrivalAddressCountryCode, UNKNOWN_STRING),
                  arlasMissionArrivalAddressCountry -> lastRow.getOrElse(arlasArrivalAddressCountry, UNKNOWN_STRING),
                  arlasMissionArrivalAddressCity -> lastRow.getOrElse(arlasArrivalAddressCity, UNKNOWN_STRING),
                  arlasMissionArrivalAddressCounty -> lastRow.getOrElse(arlasArrivalAddressCounty, UNKNOWN_STRING),
                  arlasMissionArrivalAddressState -> lastRow.getOrElse(arlasArrivalAddressState, UNKNOWN_STRING),
                  arlasMissionArrivalAddressPostcode -> lastRow
                    .getOrElse(arlasArrivalAddressPostcode, UNKNOWN_STRING)
                )
                missionTimeseries.map(_ ++ missionData)
              }
            }
        }
      }
      .map((entry: Map[String, Any]) => Row.fromSeq(newColumns.map(entry.getOrElse(_, null)).toSeq))

    spark.sqlContext.createDataFrame(interpolatedRows, newSchema)

  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasMissionId, StringType, true))
      .add(StructField(arlasMissionDuration, LongType, true))
      .add(StructField(arlasMissionDistanceSensorTravelled, DoubleType, true))
      .add(StructField(arlasMissionDistanceGpsTravelled, DoubleType, true))
      .add(StructField(arlasMissionDistanceGpsStraigthline, DoubleType, true))
      .add(StructField(arlasMissionDistanceGpsStraigthness, DoubleType, true))
      .add(StructField(arlasMissionDepartureLocationLat, DoubleType, true))
      .add(StructField(arlasMissionDepartureLocationLon, DoubleType, true))
      .add(StructField(arlasMissionDepartureTimestamp, LongType, true))
      .add(StructField(arlasMissionDepartureAddressCountryCode, StringType, true))
      .add(StructField(arlasMissionDepartureAddressCountry, StringType, true))
      .add(StructField(arlasMissionDepartureAddressCity, StringType, true))
      .add(StructField(arlasMissionDepartureAddressCounty, StringType, true))
      .add(StructField(arlasMissionDepartureAddressState, StringType, true))
      .add(StructField(arlasMissionDepartureAddressPostcode, StringType, true))
      .add(StructField(arlasMissionArrivalLocationLat, DoubleType, true))
      .add(StructField(arlasMissionArrivalLocationLon, DoubleType, true))
      .add(StructField(arlasMissionArrivalLocationTimestamp, LongType, true))
      .add(StructField(arlasMissionArrivalAddressCountryCode, StringType, true))
      .add(StructField(arlasMissionArrivalAddressCountry, StringType, true))
      .add(StructField(arlasMissionArrivalAddressCity, StringType, true))
      .add(StructField(arlasMissionArrivalAddressCounty, StringType, true))
      .add(StructField(arlasMissionArrivalAddressState, StringType, true))
      .add(StructField(arlasMissionArrivalAddressPostcode, StringType, true))
  }
}
