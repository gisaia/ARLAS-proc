package io.arlas.data.transform.timeseries

import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.arlas.data.sql.ArlasDataFrame
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import scala.collection.immutable.ListMap

class WithTraversingMissionTest extends ArlasTest {

  val sensorDistanceCol = "sensor_distance"
  val gpsDistanceCol = "gps_distance"

  val addressDF = createDataFrameWithTypes(
    spark,
    List(
      Seq(43.5437, 1.3666, "Occitanie", "31120", "Muret", "France", "fr", "Portet-sur-Garonne"),
      Seq(43.536994, 1.345754, "Occitanie", "31270", "Toulouse", "France", "fr", "Cugnaux"),
      Seq(43.516101, 1.322899, "Occitanie", "31270", "Muret", "France", "fr", "Frouzins"),
      Seq(43.459986, 1.326233, "Occitanie", "31600", "Muret", "France", "fr", "Muret")
    ),
    ListMap(
      "location_lat" -> (DoubleType, true),
      "location_lon" -> (DoubleType, true),
      "address_state" -> (StringType, true),
      "address_postcode" -> (StringType, true),
      "address_county" -> (StringType, true),
      "address_country" -> (StringType, true),
      "address_country_code" -> (StringType, true),
      "address_city" -> (StringType, true)
    )
  )

  def prefixAddressFieldsWith(prefix: String) =
    spark.createDataFrame(addressDF.rdd, StructType(addressDF.schema.fields.map(f => StructField(prefix + f.name, f.dataType, f.nullable))))

  val departureAddressesDF = prefixAddressFieldsWith(arlasDeparturePrefix)
  val arrivalAddressesDF = prefixAddressFieldsWith(arlasArrivalPrefix)
  val missionDepartureAddressesDF = prefixAddressFieldsWith(arlasMissionDeparturePrefix)
  val missionArrivalAddressesDF = prefixAddressFieldsWith(arlasMissionArrivalPrefix)

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      //new mission
      Seq("id1", 0l, 20l, 10000.0, 15000.0, 20000l, "Portet-sur-Garonne", "Cugnaux", "id1#0_50", "Portet-sur-Garonne", "Muret"),
      Seq("id1", 20l, 30l, 15000.0, 10000.0, 25000l, "Cugnaux", "Frouzins", "id1#0_50", "Portet-sur-Garonne", "Muret"),
      Seq("id1", 40l, 50l, 20000.0, 10000.0, 40000l, "Frouzins", "Muret", "id1#0_50", "Portet-sur-Garonne", "Muret"),
      //new mission
      Seq("id1", 50l, 60l, 12000.0, 14000.0, 20000l, "Muret", "Frouzins", "id1#50_70", "Muret", "Portet-sur-Garonne"),
      Seq("id1", 60l, 70l, 14000.0, 12000.0, 10000l, "Frouzins", "Portet-sur-Garonne", "id1#50_70", "Muret", "Portet-sur-Garonne"),
      //new mission
      Seq("id1", 70l, 80l, 10000.0, 14000.0, 50000l, "Portet-sur-Garonne", "Cugnaux", "id1#70_90", "Portet-sur-Garonne", "Frouzins"),
      Seq("id1", 80l, 90l, 15000.0, 13000.0, 100000l, "Cugnaux", "Frouzins", "id1#70_90", "Portet-sur-Garonne", "Frouzins"),
      //new mission
      Seq("id2", 0l, 50l, 14000.0, 14000.0, 22000l, "Frouzins", "Portet-sur-Garonne", "id2#0_50", "Frouzins", "Portet-sur-Garonne"),
      //new mission
      Seq("id3", 0l, 20l, 11000.0, 14000.0, 34000l, "Portet-sur-Garonne", "Cugnaux", "id3#0_20", "Portet-sur-Garonne", "Cugnaux"),
      //new mission
      Seq("id3", 20l, 40l, 9000.0, 11000.0, 200000l, "Cugnaux", "Portet-sur-Garonne", "id3#20_40", "Cugnaux", "Portet-sur-Garonne")
    ),
    ListMap(
      "id" -> (StringType, true),
      arlasTrackTimestampStart -> (LongType, true),
      arlasTrackTimestampEnd -> (LongType, true),
      sensorDistanceCol -> (DoubleType, true),
      gpsDistanceCol -> (DoubleType, true),
      arlasCourseDurationColumn -> (LongType, true),
      arlasDepartureAddressCity -> (StringType, true),
      arlasArrivalAddressCity -> (StringType, true),
      "expected_mission_id" -> (StringType, true),
      "expected_mission_departure_city" -> (StringType, true),
      "expected_mission_arrival_city" -> (StringType, true)
    )
  ).join(departureAddressesDF, arlasDepartureAddressCity)
    .join(arrivalAddressesDF, arlasArrivalAddressCity)

  def getDistanceUDF =
    udf((prevLat: Double, prevLon: Double, lat: Double, lon: Double) => GeoTool.getDistanceBetween(prevLat, prevLon, lat, lon))

  "WithTraversingMission" should "compute the mission data against a dataframe' timeserie " in {

    val expectedDF: DataFrame = getExpectedDF
    val transformedDF =
      testDF.enrichWithArlas(new WithTraversingMission(spark, dataModel, sensorDistanceCol, gpsDistanceCol))

    val comparisonColumns =
      Seq("id", arlasTrackTimestampStart, arlasTrackTimestampEnd) ++ expectedDF.columns.filter(_.contains("mission"))

    assertDataFrameEquality(
      transformedDF.select(comparisonColumns.map(col(_)): _*),
      expectedDF.select(comparisonColumns.map(col(_)): _*)
    )
  }

  private def getExpectedDF = {
    val window =
      Window
        .partitionBy(arlasMissionId)
        .orderBy(arlasTrackTimestampStart)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val expectedDF = testDF
      .withColumnRenamed("expected_mission_id", arlasMissionId)
      .join(missionDepartureAddressesDF, col("expected_mission_departure_city").equalTo(col(arlasMissionDepartureAddressCity)))
      .join(missionArrivalAddressesDF, col("expected_mission_arrival_city").equalTo(col(arlasMissionArrivalAddressCity)))
      .withColumn(arlasMissionDuration, sum(arlasCourseDurationColumn).over(window))
      .withColumn(arlasMissionDistanceSensorTravelled, sum(sensorDistanceCol).over(window))
      .withColumn(arlasMissionDistanceGpsTravelled, sum(gpsDistanceCol).over(window))
      .withColumn(
        arlasMissionDistanceGpsStraigthline,
        getDistanceUDF(col(arlasMissionDepartureLocationLat),
                       col(arlasMissionDepartureLocationLon),
                       col(arlasMissionArrivalLocationLat),
                       col(arlasMissionArrivalLocationLon))
      )
      .withColumn(arlasMissionDistanceGpsStraigthness, col(arlasMissionDistanceGpsStraigthline) / col(arlasMissionDistanceGpsTravelled))
      .withColumn(arlasMissionDepartureTimestamp, first(arlasTrackTimestampStart).over(window))
      .withColumn(arlasMissionArrivalTimestamp, last(arlasTrackTimestampEnd).over(window))
    expectedDF
  }
}
