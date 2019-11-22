package io.arlas.data.utils

import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString, PrecisionModel}
import org.locationtech.jts.io.{WKTReader, WKTWriter}

import org.slf4j.LoggerFactory
import scala.collection.immutable

object GeoTool {

  val LOCATION_DIGITS = 6 //required for coordinates with meter precision
  val LOCATION_PRECISION_DIGITS = 12
  val ELLIPSIS_DEFAULT_STANDARD_DEVIATION = Math.pow(10.0, -4.0)

  private val GEOHASH_BITS = Array(16, 8, 4, 2, 1)
  private val GEOHASH_BASE_32 =
    Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j',
      'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z') //note: this is sorted

  /**
    * Compute track geometry WKT between 2 geopoints (LineString)
    */
  def getTrailBetween(prevLat: Double,
                      prevLon: Double,
                      lat: Double,
                      lon: Double): Option[String] = {
    Some((new WKTWriter()).write(getTrailGeometryBetween(prevLat, prevLon, lat, lon)))
  }

  /**
    * Compute the bearing between 2 geopoints, always positive
    * @param prevLat
    * @param prevLon
    * @param lat
    * @param lon
    * @return
    */
  def getBearingBetween(prevLat: Double,
                        prevLon: Double,
                        lat: Double,
                        lon: Double): Option[Double] = {
    val geodesicCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
    geodesicCalculator.setStartingGeographicPoint(prevLon, prevLat)
    geodesicCalculator.setDestinationGeographicPoint(lon, lat)
    //azimuth is between -180 and +180, but is expected between 0 and 360
    //unlike Python, scala keeps the sign of the dividend, we need to bypass it
    Some(((geodesicCalculator.getAzimuth % 360) + 360) % 360)
  }

  def getStandardDeviationEllipsis(latCenter: Double,
                                   lonCenter: Double,
                                   latStd: Double,
                                   lonStd: Double,
                                   nbPoints: Int) = {
    val deltaTeta = 2 * Math.PI / nbPoints

    //avoid an ellipsis with all points at same position
    val latStdNotNull = if (latStd == 0) ELLIPSIS_DEFAULT_STANDARD_DEVIATION else latStd
    val lonStdNotNull = if (lonStd == 0) ELLIPSIS_DEFAULT_STANDARD_DEVIATION else lonStd

    val coords: immutable.Seq[Coordinate] =
      (0 to (nbPoints - 1)).map(i => {
        val thetaLat = latCenter + latStdNotNull * Math.sin(i * deltaTeta)
        val thetaLon = lonCenter + lonStdNotNull * Math.cos(i * deltaTeta)
        new Coordinate(thetaLon, thetaLat)
      })
    val fCoords = coords :+ coords(0) //add first point at the end
    val geometry = getNewGeometryFactory().createLineString(fCoords.toArray)
    Some(new WKTWriter().write(geometry))
  }

  def getDistanceBetween(prevLat: Double,
                         prevLon: Double,
                         lat: Double,
                         lon: Double): Option[Double] = {
    val geodesicCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
    geodesicCalculator.setStartingGeographicPoint(prevLon, prevLat)
    geodesicCalculator.setDestinationGeographicPoint(lon, lat)
    Some(geodesicCalculator.getOrthodromicDistance)
  }

  def getStraightLineDistanceFromTrails(trails: Array[String]): Option[Double] = {
    val nonNullTrails = trails.filterNot(_ == null)
    val reader = new WKTReader()
    val geometries: Seq[Coordinate] = nonNullTrails.flatMap(reader.read(_).getCoordinates)
    if (geometries.size > 1) {
      getDistanceBetween(geometries.head.y, geometries.head.x, geometries.last.y, geometries.last.x)
    } else Some(0.0)
  }

  def wktToGeometry(wkt: String): Array[(Double, Double)] = {

    if (wkt == null || wkt.isEmpty) {
      Array()
    } else {
      val factory = getNewGeometryFactory
      val reader = new WKTReader(factory)
      val trailGeometry = reader.read(wkt)
      trailGeometry.getCoordinates.map(c => (c.y, c.x))
    }
  }

  def listOfCoordsToLineString(coords: Array[(Double, Double)]) = {
    if (coords.isEmpty) {
      None
    } else {
      val geometry =
        getNewGeometryFactory().createLineString(coords.map(c => new Coordinate(c._1, c._2)))
      Some(new WKTWriter().write(geometry))
    }
  }

  /**
    * This is the spatial4j implementation (apache 2.0 licence), translated to Scala
    * @param latitude
    * @param longitude
    * @param precision
    * @return
    */
  def getGeohashFrom(latitude: Double, longitude: Double, precision: Int) = {
    val latInterval = Array(-90.0, 90.0)
    val lngInterval = Array(-180.0, 180.0)

    val geohash = new StringBuilder(precision)
    var isEven = true

    var bit = 0
    var ch = 0

    while ({
      geohash.length < precision
    }) {
      var mid = 0.0
      if (isEven) {
        mid = (lngInterval(0) + lngInterval(1)) / 2D
        if (longitude > mid) {
          ch |= GEOHASH_BITS(bit)
          lngInterval(0) = mid
        } else lngInterval(1) = mid
      } else {
        mid = (latInterval(0) + latInterval(1)) / 2D
        if (latitude > mid) {
          ch |= GEOHASH_BITS(bit)
          latInterval(0) = mid
        } else latInterval(1) = mid
      }
      isEven = !isEven
      if (bit < 4) bit += 1
      else {
        geohash.append(GEOHASH_BASE_32(ch))
        bit = 0
        ch = 0
      }
    }

    geohash.toString
  }

  private def getTrailGeometryBetween(prevLat: Double,
                                      prevLon: Double,
                                      lat: Double,
                                      lon: Double): Geometry = {
    val start = new Coordinate(prevLon, prevLat)
    val end = new Coordinate(lon, lat)
    if (start.equals2D(end)) {
      getNewGeometryFactory().createPoint(start)
    } else {
      getNewGeometryFactory().createLineString(Array(start, end))
    }
  }

  def lineStringsToSingleMultiLineString(trails: Array[String]) = {
    if (trails.isEmpty) {
      None
    } else {
      val factory = getNewGeometryFactory
      val reader = new WKTReader(factory)
      val lineStrings: Seq[LineString] =
        trails.map(reader.read(_).getCoordinates).map(factory.createLineString(_))
      val multiLineString = factory.createMultiLineString(lineStrings.toArray)
      Some(new WKTWriter().write(multiLineString))
    }
  }

  def getTrailDataFromTrailsAndCoords(trails: Array[String],
                                      latitudes: Array[Double],
                                      longitudes: Array[Double],
                                      useTrail: Array[Boolean]) = {

    if (useTrail.size != trails.size || useTrail.size != latitudes.size || useTrail.size != longitudes.size) {
      None
    } else {
      val factory = getNewGeometryFactory()
      val reader = new WKTReader(factory)

      val coordinates: Seq[Coordinate] = useTrail.zipWithIndex.flatMap {
        case (state, index) => {
          if (state == true) reader.read(trails(index)).getCoordinates.toSeq
          //resume pauses to single points
          else Seq(new Coordinate(longitudes(index), latitudes(index)))
        }
      }
      val withoutConsecutiveDuplicates = removeConsecutiveDuplicatesCoords(coordinates.toList)

      val geometry =
        if (withoutConsecutiveDuplicates.size == 1)
          factory.createPoint(withoutConsecutiveDuplicates(0))
        else factory.createLineString(withoutConsecutiveDuplicates.toArray)

      val trail = new WKTWriter().write(geometry)
      val departure = geometry.getCoordinates().head
      val arrival = geometry.getCoordinates().last

      Some(
        TrailData(
          trail,
          scaleDouble(departure.getY, LOCATION_DIGITS),
          scaleDouble(departure.getX, LOCATION_DIGITS),
          scaleDouble(arrival.getY, LOCATION_DIGITS),
          scaleDouble(arrival.getX, LOCATION_DIGITS)
        ))
    }
  }

  def groupTrailsByConsecutiveValue[T](expectedValue: T,
                                       values: Array[T],
                                       trails: Array[String]) = {

    if (values.size != trails.size) {
      None
    } else {

      val groupedTrails =
        groupConsecutiveValuesByCondition(expectedValue, Seq(values.zip(trails): _*))
      if (groupedTrails.isEmpty) None
      else {
        val factory = getNewGeometryFactory()
        val reader = new WKTReader(factory)

        val lineStrings: Seq[LineString] = groupedTrails.map(g => {
          val coordinates = g.seq.flatMap(reader.read(_).getCoordinates)
          val withoutConsecutiveDuplicates = removeConsecutiveDuplicatesCoords(coordinates.toList)
          factory.createLineString(
            if (withoutConsecutiveDuplicates.size == 1) //if single point, create linestring with 2 times the same coordinates
              Array(withoutConsecutiveDuplicates(0), withoutConsecutiveDuplicates(0))
            else withoutConsecutiveDuplicates.toArray)
        })
        val multiLineString = factory.createMultiLineString(lineStrings.toArray)
        Some(new WKTWriter().write(multiLineString))
      }
    }
  }

  def removeConsecutiveDuplicatesCoords(withDuplicatesList: List[Coordinate]): List[Coordinate] = {

    withDuplicatesList match {
      case head :: _ => {
        val (_, remainlst) = withDuplicatesList.span(_.equals2D(head))
        head :: removeConsecutiveDuplicatesCoords(remainlst)
      }
      case Nil => List()
    }
  }

  def groupConsecutiveValuesByCondition[T, R](conditionalValue: T,
                                              values: Seq[(T, R)],
                                              acc: Seq[R] = Seq(),
                                              result: Seq[Seq[R]] = Seq()): Seq[Seq[R]] = {

    lazy val resultWithAcc = if (acc.nonEmpty) result :+ acc else result

    values match {
      case head :: tail =>
        val (currentConditionValue, currentValue) = head
        if (currentConditionValue == conditionalValue)
          groupConsecutiveValuesByCondition(conditionalValue, tail, acc :+ currentValue, result)
        else
          groupConsecutiveValuesByCondition(conditionalValue, tail, List(), resultWithAcc)
      case _ => resultWithAcc
    }
  }

  private def getNewGeometryFactory() =
    new GeometryFactory(new PrecisionModel(Math.pow(10, LOCATION_DIGITS)), 4326)

  def scaleDouble(double: Double, scale: Int) =
    BigDecimal(double).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

  case class TrailData(trail: String,
                       departureLat: Double,
                       departureLon: Double,
                       arrivalLat: Double,
                       arrivalLon: Double)

}
