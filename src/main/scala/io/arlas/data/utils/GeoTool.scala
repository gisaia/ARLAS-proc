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

package io.arlas.data.utils

import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{immutable, mutable}
import scala.math.{min, max}

object GeoTool {

  val LOCATION_DIGITS = 6 //required for coordinates with meter precision
  val LOCATION_PRECISION_DIGITS = 12
  val ELLIPSIS_DEFAULT_STANDARD_DEVIATION = Math.pow(10.0, -4.0)

  //instantiate some geotools objects as constant to avoid unnecessry memory footprint
  //these are supposed to be thread safe; to the contrary GeodeticCalculator isn't (so it is re-instantiated when needed)
  val GEOMETRY_FACTORY =
    new GeometryFactory(new PrecisionModel(Math.pow(10, LOCATION_DIGITS)), 4326)
  val WKT_READER = new WKTReader(GEOMETRY_FACTORY)
  val WKT_WRITER = new WKTWriter()
  val DEFAULT_SIMPLIFY_DISTANCE_TOLERANCE = 0.0002

  private val GEOHASH_BITS = Array(16, 8, 4, 2, 1)
  private val GEOHASH_BASE_32 =
    Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't',
      'u', 'v', 'w', 'x', 'y', 'z') //note: this is sorted

  /**
    * Compute track geometry WKT between 2 geopoints (LineString)
    */
  def getTrailBetween(prevLat: Double, prevLon: Double, lat: Double, lon: Double): Option[String] = {
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
  def getBearingBetween(prevLat: Double, prevLon: Double, lat: Double, lon: Double): Option[Double] = {
    val geodeticCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
    geodeticCalculator.setStartingGeographicPoint(prevLon, prevLat)
    geodeticCalculator.setDestinationGeographicPoint(lon, lat)
    //azimuth is between -180 and +180, but is expected between 0 and 360
    //unlike Python, scala keeps the sign of the dividend, we need to bypass it
    Some(((geodeticCalculator.getAzimuth % 360) + 360) % 360)
  }

  def getStandardDeviationEllipsis(latCenter: Double, lonCenter: Double, latStd: Double, lonStd: Double, nbPoints: Int) = {
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
    val geometry = GEOMETRY_FACTORY.createLineString(fCoords.toArray)
    Some(WKT_WRITER.write(geometry))
  }

  def getDistanceBetween(prevLat: Double, prevLon: Double, lat: Double, lon: Double): Option[Double] = {
    val geodeticCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
    geodeticCalculator.setStartingGeographicPoint(prevLon, prevLat)
    geodeticCalculator.setDestinationGeographicPoint(lon, lat)
    Some(geodeticCalculator.getOrthodromicDistance)
  }

  def getStraightLineDistanceFromTrails(trails: Array[String]): Option[Double] = {
    val nonNullTrails = trails.filterNot(_ == null)
    val geometries: Seq[Coordinate] = nonNullTrails.flatMap(WKT_READER.read(_).getCoordinates)
    if (geometries.size > 1) {
      getDistanceBetween(geometries.head.y, geometries.head.x, geometries.last.y, geometries.last.x)
    } else Some(0.0)
  }

  def wktToGeometry(wkt: String): Array[(Double, Double)] = {

    if (wkt == null || wkt.isEmpty) {
      Array()
    } else {

      val trailGeometry = WKT_READER.read(wkt)
      trailGeometry.getCoordinates.map(c => (c.y, c.x))
    }
  }

  def listOfCoordsToLineString(coords: Array[(Double, Double)]) = {
    if (coords.isEmpty) {
      None
    } else {
      val geometry =
        GEOMETRY_FACTORY.createLineString(coords.map(c => new Coordinate(c._1, c._2)))
      Some(WKT_WRITER.write(geometry))
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

  private def getTrailGeometryBetween(prevLat: Double, prevLon: Double, lat: Double, lon: Double): Geometry = {
    val start = new Coordinate(prevLon, prevLat)
    val end = new Coordinate(lon, lat)
    if (start.equals2D(end)) {
      GEOMETRY_FACTORY.createPoint(start)
    } else {
      GEOMETRY_FACTORY.createLineString(Array(start, end))
    }
  }

  def lineStringsToSingleMultiLineString(trails: Array[String]) = {
    if (trails.isEmpty) {
      None
    } else {
      val lineStrings: Seq[LineString] =
        trails.map(WKT_READER.read(_).getCoordinates).map(GEOMETRY_FACTORY.createLineString(_))
      val multiLineString = GEOMETRY_FACTORY.createMultiLineString(lineStrings.toArray)
      Some(WKT_WRITER.write(multiLineString))
    }
  }

  def getTrailDataFromTrailsAndCoords(trails: Array[String],
                                      latitudes: Array[Double],
                                      longitudes: Array[Double],
                                      useTrail: Array[Boolean]) = {

    if (useTrail.size != trails.size || useTrail.size != latitudes.size || useTrail.size != longitudes.size) {
      None
    } else {

      val coordinates: Seq[Coordinate] = useTrail.zipWithIndex.flatMap {
        case (state, index) => {
          if (state == true) WKT_READER.read(trails(index)).getCoordinates.toSeq
          //resume pauses to single points
          else Seq(new Coordinate(longitudes(index), latitudes(index)))
        }
      }
      val withoutConsecutiveDuplicates = removeConsecutiveDuplicatesCoords(coordinates.toList)

      val geometry =
        if (withoutConsecutiveDuplicates.size == 1)
          GEOMETRY_FACTORY.createPoint(withoutConsecutiveDuplicates(0))
        else GEOMETRY_FACTORY.createLineString(withoutConsecutiveDuplicates.toArray)

      val trail = WKT_WRITER.write(geometry)
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

  def groupTrailsByConsecutiveValue[T](expectedValue: T, values: Array[T], trails: Array[String]) = {

    if (values.size != trails.size) {
      None
    } else {

      val groupedTrails =
        groupConsecutiveValuesByCondition(expectedValue, Seq(values.zip(trails): _*))
      if (groupedTrails.isEmpty) None
      else {

        val lineStrings: Seq[LineString] = groupedTrails.map(g => {
          val coordinates = g.seq.flatMap(WKT_READER.read(_).getCoordinates)
          val withoutConsecutiveDuplicates = removeConsecutiveDuplicatesCoords(coordinates.toList)
          GEOMETRY_FACTORY.createLineString(
            if (withoutConsecutiveDuplicates.size == 1) //if single point, create linestring with 2 times the same coordinates
              Array(withoutConsecutiveDuplicates(0), withoutConsecutiveDuplicates(0))
            else withoutConsecutiveDuplicates.toArray)
        })
        val multiLineString = GEOMETRY_FACTORY.createMultiLineString(lineStrings.toArray)
        Some(WKT_WRITER.write(multiLineString))
      }
    }
  }

  def removeConsecutiveDuplicatesCoords(withDuplicatesList: List[Coordinate]): List[Coordinate] = {

    //the use of a ListBuffer instead of an immutable list + the use of a variable with last element, instead of checking the last list element,
    //have strong performance benefits
    var last: Coordinate = null
    withDuplicatesList
      .foldLeft(ListBuffer.empty[Coordinate]) {
        case (resList, coord) =>
          if (last == null || !last.equals2D(coord)) {
            last = coord
            resList += coord
          }
          resList
      }
      .toList
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

  def simplifyGeometry(geometry: String, distanceTolerance: Option[Double]): Option[String] = {
    if (geometry == null || geometry == "") {
      None

    } else {
      val simplified =
        DouglasPeuckerSimplifier.simplify(WKT_READER.read(geometry), distanceTolerance.getOrElse(DEFAULT_SIMPLIFY_DISTANCE_TOLERANCE))

      Some(WKT_WRITER.write(simplified))
    }
  }

  def scaleDouble(double: Double, scale: Int) =
    BigDecimal(double).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

  case class TrailData(trail: String, departureLat: Double, departureLon: Double, arrivalLat: Double, arrivalLon: Double)

  def fixAntimeridianCrossingGeometries[T](trail: String) = {
    // Take a wkt geometry and apply the antimeridian checking if its a LineString

    //    val coordinates = WKT_READER.read(trail).getCoordinates.toList
    // TODO: Test if the the linestring is a candidate (cross antimeridian) to avoid parse all geometries?

    var returnedTrail: Option[String] = null
    if (WKT_READER.read(trail).getGeometryType == "LineString") {
      returnedTrail = splitLinestringAntimeridian(trail)
    } else {
      returnedTrail = Some(trail)
    }
    returnedTrail
  }

  def splitLinestringAntimeridian(trail: String): Option[String] = {
    // Take a wkt LineString and transform it into a continuous wkt MultiLineString when it cross the antimeridian

    //the use of a ListBuffer instead of an immutable list + the use of a variable with last element, instead of checking the last list element,
    //have strong performance benefits

    val originalCoordinateList = WKT_READER.read(trail).getCoordinates.toList

    var prevCoord: Coordinate = null

    var oldX: Double = 0
    var X: Double = 0
    var joinY: Double = 0

    var linestringList: ListBuffer[String] = ListBuffer.empty[String]

    val lastCorrectedCoordinateList = originalCoordinateList
      .foldLeft(ListBuffer.empty[Coordinate]) {
        case (currentCoordsList, coord) =>
          val minX = if (prevCoord == null) { 0 } else { min(coord.x, prevCoord.x) }
          val maxX = if (prevCoord == null) { 0 } else { max(coord.x, prevCoord.x) }
          if (prevCoord == null || ((maxX - minX).abs < (maxX - (minX + 360)).abs)) {
            // Normal Case: coords are added to the current linestring
            prevCoord = coord
            currentCoordsList += coord
          } else {
            // The antimeridian is crossed, the linestring is split in two linestrings linked by their join point
            var firstJoinPoint: Coordinate = new Coordinate()
            var secondJoinPoint: Coordinate = new Coordinate()

            // The order of the Join point depend on the way the antimeridian is crossed (from west or east)
            val isByLeftCrossing = coord.x < 0
            if (isByLeftCrossing) {
              oldX = prevCoord.x
              X = coord.x + 360
              firstJoinPoint.x = 180.0
              secondJoinPoint.x = -180.0
            } else {
              oldX = prevCoord.x + 360
              X = coord.x
              firstJoinPoint.x = -180.0
              secondJoinPoint.x = 180.0
            }
            // The latitude of the Join points (identical) is computed by linear interpolation
            joinY = prevCoord.y + (180 - oldX) / (X - oldX) * (coord.y - prevCoord.y)
            firstJoinPoint.y = joinY
            secondJoinPoint.y = joinY

            // The current linestring ends with the correct join point and is stored in the linestring list
            currentCoordsList += firstJoinPoint
            linestringList += WKT_WRITER.write(GEOMETRY_FACTORY.createLineString(currentCoordsList.toArray))

            // A new linestring is initialized and linked to the previous by the correct joint point
            currentCoordsList.clear()
            currentCoordsList += secondJoinPoint
            currentCoordsList += coord
            prevCoord = coord
          }
          currentCoordsList
      }
      .toList

    // The last Linestring is added to the list
    linestringList += WKT_WRITER.write(GEOMETRY_FACTORY.createLineString(lastCorrectedCoordinateList.toArray))

    // The list of Linestrings is merged in a multilinestring object which is returned
    if (linestringList.size == 1) {
      Some(linestringList.head)
    } else {
      lineStringsToSingleMultiLineString(linestringList.toArray)
    }

  }
}
