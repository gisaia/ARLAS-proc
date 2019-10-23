package io.arlas.data.utils

import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.WKTWriter
import scala.collection.immutable

object GeoTool {

  val coordinatesDecimalPrecision = 6 //required for coordinates with meter precision

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
    val latStdNotNull = if (latStd == 0) 1 else latStd
    val lonStdNotNull = if (lonStd == 0) 1 else lonStd

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

  private def getNewGeometryFactory() =
    new GeometryFactory(new PrecisionModel(Math.pow(10, coordinatesDecimalPrecision)), 4326)

}
