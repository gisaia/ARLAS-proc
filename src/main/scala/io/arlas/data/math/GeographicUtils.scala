package io.arlas.data.math

import io.arlas.data.model.DataModel
import net.sf.geographiclib.{Geodesic, GeodesicData};

object GeographicUtils {

  def sequenceToLocalCoordinates(
                                  orderedPointsSequence: interpolations.Sequence,
                                  latitudeColumn: String,
                                  longitudeColumn: String): (interpolations.Sequence, Double, Double) = {

    val firstLat: Double =
      orderedPointsSequence(0)
        .getOrElse(latitudeColumn, 0d)
        .asInstanceOf[Double]

    val firstLong: Double = orderedPointsSequence(0)
      .getOrElse(longitudeColumn, 0d)
      .asInstanceOf[Double]

    val localPoints = orderedPointsSequence
      .map(point => {

        val geodesicData: GeodesicData =
          Geodesic.WGS84.Inverse(firstLat,
                                 firstLong,
                                 point.getOrElse(latitudeColumn, 0d).asInstanceOf[Double],
                                 point.getOrElse(longitudeColumn, 0d).asInstanceOf[Double])

        val distance: Double = geodesicData.s12
        val angle: Double = Math.toRadians(90 - geodesicData.azi1)

        val localLatitude = distance * Math.sin(angle)
        val localLongitude = distance * Math.cos(angle)

        point + (latitudeColumn -> localLatitude) + (longitudeColumn -> localLongitude)
      })
      .asInstanceOf[interpolations.Sequence]

    (localPoints, firstLat, firstLong)
  }

  def sequenceFromLocalCoordinates(localPoints: interpolations.Sequence,
                                   dataModel: DataModel,
                                   firstLatitude: Double,
                                   firstLongitude: Double): interpolations.Sequence = {

    localPoints
      .map(p => {

        val localLongitude =
          p.getOrElse(dataModel.lonColumn, 0d).asInstanceOf[Double]

        val localLatitude =
          p.getOrElse(dataModel.latColumn, 0d).asInstanceOf[Double]

        val distance = Math.hypot(localLongitude, localLatitude)
        val bearing = 90 - Math.toDegrees(Math.atan2(localLatitude, localLongitude))

        val geodesicData =
          Geodesic.WGS84
            .Direct(firstLatitude, firstLongitude, bearing, distance)

        p + (dataModel.lonColumn -> geodesicData.lon2) + (dataModel.latColumn -> geodesicData.lat2)
      })
      .asInstanceOf[interpolations.Sequence]

  }

}
