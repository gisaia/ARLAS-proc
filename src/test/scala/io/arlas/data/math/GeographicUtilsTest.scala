package io.arlas.data.math

import io.arlas.data.model.DataModel
import org.scalatest.FlatSpec

class GeographicUtilsTest extends FlatSpec {

  behavior of "geographicUtils"

  it should "translate coordinates into the local plan" in {

    val pointsSequence: interpolations.Sequence = List(
      Map("lon" -> 47d, "lat" -> 15d),
      Map("lon" -> 48d, "lat" -> 14d)
      )

    val (result: interpolations.Sequence, firstLat: Double, firstLon: Double) =
      GeographicUtils.sequenceToLocalCoordinates(pointsSequence, "lat", "lon")

    assert(result(0).getOrElse("lat", -1d) == 0d)
    assert(result(0).getOrElse("lon", -1d) == 0d)

    val distance = MathUtils.distance(
      result(0).getOrElse("lon", -1d).asInstanceOf[Double],
      result(1).getOrElse("lon", -1d).asInstanceOf[Double],
      result(0).getOrElse("lat", -1d).asInstanceOf[Double],
      result(1).getOrElse("lat", -1d).asInstanceOf[Double]
      )

    //expected distance from http://www.cqsrg.org/tools/GCDistance/
    assert(MathUtils.~=(distance, 154472.51790021235, 0.000001))
  }

  it should "translate coordinates from the local plan" in {

    val dataModel = DataModel()
    val localPoints: interpolations.Sequence = List(
      Map("lon" -> 108039.09859249026, "lat" -> -110405.21709612322)
      )

    val result: interpolations.Sequence =
      GeographicUtils.sequenceFromLocalCoordinates(localPoints, dataModel, 15d, 47d)

    assert(
      MathUtils.~=(result(0).getOrElse(dataModel.lonColumn, 0d).asInstanceOf[Double],
                   48d,
                   0.00000000000001))
    assert(
      MathUtils.~=(result(0).getOrElse(dataModel.latColumn, 0d).asInstanceOf[Double],
                   14d,
                   0.00000000000001))
  }

}
