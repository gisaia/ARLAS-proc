package io.arlas.data.utils

import org.scalatest.FlatSpec

class GeoToolTest extends FlatSpec {

  "computeStandardDeviationEllipsis " should " compute the standard deviation ellipsis" in {

    val expected =
      """
LINESTRING (1.413484 43.636883, 1.412121 43.6524121, 1.408125 43.666883, 1.4017683 43.6793094, 1.393484 43.6888445, 1.3838368 43.6948385, 1.373484 43.696883, 1.3631312 43.6948385, 1.353484 43.6888445, 1.3451997 43.6793094, 1.338843 43.666883, 1.334847 43.6524121, 1.333484 43.636883, 1.334847 43.6213539, 1.338843 43.606883, 1.3451997 43.5944566, 1.353484 43.5849215, 1.3631312 43.5789275, 1.373484 43.576883, 1.3838368 43.5789275, 1.393484 43.5849215, 1.4017683 43.5944566, 1.408125 43.606883, 1.412121 43.6213539, 1.413484 43.636883)
      """
    val result =
      GeoTool.getStandardDeviationEllipsis(43.636883, 1.373484, 0.06, 0.04, 24).get
    assert(result == expected.trim)
  }

  "computeStandardDeviationEllipsis " should " compute an ellipsis having different point, with null standard deviation" in {

    val expected =
      """
LINESTRING (2.373484 43.636883, 2.3394098 43.895702, 2.2395094 44.136883, 2.0805908 44.3439898, 1.873484 44.5029084, 1.632303 44.6028088, 1.373484 44.636883, 1.114665 44.6028088, 0.873484 44.5029084, 0.6663772 44.3439898, 0.5074586 44.136883, 0.4075582 43.895702, 0.373484 43.636883, 0.4075582 43.378064, 0.5074586 43.136883, 0.6663772 42.9297762, 0.873484 42.7708576, 1.114665 42.6709572, 1.373484 42.636883, 1.632303 42.6709572, 1.873484 42.7708576, 2.0805908 42.9297762, 2.2395094 43.136883, 2.3394098 43.378064, 2.373484 43.636883)
      """
    val result =
      GeoTool.getStandardDeviationEllipsis(43.636883, 1.373484, 0.0, 0.0, 24).get
    assert(result == expected.trim)
  }

  "getGPSBearing" should "compute the bearing" in {

    val expected = 43.032734857892365
    val result = GeoTool.getBearingBetween(42.09839, 11.780456, 42.099849, 11.782285).get

    assert(result == expected)
  }

  "getGPSBearing" should "always return a bearing between 0 and 360" in {

    val expected = 338.6604651471227
    val result = GeoTool.getBearingBetween(42.099639, 11.782073, 42.099761, 11.782009).get

    assert(result == expected)
  }

}
