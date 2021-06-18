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

import org.locationtech.jts.geom.Coordinate
import org.scalatest.FlatSpec

import scala.util.Random

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
LINESTRING (1.373584 43.636883, 1.3735806 43.6369089, 1.3735706 43.636933, 1.3735547 43.6369537, 1.373534 43.6369696, 1.3735099 43.6369796, 1.373484 43.636983, 1.3734581 43.6369796, 1.373434 43.6369696, 1.3734133 43.6369537, 1.3733974 43.636933, 1.3733874 43.6369089, 1.373384 43.636883, 1.3733874 43.6368571, 1.3733974 43.636833, 1.3734133 43.6368123, 1.373434 43.6367964, 1.3734581 43.6367864, 1.373484 43.636783, 1.3735099 43.6367864, 1.373534 43.6367964, 1.3735547 43.6368123, 1.3735706 43.636833, 1.3735806 43.6368571, 1.373584 43.636883)
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

  "removeConsecutiveDuplicatesCoords" should "remove consecutive duplicated coordinates" in {
    val coordinates = List(
      new Coordinate(1.0, 1.0),
      new Coordinate(2.0, 2.0),
      new Coordinate(2.0, 2.0),
      new Coordinate(3.0, 3.0),
      new Coordinate(3.0, 3.0),
      new Coordinate(5.0, 5.0),
      new Coordinate(4.0, 4.0),
      new Coordinate(3.0, 3.0),
      new Coordinate(1.0, 1.0),
      new Coordinate(1.0, 1.0),
      new Coordinate(1.0, 1.0),
      new Coordinate(9.0, 9.0)
    )

    val expected = List(
      new Coordinate(1.0, 1.0),
      new Coordinate(2.0, 2.0),
      new Coordinate(3.0, 3.0),
      new Coordinate(5.0, 5.0),
      new Coordinate(4.0, 4.0),
      new Coordinate(3.0, 3.0),
      new Coordinate(1.0, 1.0),
      new Coordinate(9.0, 9.0)
    )

    assert(GeoTool.removeConsecutiveDuplicatesCoords(coordinates) == expected)
  }

  "removeConsecutiveDuplicatesCoords" should "not throw any StackOverflowError with many coordinates" in {
    val randomList = List.fill(1000000)(new Coordinate(Random.nextDouble(), Random.nextDouble()))
    GeoTool.removeConsecutiveDuplicatesCoords(randomList)
  }

  "groupConsecutiveValuesByCondition" should "group consecutive values based on a condition" in {
    val values = Seq((1, "a"), (1, "b"), (2, "c"), (1, "d"), (1, "e"), (1, "f"), (2, "g"))

    val expected = Seq(
      Seq("a", "b"),
      Seq("d", "e", "f")
    )

    assert(GeoTool.groupConsecutiveValuesByCondition(1, values) == expected)
  }

}
