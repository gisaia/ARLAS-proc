package io.arlas.data.app

import io.arlas.data.utils.GeoTool

object ArlasProcConfig {

  var GEODATA_BASE_PATH = "http://nominatim.services.arlas.io"
  var CLOUDSMITH_BASE_PATH = "https://dl.cloudsmith.io"
  var REFINE_TRAIL_BASE_PATH = "http://routing.services.arlas.io"

  val getGeodataUrl = (lat: Double, lon: Double, zoomLevel: Int) =>
    // %2f ensures doubles aren't formatted like an exponential
    s"${GEODATA_BASE_PATH}/reverse.php?format=json&lat=%2f&lon=%2f&zoom=${zoomLevel}"
      .format(lat, lon)

  val getCloudsmithModelUrl = (token: String,
                               repo: String,
                               version: String,
                               project: String,
                               model: String) =>
    s"${CLOUDSMITH_BASE_PATH}/${token}/${repo}/raw/versions/${version}/io.arlas.ml.models.${project}.${model}"

  val getRefineTrailUrl = (trail: String) => {
    val points =
      // %2f ensures doubles aren't formatted like an exponential
      GeoTool.wktToGeometry(trail).map(c => s"point=%2f,%2f".format(c._1, c._2)).mkString("&")
    s"${REFINE_TRAIL_BASE_PATH}/route?${points}&vehicle=car&locale=en&calc_points=true&instructions=false&points_encoded=false&type=json"
  }

}
