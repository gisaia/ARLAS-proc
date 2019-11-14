package io.arlas.data.app

object ArlasProcConfig {

  var GEODATA_BASE_PATH = "http://nominatim.services.arlas.io"
  var CLOUDSMITH_BASE_PATH = "https://dl.cloudsmith.io"

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

}
