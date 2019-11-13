package io.arlas.data.transform

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import io.arlas.data.app.ArlasProcConfig
import org.scalatest.{BeforeAndAfterEach, Suite}

trait ArlasMockServer extends BeforeAndAfterEach {

  this: Suite =>

  ArlasProcConfig.GEODATA_BASE_PATH = "http://localhost:8080"

  private val wireMockServer = new WireMockServer(
    options()
      .port(8080)
      .usingFilesUnderClasspath("wiremock"))

  override def beforeEach {
    wireMockServer.start()
  }

  override def afterEach {
    wireMockServer.stop()
  }

}
