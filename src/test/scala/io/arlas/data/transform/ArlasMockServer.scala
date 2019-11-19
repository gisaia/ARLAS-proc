package io.arlas.data.transform

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import io.arlas.data.app.ArlasProcConfig
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait ArlasMockServer extends BeforeAndAfterAll {

  this: Suite =>

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  ArlasProcConfig.GEODATA_BASE_PATH = "http://localhost:8080"
  ArlasProcConfig.REFINE_TRAIL_BASE_PATH = "http://localhost:8080"

  private lazy val wireMockServer = new WireMockServer(
    options()
      .port(8080)
      .usingFilesUnderClasspath("wiremock"))

  override protected def beforeAll(): Unit = {
    logger.info("Starting Wiremock server")
    wireMockServer.start()
    logger.info("Wiremock server started")
  }

  override protected def afterAll(): Unit = {
    logger.info("Stopping Wiremock server")
    wireMockServer.stop()
    logger.info("Stopping server started")
  }

}
