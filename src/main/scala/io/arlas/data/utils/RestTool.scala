package io.arlas.data.utils

import java.net.{HttpURLConnection, URL, UnknownHostException}
import org.slf4j.LoggerFactory
import scala.util.{Failure, Try}

object RestTool {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Try to get a URL content.
    * In case of error: if target URL cannot be reached, re-throw an exception (this is unexpected).
    * Otherwise, simply return a Try instance.
    * @param url
    * @param connectTimeout
    * @param readTimeout
    * @param requestMethod
    * @throws
    * @return
    */
  @throws[Exception]
  def get(url: String,
          connectTimeout: Int = 60000,
          readTimeout: Int = 60000,
          requestMethod: String = "GET") = {

    val tryGetContent = Try {

      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)

      val inputStream = connection.getInputStream
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) Try(inputStream.close)
      content
    }.recoverWith {
      case e: Exception => {
        logger.info(this.getClass + s" failed getting ${url} with " + e.getMessage)
        Failure(e)
      }
    }

    if (tryGetContent.isFailure &&
        (tryGetContent.failed.get.isInstanceOf[java.net.SocketTimeoutException]
        || tryGetContent.failed.get.isInstanceOf[UnknownHostException])) {
      //not good scala, but the most proper way to stop the spark application
      throw new Exception(s"Cannot read WS at ${url}, stopping!")
    }

    tryGetContent

  }

}
