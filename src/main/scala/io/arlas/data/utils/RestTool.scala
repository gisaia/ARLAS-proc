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

import java.net.{HttpURLConnection, URL, UnknownHostException}

import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object RestTool {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Try to get a URL content.
    * @param url
    * @param connectTimeout
    * @param readTimeout
    * @param requestMethod
    * @return
    */
  def get(url: String, connectTimeout: Int = 60000, readTimeout: Int = 60000, requestMethod: String = "GET") =
    Try {

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

  /**
    * Try to get a URL content.
    * In case of error: if target URL cannot be reached, re-throw an exception (this is unexpected).
    * Otherwise, simply return a Try instance.
    * @param url
    * @param connectTimeout
    * @param readTimeout
    * @param requestMethod
    * @throws Exception if target URL is not found
    * @return
    */
  @throws[Exception]
  def getOrFailOnNotAvailable(url: String, connectTimeout: Int = 60000, readTimeout: Int = 60000, requestMethod: String = "GET") = {

    val tryGetContent = get(url, connectTimeout, readTimeout, requestMethod)

    if (tryGetContent.isFailure &&
        (tryGetContent.failed.get.isInstanceOf[java.net.SocketTimeoutException]
        || tryGetContent.failed.get.isInstanceOf[UnknownHostException])) {
//    not good scala, but the most proper way to stop the spark application
      throw new Exception(s"Cannot read WS at ${url}, stopping!")
    }

    tryGetContent
  }

}
