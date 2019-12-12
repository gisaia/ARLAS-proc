/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.model

import java.security.InvalidParameterException

import io.arlas.data.app.ArlasProcConfig
import io.arlas.data.utils.RestTool
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Try}

abstract sealed class MLModel {
  def getModelString(): Try[String]
}

case class MLModelLocal(spark: SparkSession, path: String) extends MLModel {

  override def getModelString(): Try[String] = {
    Try(spark.sparkContext.textFile(path, 1).toLocalIterator.mkString)
  }
}

case class MLModelHosted(spark: SparkSession, project: String, modelName: String, version: String = "latest") extends MLModel {

  val CLOUDSMITH_TOKEN_KEY = "spark.driver.CLOUDSMITH_ML_MODELS_TOKEN"
  val CLOUDSMITH_REPO_KEY = "spark.driver.CLOUDSMITH_ML_MODELS_REPO"

  override def getModelString(): Try[String] = {

    val cloudsmithToken = Try(spark.conf.get(CLOUDSMITH_TOKEN_KEY)).toOption
    val cloudsmithRepo = Try(spark.conf.get(CLOUDSMITH_REPO_KEY)).toOption

    if (!cloudsmithToken.isDefined) {
      Failure(
        new InvalidParameterException(s"${CLOUDSMITH_TOKEN_KEY} conf not set, cannot download ML Model ${modelName}:${version}/${project}"))
    } else if (!cloudsmithRepo.isDefined) {
      Failure(
        new InvalidParameterException(s"${CLOUDSMITH_REPO_KEY} conf not set, cannot download ML Model ${modelName}:${version}/${project}"))
    } else {
      RestTool.get(ArlasProcConfig.getCloudsmithModelUrl(cloudsmithToken.get, cloudsmithRepo.get, version, project, modelName))
    }

  }

}
