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

package io.arlas.data.app.extract

import io.arlas.data.app.BasicApp
import io.arlas.data.model.{DataModel, RunOptions}
import io.arlas.data.sql._
import org.apache.spark.sql._

object CSVExtractor extends BasicApp {

  override def getName: String = "CSV Extractor"

  override def run(spark: SparkSession, dataModel: DataModel, runOptions: RunOptions): Unit = {

    readFromCsv(spark, runOptions.source)
      .asArlasBasicData(dataModel)
      .filterOnPeriod(runOptions.period)
      .writeToParquet(spark, runOptions.target)

  }
}
