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

package io.arlas.data.transform.filter

import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Remove outliers and clean temporary associated columns
  *
  * @param outlierColumn Name of boolean column containing outlier identification (True: outlier)
  */
class WithoutIdentifiedOutliers(outlierColumn: String) extends ArlasTransformer(Vector(outlierColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset
      .toDF()
      .filter(not(col(outlierColumn)))
      .drop(outlierColumn, "is_return_point", "is_local_outlier")
  }
}
