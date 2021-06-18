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

package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{udf, col}

class WithGeometrySimplifier(inputGeometryCol: String,
                             outputGeometryCol: String,
                             distanceTolerance: Option[Double] = None)
    extends ArlasTransformer(Vector(inputGeometryCol)) {

  val simplifierUDF = udf(
    (geometryCol: String) => GeoTool.simplifyGeometry(geometryCol, distanceTolerance))

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(outputGeometryCol, simplifierUDF(col(inputGeometryCol)))
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema).add(StructField(outputGeometryCol, StringType, true))
}
