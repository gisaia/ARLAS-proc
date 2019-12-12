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

package io.arlas.data.transform

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Transformer => SparkTransformer}
import org.apache.spark.sql.types.StructType

abstract class ArlasTransformer(val requiredCols: Vector[String] = Vector.empty) extends SparkTransformer {

  override def copy(extra: ParamMap): SparkTransformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
  }

  def checkSchema(schema: StructType): StructType = {
    checkRequiredColumns(schema, requiredCols)
    schema
  }

  def checkRequiredColumns(schema: StructType, cols: Vector[String]) = {
    val colsNotFound =
      cols.distinct.diff(schema.fieldNames)
    if (colsNotFound.length > 0) {
      throw DataFrameException(
        s"The ${colsNotFound.mkString(", ")} columns are not included in the DataFrame with the following columns ${schema.fieldNames
          .mkString(", ")}")
    }
  }

  override val uid: String = {
    Identifiable.randomUID(this.getClass.getSimpleName)
  }

}

case class DataFrameException(message: String) extends Exception(message)
