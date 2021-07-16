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

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._

/**
  * Standardize the static columns of each object.
  * For a column of any type and for the same value of idCol, its sets all undefined value to null.
  * Then it tries to replace these null with the first non-null value that is found;
  * if none is found then a default value is used
  * @param idColumn
  * @param cols a map with (static column name -> (default value if no valid is found, sequence of undefined values for this column)).
  *             You don't need to pass `null` as undefined value, this is always checked
  */
class StaticColumnsStandardizer(idColumn: String, cols: Map[String, (Any, Seq[Any])])
    extends ArlasTransformer(Vector(cols.keys.toSeq: _*)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val withNullDF = cols.foldLeft(dataset.toDF()) {
      case (df, c) => df.withColumn(c._1, when(col(c._1).isin(c._2._2: _*), lit(null)).otherwise(col(c._1)))
    }

    val window = Window.partitionBy(idColumn)

    cols
      .foldLeft(withNullDF) {
        case (df, c) =>
          df.withColumn(c._1, when(first(c._1, true).over(window).isNotNull, first(c._1, true).over(window)).otherwise(lit(c._2._1)))
      }
  }

}
