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

package io.arlas.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

trait DataFrameTester {

  def assertDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {

    //Check content equality
    val a = defaultSortAndColsOrder(actualDF)
    val e = defaultSortAndColsOrder(expectedDF)

    //Check scheme equality
    if (!a.schema.equals(e.schema)) {
      throw DataFrameMismatchException(
        schemeMismatchMessage(a, e)
      )
    }

    val aElements = a.collect()
    val eElements = e.collect()

    if (!aElements.sameElements(eElements)) {
      throw DataFrameMismatchException(
        contentMismatchMessage(aElements, eElements, a.schema.fields)
      )
    }
  }

  def defaultSortAndColsOrder(ds: DataFrame): DataFrame = {
    val colNames = ds.columns.sorted
    val cols = colNames.map(col)
    ds.sort(cols: _*) //sort rows
      .select(cols: _*) //sort columns for a consistent schema
  }

  private def contentMismatchMessage[Row](a: Array[Row], e: Array[Row], fields: Array[StructField]): String = {
    "DataFrame content mismatch [ actual rows | expected rows ]\n" + a
      .zip(e)
      .map {
        case (r1, r2) =>
          if (r1.equals(r2)) {
            s"= [ $r1 | $r2 ]"
          } else {
            s"# [ $r1 | $r2 ]"
          }
      }
      .mkString("\n") + s"\n\nTo be pasted into a CSV parser for debug\n'result',${fields.map(_.name).mkString("','")}' \n" + a
      .zip(e)
      .map {
        case (r1: GenericRowWithSchema, r2: GenericRowWithSchema) =>
          if (r1.equals(r2)) {
            s"'','${r1.mkString("','")}' \n '=','${r2.mkString("','")}'\n"
          } else {
            s"'','${r1.mkString("','")}' \n '#','${r2.mkString("','")}'\n"
          }
      }
      .mkString("\n") + """  """
  }

  private def schemeMismatchMessage(actualDF: DataFrame, expectedDF: DataFrame) = {
    s"""DataFrame schema mismatch
        Actual Schema:
          ${actualDF.schema}
        Expected Schema:
          ${expectedDF.schema}
        """
  }

}

case class DataFrameMismatchException(msg: String) extends Exception(msg)
