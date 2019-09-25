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

import io.arlas.data.model.DataModel
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * This transformer maps multiple values to a new column.
  * e.g. if columnA=a1 and columnB=b1 then targetColumn=c1,
  * otherwise if columnA=a2 and columnB=b2 then targetColumn=c,
  * etc...
  * If valuesMap is empty, no column is added (expected type cannot be infered)
  *
  * You should not base the mapping on the order of the values, eg. if you pass as valuesMap
  * Map(
  *   Map(col1 -> value1), result1,
  *   Map(col2 -> value2), result2
  * )
  * you don't know which mapping will be applied at first
  * @param dataModel
  * @param valuesMap
  * @param targetColumn
  * @tparam A
  */
class MultipleColsValuesMapper[A](dataModel: DataModel,
                                  valuesMap: Map[A, Map[String, Any]],
                                  targetColumn: String) //TODO invert map order
    extends ArlasTransformer(dataModel, valuesMap.values.flatMap(_.keySet).toVector) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    valuesMap.size match {
      case 0 => dataset.toDF()
      case _ => dataset.withColumn(targetColumn, recursiveWhenOtherwise(valuesMap.toList))
    }
  }

  def recursiveWhenOtherwise(valuesList: List[(Any, Map[String, Any])]): Column =
    valuesList match {
      case Nil => null
      case x :: xs =>
        val resultValue = x._1
        val inputValues = x._2

        //build the spark conditions like lit(true).and(colA.equalTo(a1)).and(colB.equalTo(b1))
        val theAnds = inputValues.foldLeft(lit(true))((theAndsRec, anInputValue) => {
          theAndsRec.and(col(anInputValue._1).equalTo(anInputValue._2))
        })

        //set the resultValue when conditions are checked, otherwise check another value
        when(theAnds, resultValue).otherwise(recursiveWhenOtherwise(xs))
    }

  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    valuesMap.size match {
      case 0 => transformedSchema
      case _ =>
        transformedSchema.add(StructField(targetColumn, findDataTypeForValue(valuesMap.head._1)))
    }
  }

}
