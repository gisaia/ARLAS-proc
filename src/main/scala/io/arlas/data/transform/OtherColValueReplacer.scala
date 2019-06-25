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

import org.apache.spark.sql.functions._
import io.arlas.data.model.DataModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Replace a value by another, of any type.
  * The new value may be in another column, or in the same
  * Example of use case: if col1 is null, then we want to override the value of col2
  * @param dataModel
  * @param sourceColumn
  * @param targetColumn
  * @param sourceValue
  * @param newValue
  * @tparam A scala type of source column
  * @tparam B scala type of target column
  */
class OtherColValueReplacer[A, B](dataModel: DataModel, sourceColumn: String, targetColumn: String, sourceValue: A, newValue: B)
  extends ArlasTransformer(dataModel, Vector(sourceColumn, targetColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    //also manage the case where sourceValue=null
    val newValueCol = Option(sourceValue)
      .map(v => when(col(sourceColumn).equalTo(v), lit(newValue)).otherwise(col(targetColumn)))
      .getOrElse(when(col(sourceColumn).isNull, lit(newValue)).otherwise(col(targetColumn)))

    dataset.withColumn(targetColumn, newValueCol)
  }

  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    val sourceColumnDataType = transformedSchema.fields.filter(_.name == sourceColumn).head.dataType
    val targetColumnDataType = transformedSchema.fields.filter(_.name == targetColumn).head.dataType

    //check that source value & column are of the same type
    Option(sourceValue).map(findDataTypeForValue(_)).map(
      dataType => {
        if (sourceColumnDataType != dataType) {
          throw new DataFrameException(s"The column ${sourceColumn} is expected to be of type ${dataType.typeName}, " +
                                       s"current: ${sourceColumnDataType.typeName}")
        }
      }
    )

    //check that target value & column are of the same type
    Option(newValue).map(findDataTypeForValue(_)).map(
      dataType => {
        if (targetColumnDataType != dataType) {
          throw new DataFrameException(s"The column ${targetColumn} is expected to be of type ${dataType.typeName}, " +
                                       s"current: ${targetColumnDataType.typeName}")
        }
      }
      )

    transformedSchema
  }

  /**
  * List of datatype with matching scala types on https://stackoverflow.com/a/32899979/1943432
  * ArrayType, MapType not supported because they need typing;
  * DecimalType and StructType not supported because it is a DataType but AbstractDataType with different hierarch
  */
  private def findDataTypeForValue(value: Any): DataType =
    value match {
      case b: Byte => ByteType
      case s: Short => ShortType
      case i: Int => IntegerType
      case l: Long => LongType
      case f: Float => FloatType
      case d: Double => DoubleType
      case s: String => StringType
      case b: Array[Byte] => BinaryType
      case b: Boolean => BooleanType
      case t: java.sql.Timestamp => TimestampType
      case d: java.sql.Date => DateType
      case _ => throw new DataFrameException(s"Unsupported type ${sourceValue.getClass.getName}")
    }

}
