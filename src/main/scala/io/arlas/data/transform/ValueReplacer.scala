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
  * To be extended in order to implement transformSchema, by checking that column is of expected type
  * @param dataModel
  * @param column
  * @param oldValue
  * @param newValue
  * @tparam A scala type of the column
  */
class ValueReplacer[A](dataModel: DataModel, column: String, oldValue: A, newValue: A)
  extends ArlasTransformer(dataModel, Vector(column)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(column, when(col(column).equalTo(oldValue), lit(newValue)).otherwise(col(column)))
  }

  override def transformSchema(schema: StructType): StructType = {
    val transformedSchema = super.transformSchema(schema)
    val columnDataType = transformedSchema.fields.filter(_.name == column).head.dataType

    // List of datatype with matching scala types on https://stackoverflow.com/a/32899979/1943432
    // ArrayType, MapType not supported because they need typing;
    // DecimalType and StructType not supported because it is a DataType but AbstractDataType with different hierarchy
    val expectedDataType: DataType = oldValue match {
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

      case _ => throw new DataFrameException(s"Unsupported type ${oldValue.getClass.getName}")
    }

    if (columnDataType != expectedDataType) {
      throw new DataFrameException(s"The column ${column} should be of type ${expectedDataType.typeName}, actual: ${columnDataType.typeName}")
    }

    transformedSchema
  }

}
