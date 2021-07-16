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

package io.arlas.data.transform

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.Numeric.Implicits._
import scala.collection.immutable.ListMap

object ArlasTestHelper {

  def createDataFrameWithTypes(spark: SparkSession, data: List[Seq[_]], types: ListMap[String, (DataType, Boolean)]) = {

    import scala.collection.JavaConversions._

    spark.createDataFrame(
      data.map(Row.fromSeq(_)),
      StructType(
        types
          .map(t => StructField(t._1, t._2._1, t._2._2))
          .toList)
    )
  }

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  def scaleDouble(double: Double, scale: Int) =
    BigDecimal(double).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
}
