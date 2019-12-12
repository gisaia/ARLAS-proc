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
