package io.arlas.data.transform

import java.time._
import java.time.format.DateTimeFormatter

import io.arlas.data.model.DataModel
import io.arlas.data.extract.transformations._
import io.arlas.data.transform.transformations._
import io.arlas.data.{DataFrameTester, TestSparkSession}
import org.apache.commons.math3.analysis.interpolation.{SplineInterpolator, UnivariateInterpolator}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class TransformationWithSequenceResampledTest extends FlatSpec with Matchers with TestSparkSession with DataFrameTester {

  import spark.implicits._

  val timeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ssXXX")
  timeFormatter.withZone(ZoneOffset.UTC)

  val source = testData

  val expected =  {
    val objectASeq1Raw =source.filter(row => row._1.equals("ObjectA")
      && LocalDateTime.parse(row._2, timeFormatter).isBefore(LocalDateTime.parse("01/06/2018 00:10:00+02:00", timeFormatter)))
    val objectASeq2Raw =source.filter(row => row._1.equals("ObjectA")
      && LocalDateTime.parse(row._2, timeFormatter).isAfter(LocalDateTime.parse("01/06/2018 00:10:00+02:00", timeFormatter)))
    val objectBSeq1Raw =source.filter(row => row._1.equals("ObjectB")
      && LocalDateTime.parse(row._2, timeFormatter).isBefore(LocalDateTime.parse("01/06/2018 00:07:00+02:00", timeFormatter)))
    val objectBSeq2Raw =source.filter(row => row._1.equals("ObjectB")
      && LocalDateTime.parse(row._2, timeFormatter).isAfter(LocalDateTime.parse("01/06/2018 00:07:00+02:00", timeFormatter)))
    expectedInterpolation(objectASeq1Raw,15) ++ expectedInterpolation(objectASeq2Raw,15) ++ expectedInterpolation(objectBSeq1Raw,15) ++ expectedInterpolation(objectBSeq2Raw,15)
  }

  def expectedInterpolation(data: Seq[(String,String,Double,Double)], timeSampling: Long) : Seq[(String,String,Double,Double,String)] = {
    val dataTimestamped = data.map(row => (ZonedDateTime.parse(row._2, timeFormatter).toEpochSecond(),row._3, row._4)).distinct.sortBy(_._1)
    val ts = dataTimestamped.map(_._1.toDouble).toArray
    val lat = dataTimestamped.map(_._2).toArray
    val lon = dataTimestamped.map(_._3).toArray
    val interpolator = new SplineInterpolator()
    val functionLat = interpolator.interpolate(ts, lat);
    val functionLon = interpolator.interpolate(ts, lon);
    val minTs = (ts.min - ts.min%timeSampling + timeSampling).toLong
    val maxTs = (ts.max - ts.max%timeSampling).toLong
    val id = data.head._1
    val sequence = s"""${id}#${ts.min.toLong}"""
    List.range(minTs, maxTs, timeSampling)
        .map(ts => (id, s"${ZonedDateTime.ofInstant(Instant.ofEpochSecond(ts),ZoneOffset.UTC).format(timeFormatter)}", functionLat.value(ts), functionLon.value(ts), sequence))
  }

  "withSequenceResampled transformation" should " resample data against dataframe's sequences" in {

    val dataModel = new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", sequenceGap = 300)

    val sourceDF = source.toDF("id", "timestamp", "lat", "lon")

    val actualDF = sourceDF
      .transform(withArlasTimestamp(dataModel))
      .transform(withArlasPartition(dataModel))

    val transformedDf: DataFrame = doPipelineTransform(
      actualDF,
      new WithSequenceIdTransformer(dataModel),
      new WithSequenceResampledTransformer(dataModel, spark))
      .drop(arlasTimestampColumn, arlasPartitionColumn)

    val expectedDF =  expected
      .toDF("id", "timestamp", "lat", "lon", arlasSequenceIdColumn)

    assertDataFrameEquality(transformedDf, expectedDF)
  }

}
