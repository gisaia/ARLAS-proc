package io.arlas.data.transform.testdata
import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTestHelper.{mean, stdDev}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.datum.DefaultEllipsoid
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKTWriter
import io.arlas.data.transform.ArlasTestHelper._

class FlowFragmentDataGenerator(
    spark: SparkSession,
    baseDF: DataFrame,
    dataModel: DataModel,
    averagedColumns: List[String],
    standardDeviationEllipsisNbPoints: Int
) extends TestDataGenerator {

  override def get(): DataFrame = {

    val flowFragmentData: Seq[Row] = getData
    val flowFragmentSchema: StructType = getSchema

    spark
      .createDataFrame(
        spark.sparkContext.parallelize(flowFragmentData),
        flowFragmentSchema
      )
  }

  private def getData = {
    val flowFragmentData = baseDF
      .collect()
      .groupBy(_.getAs[String](dataModel.idColumn))
      .flatMap {
        case (id, rows) => {
          rows
            .sliding(2)
            .map(window => {

              val prevLat = window(0).getAs[Double](dataModel.latColumn)
              val prevLon = window(0).getAs[Double](dataModel.lonColumn)
              val curLat = window(1).getAs[Double](dataModel.latColumn)
              val curLon = window(1).getAs[Double](dataModel.lonColumn)
              val prevTimestamp = window(0).getAs[Long](arlasTimestampColumn)
              val curTimestamp = window(1).getAs[Long](arlasTimestampColumn)

              val latMean =
                scaleDouble(mean(Seq(prevLat, curLat)), GeoTool.coordinatesDecimalPrecision)
              val lonMean =
                scaleDouble(mean(Seq(prevLon, curLon)), GeoTool.coordinatesDecimalPrecision)
              val latStd = stdDev(Seq(prevLat, curLat))
              val lonStd = stdDev(Seq(prevLon, curLon))

              val start = new Coordinate(prevLon, prevLat)
              val end = new Coordinate(curLon, curLat)
              val trail =
                if (start.equals2D(end)) new GeometryFactory().createPoint(start)
                else new GeometryFactory().createLineString(Array(start, end))

              val geodesicCalculator = new GeodeticCalculator(DefaultEllipsoid.WGS84)
              geodesicCalculator.setStartingGeographicPoint(prevLon, prevLat)
              geodesicCalculator.setDestinationGeographicPoint(curLon, curLat)

              val duration = curTimestamp - prevTimestamp

              val azimuth = geodesicCalculator.getAzimuth
              val bearing = ((azimuth % 360) + 360) % 360

              val averagedValues = averagedColumns.map(averagedColumn =>
                (window(0).getAs[Double](averagedColumn) + window(1)
                  .getAs[Double](averagedColumn)) / 2.0)

              Row.fromSeq(window(1).toSeq ++ Array[Any](
                s"""${id}#${prevTimestamp}_${curTimestamp}""",
                2,
                new WKTWriter().write(trail),
                duration,
                prevTimestamp,
                curTimestamp,
                (prevTimestamp + curTimestamp) / 2,
                latMean,
                lonMean,
                latStd,
                lonStd,
                GeoTool
                  .getStandardDeviationEllipsis(
                    latMean,
                    lonMean,
                    latStd,
                    lonStd,
                    standardDeviationEllipsisNbPoints
                  )
                  .get,
                geodesicCalculator.getOrthodromicDistance(),
                geodesicCalculator.getOrthodromicDistance(),
                1.0,
                geodesicCalculator.getOrthodromicDistance() / duration / 1000 * 3600,
                bearing
              ) ++ averagedValues)
            })
        }
      }
      .toSeq
    flowFragmentData
  }

  private def getSchema = {

    val schema = baseDF.schema
      .add(StructField(arlasTrackId, StringType, true))
      .add(StructField(arlasTrackNbGeopoints, IntegerType, true))
      .add(StructField(arlasTrackTrail, StringType, true))
      .add(StructField(arlasTrackDuration, LongType, true))
      .add(StructField(arlasTrackTimestampStart, LongType, true))
      .add(StructField(arlasTrackTimestampEnd, LongType, true))
      .add(StructField(arlasTrackTimestampCenter, LongType, true))
      .add(StructField(arlasTrackLocationLat, DoubleType, true))
      .add(StructField(arlasTrackLocationLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLat, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionValueLon, DoubleType, true))
      .add(StructField(arlasTrackLocationPrecisionGeometry, StringType, true))
      .add(StructField(arlasTrackDistanceGpsTravelled, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthLine, DoubleType, true))
      .add(StructField(arlasTrackDistanceGpsStraigthness, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsSpeedKmh, DoubleType, true))
      .add(StructField(arlasTrackDynamicsGpsBearing, DoubleType, true))

    averagedColumns.foldLeft(schema) { (s, c) =>
      s.add(StructField(arlasTrackPrefix + c, DoubleType, true))
    }
  }

}
