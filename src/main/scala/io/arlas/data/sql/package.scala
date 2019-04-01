package io.arlas.data
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.arlas.data.extract.transformations.{arlasPartitionColumn, arlasTimestampColumn}
import io.arlas.data.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object sql extends DataFrameReader {

  def getPeriod(start: String, stop: String): Option[Period] = {
    Some(
      Period(ZonedDateTime.parse(start, DateTimeFormatter.ISO_OFFSET_DATE_TIME),
             ZonedDateTime.parse(stop, DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
  }

  implicit class ArlasDataFrame(df: DataFrame) extends WritableDataFrame(df) {

    def filterOnPeriod(period: Option[Period]): DataFrame = {
      period match {
        case Some(p: Period) => {
          df.where(
              col(arlasPartitionColumn) >= Integer.valueOf(
                p.start.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
                && col(arlasPartitionColumn) <= Integer.valueOf(
                  p.stop.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
            .where(
              col(arlasTimestampColumn) >= p.start.toEpochSecond && col(arlasTimestampColumn) <= p.stop.toEpochSecond)
        }
        case _ => df
      }
    }
  }
}
