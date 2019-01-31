package io.arlas.data.extract

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import io.arlas.data.model.DataModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType}

object transformations {

  val arlasTimestampColumn = "arlas_timestamp"
  val arlasPartitionColumn = "arlas_partition"

  def getUdf(timeFormat: String) : UserDefinedFunction = udf{ date: String =>
    val timeFormatter = DateTimeFormatter.ofPattern(timeFormat)
    date match {
      case null => None
      case d => {
        try
          Some(ZonedDateTime.parse(date, timeFormatter).toEpochSecond)
        catch {
          case dtpe: DateTimeParseException => {
            try
              Some(ZonedDateTime.parse(date, timeFormatter.withZone(ZoneOffset.UTC)).toEpochSecond)
            catch {
              case _: Exception => None
            }
          }
          case _: Exception => None
        }
      }
    }
  }

  def withArlasTimestamp(dataModel: DataModel)
                        (df: DataFrame): DataFrame = {

    val timestampConversion = getUdf(dataModel.timeFormat)
    df.withColumn(arlasTimestampColumn,timestampConversion(col(dataModel.timestampColumn)))
  }

  def withArlasPartition(dataModel: DataModel)
                        (df: DataFrame): DataFrame = {
    df.withColumn(arlasPartitionColumn,date_format(to_date(col(dataModel.timestampColumn),dataModel.timeFormat),"yyyyMMdd").cast(IntegerType))
  }

}
