package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.immutable.ListMap

class StopPauseSummaryTransformer(spark: SparkSession,
                                  dataModel: DataModel,
                                  standardDeviationEllipsisNbPoint: Int,
                                  irregularTempo: String,
                                  tempoPropotionColumns: Map[String, String],
                                  weightAveragedColumns: Seq[String])
    extends FragmentSummaryTransformer(
      spark,
      dataModel,
      standardDeviationEllipsisNbPoint,
      irregularTempo,
      tempoPropotionColumns,
      weightAveragedColumns
    ) {

  override def getAggregationColumn(): String = arlasMotionIdColumn

  override def getAggregateCondition(): Column =
    col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)

  override def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] =
    ListMap(
      arlasTrackTrail -> col(arlasTrackLocationPrecisionGeometry)
    )

  override def getPropagatedColumns(): Seq[String] = {
    Seq(
      arlasMovingStateColumn,
      arlasCourseOrStopColumn,
      arlasCourseStateColumn,
      arlasMotionDurationColumn,
      arlasCourseIdColumn,
      arlasCourseDurationColumn,
      dataModel.idColumn
    )
  }
}
