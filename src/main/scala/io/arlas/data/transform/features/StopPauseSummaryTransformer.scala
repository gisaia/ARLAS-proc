package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.WindowSpec

class StopPauseSummaryTransformer(dataModel: DataModel,
                                  standardDeviationEllipsisNbPoint: Int,
                                  salvoTempo: String,
                                  irregularTempo: String,
                                  tempoPropotionColumns: Map[String, String],
                                  weightAveragedColumns: Seq[String],
                                  additionalPropagatedColumns: Seq[String] = Seq())
    extends FragmentSummaryTransformer(
      dataModel,
      standardDeviationEllipsisNbPoint,
      salvoTempo,
      irregularTempo,
      tempoPropotionColumns,
      weightAveragedColumns,
      additionalPropagatedColumns
    ) {

  override def getAggregationColumn(): String = arlasMotionIdColumn

  override def getAggregateConditionColumns(): Seq[String] = Seq(arlasMovingStateColumn)

  override def getAggregateCondition(): Column =
    col(arlasMovingStateColumn).equalTo(ArlasMovingStates.STILL)

  override def additionalAggregations(window: WindowSpec): Map[(Int, String), Column] = Map(
    (200, arlasTrackTrail) -> col(arlasTrackLocationPrecisionGeometry)
  )

  override def propagatedColumns(): Seq[String] = {
    Seq(
      arlasCourseOrStopColumn,
      arlasCourseStateColumn,
      arlasMotionDurationColumn,
      arlasCourseIdColumn,
      arlasCourseDurationColumn,
      dataModel.idColumn
    )
  }
}
