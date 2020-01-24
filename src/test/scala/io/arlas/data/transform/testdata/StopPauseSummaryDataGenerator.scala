package io.arlas.data.transform.testdata

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types.{BooleanType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

class StopPauseSummaryDataGenerator(spark: SparkSession,
                                    baseDF: DataFrame,
                                    dataModel: DataModel,
                                    speedColumn: String,
                                    tempoProportionsColumns: Map[String, String],
                                    tempoIrregular: String,
                                    standardDeviationEllipsisNbPoints: Int)
    extends FragmentSummaryDataGenerator(
      spark = spark,
      baseDF = baseDF,
      dataModel = dataModel,
      speedColumn = speedColumn,
      tempoProportionsColumns = tempoProportionsColumns,
      tempoIrregular = tempoIrregular,
      standardDeviationEllipsisNbPoints = standardDeviationEllipsisNbPoints,
      aggregationColumn = arlasMotionIdColumn,
      aggregationCondition = (arlasMovingStateColumn, ArlasMovingStates.STILL),
      additionalAggregations = (values, rows) =>
        values ++ Map(
          //update existing columns
          arlasTrackTrail -> values.getOrElse(arlasTrackLocationPrecisionGeometry, ""),
          arlasMotionDurationColumn -> rows.head.getAs[Long](arlasMotionDurationColumn),
          arlasMotionIdColumn -> rows.head.getAs[Long](arlasMotionIdColumn)
      )
    )
