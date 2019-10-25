package io.arlas.data.transform.testdata

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasMovingStates
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class StopPauseSummaryDataGenerator(spark: SparkSession,
                                    baseDF: DataFrame,
                                    dataModel: DataModel,
                                    speedColumn: String,
                                    tempoProportionsColumns: Map[String, String],
                                    tempoIrregular: String,
                                    standardDeviationEllipsisNbPoints: Int)
    extends FragmentSummaryDataGenerator(
      spark,
      baseDF,
      dataModel,
      speedColumn,
      tempoProportionsColumns,
      tempoIrregular,
      standardDeviationEllipsisNbPoints,
      arlasMotionIdColumn,
      (arlasMovingStateColumn, ArlasMovingStates.STILL),
      Seq(StructField(arlasTempoColumn, StringType, true),
          StructField(arlasTrackTempoEmissionIsMulti, BooleanType, true)),
      Seq(),
      (values, rows) => {
        values.map(
          m =>
            if (m._1 == arlasTrackTrail)
              arlasTrackTrail -> values.getOrElse(arlasTrackLocationPrecisionGeometry, "")
            else if (m._1 == arlasMotionDurationColumn)
              arlasMotionDurationColumn -> rows.head.getAs[Long](arlasMotionDurationColumn)
            else if (m._1 == arlasMotionIdColumn)
              arlasMotionIdColumn -> rows.head.getAs[Long](arlasMotionIdColumn)
            else m)
      }
    )
