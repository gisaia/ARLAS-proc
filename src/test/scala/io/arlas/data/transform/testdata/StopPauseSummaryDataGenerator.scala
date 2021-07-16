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
