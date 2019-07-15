/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WithStateIdFromStateTest extends ArlasTest {

  "WithStateIdFromState transformation " should " fill/generate state id against dataframe's timeseries" in {

    val sourceDF = cleanedDF

    val transformedDF: DataFrame = sourceDF
      .enrichWithArlas(
        new WithArlasVisibilityStateFromTimestamp(dataModel, processingConfig.visibilityTimeout),
        new WithStateIdFromState(dataModel, arlasVisibilityStateColumn, ArlasVisibilityStates.APPEAR.toString, arlasVisibleSequenceIdColumn))

    val expectedDF = visibleSequencesDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithStateIdFromState transformation " should " resume state id when adding a warm up period" in {

    val sourceDF = cleanedDF

    val warmupDF: DataFrame = sourceDF
      .filter(col(arlasTimestampColumn) < 1527804100)
      .enrichWithArlas(
        new WithArlasVisibilityStateFromTimestamp(dataModel, processingConfig.visibilityTimeout),
        new WithStateIdFromState(dataModel, arlasVisibilityStateColumn, ArlasVisibilityStates.APPEAR.toString, arlasVisibleSequenceIdColumn))

    val transformedDF: DataFrame = sourceDF
      .withEmptyCol(arlasVisibleSequenceIdColumn)
      .withEmptyCol(arlasVisibilityStateColumn)
      .filter(col(arlasTimestampColumn) >= 1527804100)
      .unionByName(warmupDF)
      .enrichWithArlas(
        new WithArlasVisibilityStateFromTimestamp(dataModel, processingConfig.visibilityTimeout),
        new WithStateIdFromState(dataModel, arlasVisibilityStateColumn, ArlasVisibilityStates.APPEAR.toString, arlasVisibleSequenceIdColumn))

    val expectedDF = visibleSequencesDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
