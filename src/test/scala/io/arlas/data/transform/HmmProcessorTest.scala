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

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns.{arlasMovingStateColumn, arlasTimestampColumn, arlasVisibleSequenceIdColumn}
import io.arlas.data.sql._
import org.apache.spark.sql.functions._

class HmmProcessorTest extends ArlasTest {

  "HmmProcessor " should " have unknown result with not existing source column" in {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", visibilityTimeout = 300)

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new HmmProcessor(
          dataModel,
          spark,
          "notExisting",
          "src/test/resources/hmm_test_model.json",
          arlasVisibleSequenceIdColumn,
          "result")
        )

    val expectedDf = visibleSequencesDF.withColumn("result", lit("Unknown"))

    assertDataFrameEquality(transformedDf, expectedDf)
  }

  "HmmProcessor " should " have unknown result with not existing model" in {

    val dataModel =
      new DataModel(timeFormat = "dd/MM/yyyy HH:mm:ssXXX", visibilityTimeout = 300)

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new HmmProcessor(
          dataModel,
          spark,
          "speed",
          "src/test/resources/not_existing.json",
          arlasVisibleSequenceIdColumn,
          "result")
        )

    val expectedDf = visibleSequencesDF.withColumn("result", lit("Unknown"))

    assertDataFrameEquality(transformedDf, expectedDf)
  }


}
