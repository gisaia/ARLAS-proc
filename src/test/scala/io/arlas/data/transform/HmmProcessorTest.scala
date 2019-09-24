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

import io.arlas.data.model.{MLModelLocal}
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.sql._
import org.apache.spark.sql.functions._

class HmmProcessorTest extends ArlasTest {

  "HmmProcessor " should " have unknown result with not existing source column" in {

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new HmmProcessor(dataModel,
                         spark,
                         "notExisting",
                         MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json"),
                         arlasVisibleSequenceIdColumn,
                         "result",
                         5000)
      )

    val expectedDf = visibleSequencesDF.withColumn("result", lit("Unknown"))

    assertDataFrameEquality(transformedDf, expectedDf)
  }

  "HmmProcessor " should " have unknown result with not existing model" in {

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new HmmProcessor(dataModel,
                         spark,
                         "speed",
                         MLModelLocal(spark, "src/test/resources/not_existing.json"),
                         arlasVisibleSequenceIdColumn,
                         "result",
                         5000)
      )

    val expectedDf = visibleSequencesDF.withColumn("result", lit("Unknown"))

    assertDataFrameEquality(transformedDf, expectedDf)
  }

  "HmmProcessor transformation" should " not break using a window" in {

    val transformedDf = visibleSequencesDF
      .enrichWithArlas(
        new HmmProcessor(dataModel,
                         spark,
                         "speed",
                         MLModelLocal(spark, "src/test/resources/hmm_stillmove_model.json"),
                         arlasVisibleSequenceIdColumn,
                         arlasMovingStateColumn,
                         10))
      .count()
  }

}
