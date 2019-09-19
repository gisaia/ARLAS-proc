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

class IdUpdaterTest extends ArlasTest {

  val sequenceIdObjA =
    when(col(arlasTimestampColumn).lt(lit(1527804601)), lit("ObjectA#1527804000_1527804291"))
      .otherwise(lit("ObjectA#1527804291_1527804771"))
  val sequenceIdObjB =
    when(col(arlasTimestampColumn).lt(lit(1527804451)), lit("ObjectB#1527804000_1527804060"))
      .otherwise(lit("ObjectB#1527804060_1527804600"))

  val baseDF = cleanedDF
    .enrichWithArlas(new FlowFragmentMapper(dataModel, spark, dataModel.idColumn))
    .asArlasVisibleSequencesFromTimestamp(dataModel, visibilityTimeout)
  val expectedDF = baseDF
    .withColumn(
      arlasVisibleSequenceIdColumn,
      when(col(dataModel.idColumn).equalTo("ObjectA"), sequenceIdObjA)
        .otherwise(when(col(dataModel.idColumn).equalTo("ObjectB"), sequenceIdObjB)
          .otherwise(lit(null))) //NB : last otherwise is useless but create a nullable column for schema compliance
    )

  "WithDurationFromId transformation " should " compute duration of visibility sequences" in {

    val transformedDF: DataFrame = baseDF
      .enrichWithArlas(
        new IdUpdater(dataModel, arlasVisibleSequenceIdColumn)
      )

    assertDataFrameEquality(
      transformedDF.select(dataModel.idColumn, arlasVisibleSequenceIdColumn),
      expectedDF.select(dataModel.idColumn, arlasVisibleSequenceIdColumn)
    )
  }

}
