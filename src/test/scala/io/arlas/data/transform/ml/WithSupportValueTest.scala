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

package io.arlas.data.transform.ml

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.types._
import io.arlas.data.transform.ArlasTestHelper._
import scala.collection.immutable.ListMap

class WithSupportValueTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq(10l, 4.0d, 0.1d, "tempo_other", Seq(0.1d)),
      Seq(10l, 4.0d, 0.2d, "tempo_irregular", Seq(0.4d, 0.4d)),
      Seq(100l, 10.0d, 0.3d, "tempo_other", Seq(0.3d)),
      Seq(100l, 10.0d, 0.4d, "tempo_irregular", Seq.fill(8)(0.1d))
    ),
    ListMap(
      arlasTrackDuration -> (LongType, true),
      "distance" -> (DoubleType, true),
      speedColumn -> (DoubleType, true),
      arlasTempoColumn -> (StringType, true),
      "expected" + speedColumn + "_array" -> (ArrayType(DoubleType, true), false)
    )
  )

  val baseDF = testDF.drop("expected" + speedColumn + "_array")

  "WithSupportValues " should "add a column with a list of support column's value" in {

    val expectedDF =
      testDF.withColumnRenamed("expected" + speedColumn + "_array", speedColumn + "_array")

    val transformedDF = baseDF.enrichWithArlas(
      new WithSupportValues(speedColumn, 5, 8, "tempo_irregular", "distance"))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
