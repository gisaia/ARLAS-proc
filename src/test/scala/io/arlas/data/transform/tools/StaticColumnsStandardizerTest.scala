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

package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import io.arlas.data.sql._

import scala.collection.immutable.ListMap

class StaticColumnsStandardizerTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 1l, "unknown", "val2", "val3", 1, "val1", "val2", "val3", 1),
      Seq("id1", 2l, "val1", "val2", "val3", null, "val1", "val2", "val3", 1),
      Seq("id1", 3l, "undefined", "undefined", "unknown", null, "val1", "val2", "unknown", 1),
      Seq("id1", 4l, null, null, null, null, "val1", "val2", null, 1),
      Seq("id2", 1l, "undefined", "notset", "unknown", null, "default", "notset", "unknown", 0),
      Seq("id3", 1l, "val1", "val2", "val3", -1, "val1", "val2", "val3", 0),
      Seq("id4", 1l, null, null, null, 3, "default", null, null, 3)
    ),
    ListMap(
      dataModel.idColumn -> (StringType, true),
      "timestamp" -> (LongType, true),
      "col1" -> (StringType, true),
      "col2" -> (StringType, true),
      "col3" -> (StringType, true),
      "col4" -> (IntegerType, true),
      "expected_col1" -> (StringType, true),
      "expected_col2" -> (StringType, true),
      "expected_col3" -> (StringType, true),
      "expected_col4" -> (IntegerType, true)
    )
  )

  val expectedDF = testDF
    .drop("col1", "col2", "col3", "col4")
    .withColumnRenamed("expected_col1", "col1")
    .withColumnRenamed("expected_col2", "col2")
    .withColumnRenamed("expected_col3", "col3")
    .withColumnRenamed("expected_col4", "col4")

  "StaticValuesStandardizerTest" should "standardize static columns by object" in {

    val unknownValues = Seq("unknown", "undefined")

    val transformedDF = testDF
      .drop("expected_col1", "expected_col2", "expected_col3", "expected_col4")
      .enrichWithArlas(
        new StaticColumnsStandardizer(dataModel.idColumn,
                                      Map("col1" -> ("default", unknownValues), "col2" -> (null, unknownValues), "col4" -> (0, Seq(-1)))))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
