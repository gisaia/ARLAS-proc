package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import org.apache.spark.sql.types.{LongType, StringType}
import io.arlas.data.sql._
import scala.collection.immutable.ListMap

class StaticColumnsStandardizerTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 1l, "unknown", "val2", "val3", "val1", "val2", "val3"),
      Seq("id1", 2l, "val1", "val2", "val3", "val1", "val2", "val3"),
      Seq("id1", 3l, "undefined", "undefined", "unknown", "val1", "val2", "unknown"),
      Seq("id1", 4l, null, null, null, "val1", "val2", null),
      Seq("id2", 1l, "undefined", "notset", "unknown", StaticColumnsStandardizer.DEFAULT_UNDEFINED, "notset", "unknown"),
      Seq("id3", 1l, "val1", "val2", "val3", "val1", "val2", "val3"),
      Seq("id4", 1l, null, null, null, StaticColumnsStandardizer.DEFAULT_UNDEFINED, StaticColumnsStandardizer.DEFAULT_UNDEFINED, null)
    ),
    ListMap(
      dataModel.idColumn -> (StringType, true),
      "timestamp" -> (LongType, true),
      "col1" -> (StringType, true),
      "col2" -> (StringType, true),
      "col3" -> (StringType, true),
      "expected_col1" -> (StringType, true),
      "expected_col2" -> (StringType, true),
      "expected_col3" -> (StringType, true)
    )
  )

  val expectedDF = testDF
    .drop("col1", "col2", "col3")
    .withColumnRenamed("expected_col1", "col1")
    .withColumnRenamed("expected_col2", "col2")
    .withColumnRenamed("expected_col3", "col3")

  "StaticValuesStandardizerTest" should "standardize static columns by object" in {

    val unknownValues = Seq("unknown", "undefined")

    val transformedDF = testDF
      .drop("expected_col1", "expected_col2", "expected_col3")
      .enrichWithArlas(new StaticColumnsStandardizer(dataModel, Map("col1" -> unknownValues, "col2" -> unknownValues)))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
