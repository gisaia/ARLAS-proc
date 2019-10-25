package io.arlas.data.sql

import io.arlas.data.transform.{ArlasTest, DataFrameException}
import io.arlas.data.transform.ArlasTransformerColumns.arlasTimestampColumn
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.apache.spark.sql.functions._
import io.arlas.data.transform.ArlasTestHelper._
import scala.collection.immutable.ListMap

class WritableDataFrameTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", 0l, 0.123, "toulouse"),
      Seq("id1", 30l, 1.234, "blagnac")
    ),
    ListMap(
      "id" -> (StringType, true),
      arlasTimestampColumn -> (LongType, true),
      "speed" -> (DoubleType, true),
      "city" -> (StringType, true)
    )
  )

  "withColumnsNested" should "create structures recursively" in {

    val expectedDF =
      testDF.withColumn("user", struct(struct(col("city").as("address_city")).as("address")))
    val transformedDF =
      testDF.withColumnsNested(
        Map(
          "user" ->
            ColumnGroup("address" ->
              ColumnGroup("address_city" -> "city"))))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withColumnsNested" should "be able to use columns objects" in {

    val speedCityColumn = concat(col("speed"), lit("-"), col("city"))
    val castedSpeed = col("speed").cast(StringType)
    val expectedDF = testDF
      .withColumn("cast", struct(speedCityColumn.as("speed_city"), castedSpeed.as("casted_speed")))

    val transformedDF =
      testDF.withColumnsNested(
        Map("cast" ->
          ColumnGroup("casted_speed" -> castedSpeed, "speed_city" -> speedCityColumn)))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "withColumnsNested" should "fail if a column already exists with one of the basic structures names" in {

    val thrown = intercept[DataFrameException] {
      testDF.withColumnsNested(
        Map(
          "city" -> ColumnGroup("new_city" -> "city")
        ))
    }
    assert(
      thrown.getMessage === "ColumnGroup city cannot be created because a column already exists with this name")
  }

}
