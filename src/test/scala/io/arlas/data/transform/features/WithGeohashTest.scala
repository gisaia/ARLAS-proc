package io.arlas.data.transform.features

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.functions._
import io.arlas.data.transform.ArlasTestHelper._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import io.arlas.data.transform.ArlasTransformerColumns._

class WithGeohashTest extends ArlasTest {

  val testDF =
    createDataFrameWithTypes(
      spark,
      List(
        Seq(
          "id1",
          "LINESTRING (11.782009 42.099761, 11.781858 42.099615, 11.782359 42.100029) ",
          Array("sr2rs0")
        )
      ),
      ListMap(
        "id" -> (StringType, true),
        "trail" -> (StringType, true),
        "expected_geohash" -> (ArrayType(StringType), true)
      )
    )

  val baseDF =
    testDF.drop("expected_geohash")

  "WithGeohash" should "get geohashes of trails" in {

    val expectedDF = testDF
      .withColumnRenamed("expected_geohash", arlasTrackTrailGeohashes)

    val transformedDF =
      baseDF.enrichWithArlas(new WithGeohash("trail", arlasTrackTrailGeohashes, 6))

    assertDataFrameEquality(transformedDF, expectedDF)
  }

  "WithGeohash" should "not fail with empty trail" in {

    val transformedDF =
      testDF
        .withColumn("trail", lit(null))
        .enrichWithArlas(new WithGeohash("trail", arlasTrackTrailGeohashes, 6))

    assert(
      transformedDF
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](arlasTrackTrailGeohashes)
        .isEmpty)
  }

}
