package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper.createDataFrameWithTypes
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType}
import io.arlas.data.sql._
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap

class OutliersRemoverTest extends ArlasTest {

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq("id1", "01/06/2018 00:00:00+02:00", 57.21679, 8.310548, false),
      Seq("id1", "01/06/2018 00:00:01+02:00", 57.21704, 8.311482, false),
      Seq("id1", "01/06/2018 00:00:02+02:00", 57.217257, 8.312332, false),
      Seq("id1", "01/06/2018 00:00:03+02:00", 57.217523, 8.313265, false),
      Seq("id1", "01/06/2018 00:00:04+02:00", 58.217773, 8.314198, true),
      Seq("id1", "01/06/2018 00:00:05+02:00", 57.218057, 8.315232, false),
      Seq("id1", "01/06/2018 00:00:06+02:00", 57.218323, 8.316165, false),
      Seq("id1", "01/06/2018 00:00:07+02:00", 57.21859, 9.317097, true),
      Seq("id1", "01/06/2018 00:00:08+02:00", 57.21884, 8.318032, false),
      Seq("id1", "01/06/2018 00:00:09+02:00", 57.219072, 8.318865, false),
      Seq("id1", "01/06/2018 00:00:10+02:00", 56.719338, 8.319798, true),
      Seq("id1", "01/06/2018 00:00:11+02:00", 57.219605, 8.320732, false),
      Seq("id1", "01/06/2018 00:00:12+02:00", 57.219907, 8.321765, false),
      Seq("id1", "01/06/2018 00:00:13+02:00", 57.220173, 8.322698, false),
      Seq("id1", "01/06/2018 00:00:14+02:00", 57.22044, 7.823632, true),
      Seq("id1", "01/06/2018 00:00:15+02:00", 58.020673, 9.124482, true),
      Seq("id1", "01/06/2018 00:00:16+02:00", 57.220957, 8.325398, false),
      Seq("id1", "01/06/2018 00:00:17+02:00", 57.221207, 8.326332, false),
      Seq("id1", "01/06/2018 00:00:18+02:00", 57.22149, 8.327365, false),
      Seq("id1", "01/06/2018 00:00:19+02:00", 57.221722, 8.328212, false),
      Seq("id2", "01/06/2018 00:00:00+02:00", 57.21679, 8.310548, false),
      Seq("id2", "01/06/2018 00:00:01+02:00", 57.21704, 8.311482, false),
      Seq("id2", "01/06/2018 00:00:02+02:00", 57.217257, 8.312332, false),
      Seq("id2", "01/06/2018 00:00:03+02:00", 57.217523, 8.313265, false),
      Seq("id2", "01/06/2018 00:00:04+02:00", 58.217773, 8.314198, true),
      Seq("id2", "01/06/2018 00:00:05+02:00", 57.218057, 8.315232, false),
      Seq("id2", "01/06/2018 00:00:06+02:00", 57.218323, 8.316165, false),
      Seq("id2", "01/06/2018 00:00:07+02:00", 57.21859, 9.317097, false)
    ),
    ListMap(
      "id" -> (StringType, true),
      "timestamp" -> (StringType, true),
      "lat" -> (DoubleType, true),
      "lon" -> (DoubleType, true),
      "isOutlier" -> (BooleanType, true)
    )
  )

  "OutliersRemover" should "remove outliers" in {
    val transformedDF = testDF
      .enrichWithArlas(new OutliersRemover(dataModel, dataModel.idColumn, 5, 0.1))
      .drop("isOutlier")
    val expectedDF = testDF
      .filter(col("isOutlier").equalTo(false))
      .drop("isOutlier")

    assertDataFrameEquality(transformedDF, expectedDF)
  }
}
