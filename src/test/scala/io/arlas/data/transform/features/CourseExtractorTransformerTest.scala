package io.arlas.data.transform.features

import io.arlas.data.sql._
import io.arlas.data.transform.ArlasTest

class CourseExtractorTransformerTest extends ArlasTest {

  val transformedDF = courseExtractorBaseDF
    .enrichWithArlas(
      new CourseExtractorTransformer(spark,
                                     dataModel,
                                     standardDeviationEllipsisNbPoints,
                                     tempoIrregular,
                                     tempoProportionsColumns,
                                     averagedColumns))

  "CourseExtractorTransformer transformation" should "aggregate the course fragments against dataframe's timeseries" in {

    val expectedDF = courseExtractorDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
