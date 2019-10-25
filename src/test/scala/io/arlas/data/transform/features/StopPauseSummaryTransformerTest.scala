package io.arlas.data.transform.features

import io.arlas.data.transform._
import io.arlas.data.sql._

class StopPauseSummaryTransformerTest extends ArlasTest {

  val transformedDF = getStopPauseSummaryBaseDF
    .enrichWithArlas(
      new StopPauseSummaryTransformer(
        dataModel,
        standardDeviationEllipsisNbPoints,
        tempoSalvo,
        tempoIrregular,
        tempoProportionsColumns,
        averagedColumns
      ))

  "StopPauseSummaryTransformer transformation" should "aggregate the stop-pause fragments against dataframe's timeseries" in {

    val expectedDF = stopPauseSummaryDF

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
