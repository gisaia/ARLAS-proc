package io.arlas.data.transform.timeseries

import io.arlas.data.model.DataModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

class WithStateIdOnStateChangeOrUnique(dataModel: DataModel,
                                       stateColumn: String,
                                       orderColumn: String,
                                       targetIdColumn: String,
                                       uniqueState: String)
    extends WithStateId(
      dataModel,
      orderColumn,
      targetIdColumn, {
        val window = Window.partitionBy(dataModel.idColumn).orderBy(orderColumn)
        lag(stateColumn, 1)
          .over(window)
          .notEqual(col(stateColumn))
          .or(lag(stateColumn, 1).over(window).isNull)
          .or(col(stateColumn).equalTo(uniqueState))
      }
    )
