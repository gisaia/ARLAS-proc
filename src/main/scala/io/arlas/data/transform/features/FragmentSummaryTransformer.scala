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

package io.arlas.data.transform.features

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.immutable.ListMap

abstract class FragmentSummaryTransformer(
    dataModel: DataModel,
    standardDeviationEllipsisNbPoint: Int,
    salvoTempo: String,
    irregularTempo: String,
    tempoProportionColumns: Map[String, String], //key = proportion column, value = related tempo column
    weightAveragedColumns: Seq[String], //columns to weight average in aggregations
    additionalPropagatedColumns: Seq[String] = Seq())
    extends ArlasTransformer(
      Vector(
        arlasTimestampColumn,
        arlasTrackTimestampStart,
        arlasTrackTimestampEnd,
        arlasTrackDuration,
        arlasTrackDynamicsGpsSpeedKmh,
        arlasTrackDynamicsGpsBearing,
        arlasTrackTrail,
        arlasTrackDistanceGpsTravelled,
        arlasTrackNbGeopoints,
        arlasTrackLocationLat,
        arlasTrackLocationLon,
        arlasTrackDistanceSensorTravelled
      ) ++ weightAveragedColumns ++ tempoProportionColumns.keys) {

  /**
    * Return the column on which is done the `group by`
    * @return
    */
  def getAggregationColumn(): String

  /**
    * Apply aggregation only on given rows that match this method returned value
    * @return
    */
  def getAggregateCondition(): Column

  /**
    * Should contain the columns used in aggregation condition, it will be used for checking
    * @return
    */
  def getAggregateConditionColumns(): Seq[String]

  /**
    * To be overriden by child class to add columns to propagate
    * Columns added in `afterAggregations` should be declared here
    * @return
    */
  def propagatedColumns(): Seq[String] = Seq()

  /**
    * To be overriden by child classes to add some aggregations
    * @param window
    * @return a ListMap where key = (target row index, target row name) and value = resulting column
    *         Row index should be carrefully choosen because in some cases you may aggregate on a window
    *         whose first row has already been aggregated, which will give bad result.
    *         With row index, you can order the aggregations to avoid it.
    */
  def additionalAggregations(window: WindowSpec): Map[(Int, String), Column] = Map()

  /**
    * To be overriden by child classes to add some processing after the aggregation
    * @param df
    * @return
    */
  def afterAggregations(df: DataFrame): DataFrame = df

  /**
    * To be overriden by child class to add some processing after the transformation
    * (i.a. when aggregation is done)
    * @param df
    * @return
    */
  def afterTransform(df: DataFrame): DataFrame = df

  val tmpKeepColumn = "tmp_keep"

  private def getFragmentSummaryAggregations(window: WindowSpec): Map[(Int, String), Column] =
    Map(
      (100, arlasTrackDistanceGpsStraigthLine) -> getStraightLineDistanceUDF(
        first(arlasTrackTrail).over(window),
        last(arlasTrackTrail).over(window)),
      (100, arlasTrackDistanceGpsTravelled) -> sum(arlasTrackDistanceGpsTravelled).over(window),
      (110, arlasTrackDistanceGpsStraigthness) -> col(arlasTrackDistanceGpsStraigthLine) / col(
        arlasTrackDistanceGpsTravelled),
      (100, arlasTrackNbGeopoints) -> (sum(arlasTrackNbGeopoints).over(window) - count(lit(1))
        .over(window) + lit(1)).cast(IntegerType),
      (100, arlasTrackTimestampStart) -> min(arlasTrackTimestampStart).over(window),
      (110, arlasTimestampColumn) -> col(arlasTrackTimestampStart),
      (100, arlasTrackTimestampEnd) -> (max(arlasTrackTimestampEnd).over(window)).cast(LongType),
      (100, arlasTrackDuration) -> sum(arlasTrackDuration).over(window),
      (100, arlasTrackLocationPrecisionValueLat) -> round(
        stddev_pop(arlasTrackLocationLat).over(window),
        GeoTool.LOCATION_PRECISION_DIGITS),
      (100, arlasTrackLocationPrecisionValueLon) -> round(
        stddev_pop(arlasTrackLocationLon).over(window),
        GeoTool.LOCATION_PRECISION_DIGITS),
      (110, arlasTrackLocationLat) -> round(mean(arlasTrackLocationLat).over(window),
                                            GeoTool.LOCATION_DIGITS),
      (110, arlasTrackLocationLon) -> round(mean(arlasTrackLocationLon).over(window),
                                            GeoTool.LOCATION_DIGITS),
      (100, arlasTrackDistanceSensorTravelled) -> sum(arlasTrackDistanceSensorTravelled).over(
        window),
      (110, arlasTrackTimestampCenter) -> ((col(arlasTrackTimestampStart) + col(
        arlasTrackTimestampEnd)) / 2)
        .cast(LongType),
      (110, arlasTrackId) -> concat(col(dataModel.idColumn),
                                    lit("#"),
                                    col(arlasTrackTimestampStart),
                                    lit("_"),
                                    col(arlasTrackTimestampEnd)),
      (120, arlasTrackLocationPrecisionGeometry) -> getStandardDeviationEllipsis(
        col(arlasTrackLocationLat),
        col(arlasTrackLocationLon),
        col(arlasTrackLocationPrecisionValueLat),
        col(arlasTrackLocationPrecisionValueLon)),
      (100, arlasTrackTempoEmissionIsMulti) ->
        when(getNbTemposWithSignificantProportions(
               tempoProportionColumns.filter(_._2 != irregularTempo).keys.toSeq,
               lit(0))
               .gt(1),
             lit(true))
          .otherwise(lit(false)),
      (100, arlasTempoColumn) -> getMainTempo(tempoProportionColumns)
    )

  override def transformSchema(schema: StructType): StructType = {
    checkRequiredColumns(
      schema,
      Vector(getAggregationColumn()) ++ getAggregateConditionColumns() ++ additionalPropagatedColumns ++ propagatedColumns() ++ tempoProportionColumns.keys)

    Seq(
      (arlasTrackId, StringType),
      (arlasTrackLocationPrecisionValueLat, DoubleType),
      (arlasTrackLocationPrecisionValueLon, DoubleType),
      (arlasTrackLocationPrecisionGeometry, StringType),
      (arlasTrackDistanceGpsStraigthLine, DoubleType),
      (arlasTrackDistanceGpsStraigthness, DoubleType),
      (arlasTempoColumn, StringType),
      (arlasTrackTempoEmissionIsMulti, BooleanType)
    ).foldLeft(checkSchema(schema)) { (sc, c) =>
      if (sc.fieldNames.contains(c._1)) sc
      else sc.add(StructField(c._1, c._2, true))
    }
  }

  override final def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window
      .partitionBy(getAggregationColumn())
      .orderBy(arlasTrackTimestampStart)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    //Mark the rows to keep at the end of the processing.
    //For all distinct value of `groupByColumn()` that matches `aggregateConditionColumn()`,
    // a single row should be kept (arbitrarily the first is kept)
    //For rows that do not match `aggregateConditionColumn()`, all rows are kept
    val baseDF = dataset
      .withColumn(
        tmpKeepColumn,
        whenAggregate(
          when(first(arlasTrackTimestampStart).over(window).equalTo(col(arlasTrackTimestampStart)),
               lit(true))).otherwise(lit(true))
      )

    //weight average the columns by track_duration
    val weightAveragedDF = weightAveragedColumns.foldLeft(baseDF) { (df, spec) =>
      df.withColumn(spec,
                    whenAggregateAndKeep(
                      (sum(col(spec) * col(arlasTrackDuration)).over(window) / sum(
                        arlasTrackDuration).over(window)).as(spec)).otherwise(col(spec)))
    }

    //process tempo proportions columns
    val tempoProportionsDF = tempoProportionColumns.keys.foldLeft(weightAveragedDF) { (df, spec) =>
      df.withColumn(
        spec,
        whenAggregateAndKeep(
          sum(col(spec) * col(arlasTrackDuration))
            .over(window)
            .divide(sum(arlasTrackDuration).over(window))
            .as(spec)).otherwise(
          col(spec)
        )
      )
    }

    //applying the aggregation make the related column nullable, so this impacts the schema
    val aggregateddDF =
      ListMap(
        (getFragmentSummaryAggregations(window) ++ additionalAggregations(window)).toSeq
        //order target rows by their index
          .sortBy(r => (r._1._1)): _*)
        .foldLeft(tempoProportionsDF) { (df, spec) =>
          df.withColumn(
            spec._1._2,
            whenAggregateAndKeep(spec._2).otherwise(
              //for non aggregated rows, keep  existing value if exists, or null
              if (df.columns.contains(spec._1._2))
                col(spec._1._2)
              else lit(null))
          )
        }

    val afterAggregationsDF = afterAggregations(aggregateddDF)

    //for aggregated rows, set some columns to null
    //transformed columns and static columns should be kept, but some other columns
    //may still have the value of the origin row, that we want to hide.
    val keepColums = additionalPropagatedColumns ++ propagatedColumns() ++ tempoProportionColumns.keys ++ weightAveragedColumns ++ getFragmentSummaryAggregations(
      window).map(_._1._2).toSeq ++ additionalAggregations(window)
      .map(_._1._2)
      .toSeq :+ arlasPartitionColumn

    val aggregationsWithOnlyPropagatedAndAggregatedColsDF = afterAggregationsDF.columns
      .filterNot(
        (Seq(tmpKeepColumn, getAggregationColumn()) ++ getAggregateConditionColumns()).contains(_))
      .foldLeft(afterAggregationsDF) { (df, column) =>
        if (!keepColums.contains(column))
          df.withColumn(column, whenAggregateAndKeep(lit(null)).otherwise(col(column)))
        else df
      }

    //keep only one row for aggregations
    val transformedDF = aggregationsWithOnlyPropagatedAndAggregatedColsDF
      .filter(col(tmpKeepColumn).equalTo(lit(true)))
      .drop(tmpKeepColumn)
      .withColumn(arlasTimestampColumn, coalesce(col(arlasTimestampColumn), lit(0l))) //make arlasTimestampColumn not nullable

    afterTransform(transformedDF)
  }

  def whenAggregate(expr: Column) = {
    when(getAggregateCondition(), expr)
  }

  def whenAggregateAndKeep(expr: Column) = {
    when(getAggregateCondition().and(col(tmpKeepColumn).equalTo(lit(true))), expr)
  }

  val getStandardDeviationEllipsis = udf(
    (latCenter: Double, lonCenter: Double, latStd: Double, lonStd: Double) => {
      GeoTool.getStandardDeviationEllipsis(latCenter,
                                           lonCenter,
                                           latStd,
                                           lonStd,
                                           standardDeviationEllipsisNbPoint)
    })

  val getStraightLineDistanceUDF = udf((firstTrail: String, lastTrail: String) =>
    GeoTool.getStraightLineDistanceFromTrails(Array(firstTrail, lastTrail)))

  /**
    * compare recursively each tempo proportion column to the greatest proportion, to define which proportion is the highest,
    * @param tempoProportionColumns
    * @return
    */
  def getMainTempo(tempoProportionColumns: Map[String, String]): Column = {

    val regularTempoColumns =
      tempoProportionColumns.filter(_._2 != irregularTempo).keys.toSeq.map(col(_))

    val greatestTempo =
      if (regularTempoColumns.size > 1) greatest(regularTempoColumns: _*)
      else if (regularTempoColumns.size == 1) regularTempoColumns(0)
      else lit(0.0)

    def recursivelyGetMainTempo(tempoProportionColumns: Map[String, String]): Column = {

      if (tempoProportionColumns.size > 0) {
        val firstTempo = tempoProportionColumns.head
        //greatestTempo == 0.0 means only irregular
        when(greatestTempo.notEqual(0.0).and(greatestTempo.equalTo(col(firstTempo._1))),
             firstTempo._2)
          .otherwise(recursivelyGetMainTempo(tempoProportionColumns.tail))
      } else lit(irregularTempo)
    }

    recursivelyGetMainTempo(tempoProportionColumns)
  }

  /**
    * For each tempo, check if it has a significant proportion
    * @param tempoPropotionsColumns
    * @param baseValue
    * @return
    */
  def getNbTemposWithSignificantProportions(tempoPropotionsColumns: Seq[String],
                                            baseValue: Column): Column = {
    if (tempoPropotionsColumns.size == 0) {
      baseValue
    } else {
      val head = tempoPropotionsColumns.head
      getNbTemposWithSignificantProportions(
        tempoPropotionsColumns.tail,
        baseValue + when(col(head).gt(lit(0.1)), 1).otherwise(0))
    }
  }

}
