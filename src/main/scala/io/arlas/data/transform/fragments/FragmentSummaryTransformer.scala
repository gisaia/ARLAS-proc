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

package io.arlas.data.transform.fragments

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.{arlasTrackTimestampStart, _}
import io.arlas.data.utils.GeoTool
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

/**
  * This will summarize (i.a. aggregate) some fragments within the input dataframe.
  * It means that, based on a condition, few rows will be aggregated into single rows
  * (where other rows not matching the condition will stay the same).
  * A prerequisite is a that input dataframe must be made of fragments.
  * The consequence of summarization is that some existing (fragment-related) fields
  * will be aggregated. For example, for 10 rows aggregated, the duration of the resulting
  * row will be the sum of the 10 rows.
  *
  * The aggregations to apply can be extended in a child class.
  *
  * In order to aggregate only some rows, a simple `groupBy` cannot be used because it will aggregate all.
  * The trick is to:
  * - mark rows that match the condition ("rows to aggregate")
  * - for each aggregation, mark the first row
  * - duplicate its first row (these will be the aggregation result rows)
  * - use a window excluding this new row, and summarize (apply all aggregations) on it
  * - finally delete other rows.
  *
  * A a consequence, a problem with the duplicated row is that not-aggregated fields keep their originating value, which is not reliable.
  * Our solution is to set null to every columns, except columns that are aggregated + columns from the so-called "propagated columns"
  *
  * @param dataModel Data model containing names of structuring columns (id, lat, lon, time)
  * @param irregularTempo value of the irregular tempo (i.a. greater than defined tempos, so there were probably pauses)
  * @param tempoProportionColumns Map of (tempo proportion column -> related tempo column)
  * @param weightAveragedColumns Columns to weight average over track duration, in aggregations
  */
abstract class FragmentSummaryTransformer(spark: SparkSession,
                                          dataModel: DataModel,
                                          irregularTempo: String = "tempo_irregular",
                                          tempoProportionColumns: Option[Map[String, String]] = None,
                                          weightAveragedColumns: Option[Seq[String]] = None,
                                          computePrecision: Boolean = false)
    extends ArlasTransformer(
      Vector(
        arlasTimestampColumn,
        arlasTrackTimestampStart,
        arlasTrackTimestampEnd,
        arlasTrackDuration,
        arlasTrackDynamicsGpsSpeed,
        arlasTrackDynamicsGpsBearing,
        arlasTrackTrail,
        arlasTrackDistanceGpsTravelled,
        arlasTrackNbGeopoints,
        arlasTrackLocationLat,
        arlasTrackLocationLon,
        arlasTrackEndLocationLat,
        arlasTrackEndLocationLon
      ) ++ (weightAveragedColumns match {
        case Some(weightAverageColumnsSeq) => weightAverageColumnsSeq
        case None                          => Seq()
      }) ++ (tempoProportionColumns match {
        case Some(tempoProportionColumnsMap) => tempoProportionColumnsMap.keys
        case None                            => Seq()
      })) {

  val tmpAggRowOrder = "tmp_agg_row_order"

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
    * To be overriden by child class to add columns to propagate
    * Columns added in `afterAggregations` and `getAggregateCondition` should be declared here
    * @return
    */
  def getPropagatedColumns(): Seq[String] = Seq()

  /**
    * To be overriden by child classes to add or replace columns, in aggregation results rows only.
    * These columns may be computed over a window, or not.
    * @param window
    * @return a ListMap (target field -> related Column instance)
    */
  def getAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] = ListMap()

  /**
    * To be overriden by child class to add some processing after the transformation
    * (i.a. when aggregation is done)
    * @param df
    * @return
    */
  def afterTransform(df: DataFrame): DataFrame = df

  /**
    * Get the columns that are specific to fragment summarization, to add or remplace in aggregation results only.
    * @param window
    * @return a ListMap (target field -> related Column instance)
    */
  private def getFragmentAggregatedRowsColumns(window: WindowSpec): ListMap[String, Column] = {

    val listMapTempo = tempoProportionColumns match {
      case Some(tempoProportionColumnsMap) =>
        ListMap(
          arlasTrackTempoEmissionIsMulti ->
            when(getNbTemposWithSignificantProportions(tempoProportionColumnsMap.filter(_._2 != irregularTempo).keys.toSeq, lit(0))
                   .gt(1),
                 lit(true))
              .otherwise(lit(false)),
          arlasTempoColumn -> getMainTempo(tempoProportionColumnsMap)
        )
      case None => ListMap.empty[String, Column]
    }

    val listMapPrecision = if (computePrecision) {
      ListMap(
        arlasTrackLocationPrecisionValueLat -> round(stddev_pop(arlasTrackLocationLat).over(window), GeoTool.LOCATION_PRECISION_DIGITS),
        arlasTrackLocationPrecisionValueLon -> round(stddev_pop(arlasTrackLocationLon).over(window), GeoTool.LOCATION_PRECISION_DIGITS)
      )
    } else {
      ListMap.empty[String, Column]
    }

    ListMap(
      arlasTrackDistanceGpsStraigthLine -> getStraightLineDistanceUDF(first(arlasTrackTrail).over(window),
                                                                      last(arlasTrackTrail).over(window)),
      arlasTrackDistanceGpsTravelled -> sum(arlasTrackDistanceGpsTravelled).over(window),
      arlasTrackDistanceGpsStraigthness -> col(arlasTrackDistanceGpsStraigthLine) / col(arlasTrackDistanceGpsTravelled),
      arlasTrackNbGeopoints -> (sum(arlasTrackNbGeopoints).over(window) - count(lit(1))
        .over(window) + lit(1)).cast(IntegerType),
      arlasTrackTimestampStart -> min(arlasTrackTimestampStart).over(window),
      arlasTrackTimestampEnd -> max(arlasTrackTimestampEnd).over(window),
      arlasTrackDuration -> sum(arlasTrackDuration).over(window),
      arlasTrackLocationLat -> round(mean(arlasTrackLocationLat).over(window), GeoTool.LOCATION_DIGITS),
      arlasTrackLocationLon -> round(mean(arlasTrackLocationLon).over(window), GeoTool.LOCATION_DIGITS),
      arlasTrackEndLocationLat -> last(arlasTrackEndLocationLat).over(window),
      arlasTrackEndLocationLon -> last(arlasTrackEndLocationLon).over(window),
      arlasTrackTimestampCenter -> ((col(arlasTrackTimestampStart) + col(arlasTrackTimestampEnd)) / 2)
        .cast(LongType),
      arlasTimestampColumn -> col(arlasTrackTimestampCenter),
      arlasTrackId -> concat(col(dataModel.idColumn), lit("#"), col(arlasTrackTimestampStart), lit("_"), col(arlasTrackTimestampEnd)),
    ) ++ listMapPrecision ++ listMapTempo
  }

  override def transformSchema(schema: StructType): StructType = {
    checkRequiredColumns(
      schema,
      Vector(getAggregationColumn()) ++
        getPropagatedColumns() ++
        (tempoProportionColumns match {
          case Some(tempoProportionColumnsMap) => tempoProportionColumnsMap.keys
          case None                            => Seq()
        })
    )

    val tempoSeq = tempoProportionColumns match {
      case Some(value) => Seq((arlasTrackTempoEmissionIsMulti, BooleanType), (arlasTempoColumn, StringType))
      case None        => Seq()
    }

    val precisionSeq = if (computePrecision) {
      Seq((arlasTrackLocationPrecisionValueLat, DoubleType),
          (arlasTrackLocationPrecisionValueLon, DoubleType),
          (arlasTrackLocationPrecisionGeometry, StringType))
    } else {
      Seq()
    }

    (precisionSeq ++ tempoSeq ++ Seq(
      (arlasTrackDistanceGpsStraigthLine, DoubleType),
      (arlasTrackDistanceGpsStraigthness, DoubleType),
      (arlasTrackId, StringType)
    )).foldLeft(checkSchema(schema)) { (sc, c) =>
      if (sc.fieldNames.contains(c._1)) sc
      else sc.add(StructField(c._1, c._2, true))
    }
  }

  override final def transform(dataset: Dataset[_]): DataFrame = {

    val orderWindow = Window
      .partitionBy(getAggregationColumn())
      .orderBy(arlasTrackTimestampStart)

    val window = Window
      .partitionBy(getAggregationColumn())
      .orderBy(tmpAggRowOrder)
      //exclude the first element, i.a. with row order = 0
      .rowsBetween(1, Window.unboundedFollowing)

    // `tmpAggRowOrder` is null for non-aggregated rows.
    // for aggregated rows, it starts from 1 to n.
    // Later, for each aggregated rows, we will add a row with the results of the aggregations.
    // We will set its `tmpAggRowOrder` to 0.
    // Then using a window starting at element 1, we will process the aggregations without considering this additional row.
    // At the end, only rows whose index is null or 0 will be kept.
    val baseDF = dataset
      .withColumn(tmpAggRowOrder, when(getAggregateCondition(), row_number().over(orderWindow)))

    val allColsNullableSchema = baseDF.schema.fields.foldLeft(new StructType()) { (sc, c) =>
      sc.add(StructField(c.name, c.dataType, true))
    }

    val aggKeepColumnsIndices =
      (getPropagatedColumns :+ getAggregationColumn)
        .map(baseDF.columns.indexOf(_))

    val withAggResultRowDF = baseDF
      .flatMap((r: Row) => {
        // Duplicate the first row by keeping only some columns.
        // This row will be the result of the aggregations.
        if (r.getAs[Int](tmpAggRowOrder) == 1) {

          val rSeq = r.toSeq
          val newRowSeq = rSeq.zipWithIndex.map {
            //pattern matching with _ instead of AnyVal also matches null values
            case (_, b: Int) => {
              //in aggregation results rows, row number is 0. So this will be the first row considered by the rowNumberIndex
              //and it will be excluded from computations as window.rowsBetween starts at 1
              if (b == r.schema.fieldNames.indexOf(tmpAggRowOrder)) 0
              else if (aggKeepColumnsIndices.contains(b)) rSeq(b)
              else null
            }
          }
          Seq(r, Row.fromSeq(newRowSeq))
        } else Seq(r)
      })(RowEncoder(allColsNullableSchema))

    // Weight average the columns by track_duration
    val weightAveragedDF = weightAveragedColumns match {
      case Some(weightAverageColumnsSeq) =>
        weightAverageColumnsSeq.foldLeft(withAggResultRowDF) { (df, spec) =>
          df.withColumn(
            spec,
            whenAggregateAndKeep((sum(col(spec) * col(arlasTrackDuration)).over(window) / sum(arlasTrackDuration).over(window)).as(spec))
              .otherwise(col(spec)))
        }
      case None => withAggResultRowDF
    }

    //process tempo proportions columns
    val tempoProportionsDF = tempoProportionColumns match {
      case Some(tempoProportionColumnsMap) =>
        tempoProportionColumnsMap.keys.foldLeft(weightAveragedDF) { (df, spec) =>
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
      case None => weightAveragedDF
    }

    val aggregatedDF =
      (getFragmentAggregatedRowsColumns(window) ++ getAggregatedRowsColumns(window))
        .foldLeft(tempoProportionsDF) { (df, spec) =>
          df.withColumn(
            spec._1,
            whenAggregateAndKeep(spec._2).otherwise(
              //for non aggregated rows, keep existing value if it exists, or null
              if (df.columns.contains(spec._1))
                col(spec._1)
              //please note that the aggregated column becomes nullable, if it wasn't (so the dataframe schema may change)
              else lit(null))
          )
        }
        .withColumn(arlasTrackDynamicsGpsSpeed, col(arlasTrackDistanceGpsTravelled) / col(arlasTrackDuration)) // Recompute gps speed for the new created fragments

    //keep only one result row for aggregations
    val transformedDF = aggregatedDF
      .filter(col(tmpAggRowOrder).isNull.or(col(tmpAggRowOrder).equalTo(lit(0))))
      .drop(tmpAggRowOrder)

    val afterTransformedDF = afterTransform(transformedDF)
      .withColumn(arlasPartitionColumn, // compute new arlas_partition value
                  from_unixtime(col(arlasTrackTimestampCenter), arlasPartitionFormat).cast(IntegerType))

    //apply the origin schema with new columns, i.a. not with only nullable columns
    // First, use the same column order (otherwise we should define the same order in
    //`transformSchema` as the new added columns, which is restrictive)
    val transformedSortedSchema =
      transformSchema(dataset.schema).fields.sortBy(_.name).foldLeft(new StructType()) {
        case (s, f) => s.add(f)
      }
    val originFields = afterTransformedDF.schema.fieldNames.sorted

    spark.createDataFrame(afterTransformedDF.select(originFields.head, originFields.tail: _*).rdd, transformedSortedSchema)
  }

  def whenAggregateAndKeep(expr: Column) = {
    when(col(tmpAggRowOrder).equalTo(lit(0)), expr)
  }

  val getStandardDeviationEllipsis = udf((latCenter: Double, lonCenter: Double, latStd: Double, lonStd: Double) => {
    GeoTool.getStandardDeviationEllipsis(latCenter, lonCenter, latStd, lonStd, 12)
  })

  val getStraightLineDistanceUDF = udf(
    (firstTrail: String, lastTrail: String) => GeoTool.getStraightLineDistanceFromTrails(Array(firstTrail, lastTrail)))

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
        when(greatestTempo.notEqual(0.0).and(greatestTempo.equalTo(col(firstTempo._1))), firstTempo._2)
          .otherwise(recursivelyGetMainTempo(tempoProportionColumns.tail))
      } else lit(irregularTempo)
    }

    recursivelyGetMainTempo(tempoProportionColumns)
  }

  /**
    * For each tempo, check if it has a significant proportion
    * @param tempoProportionsColumns
    * @param baseValue
    * @return
    */
  def getNbTemposWithSignificantProportions(tempoProportionsColumns: Seq[String], baseValue: Column): Column = {

    if (tempoProportionsColumns.size == 0) {
      baseValue
    } else {
      val head = tempoProportionsColumns.head
      getNbTemposWithSignificantProportions(tempoProportionsColumns.tail, baseValue + when(col(head).gt(lit(0.1)), 1).otherwise(0))
    }
  }

}
