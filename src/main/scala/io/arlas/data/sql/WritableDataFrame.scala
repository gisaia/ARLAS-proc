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

package io.arlas.data.sql

import io.arlas.data.model.DataModel
import io.arlas.data.transform.ArlasTransformerColumns._
import io.arlas.data.transform.DataFrameException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.elasticsearch.spark.sql._

class WritableDataFrame(df: DataFrame) extends TransformableDataFrame(df) {

  val PARQUET_BLOCK_SIZE: Int = 256 * 1024 * 1024
  val arlasElasticsearchIdColumn = "arlas_es_id"

  def withColumnsNested(s: Map[String, ColumnGroup]): DataFrame = {

    //first, check no column exists with expected structures names
    s.keys
      .filter(df.columns.contains(_))
      .foreach(d =>
        throw new DataFrameException(s"ColumnGroup ${d} cannot be created because a column already exists with this" +
          s" name"))

    //recursively create a column or a structure
    def recursiveStructure(s: ColumnGroupingElement): Column = {
      s match {
        case ImplicitColumnName(v) => col(v)
        case ImplicitColumnObj(c)  => c
        case v: ColumnGroup => {
          struct(v.elements.map(m => recursiveStructure(m._2).as(m._1)).toSeq: _*)
        }
        case _ => lit(null)
      }
    }

    s.flatMap {
        case (c: String, v: ColumnGroup) => Seq((c, recursiveStructure(v)))
      }
      .foldLeft(df) { (accDF, c) =>
        accDF.withColumn(c._1, c._2)
      }
  }

  /**
    * @param target Path of the parquet file to store data
    * @param saveMode Mode of storage: "SaveMode.Append" (add data to parquet file) or "SaveMode.Overwrite" (erase existing data)
    */
  def writeToParquet(target: String, saveMode: SaveMode = SaveMode.Append): Unit = {
    df.repartition(col(arlasPartitionColumn))
      .write
      .option("compression", "snappy")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE.toString)
      .mode(saveMode)
      .partitionBy(arlasPartitionColumn)
      .parquet(target)
  }

  def asArlasEsData(dataModel: DataModel): DataFrame = {
    df.withColumn(arlasGeoPointColumn, concat(col(dataModel.latColumn), lit(","), col(dataModel.lonColumn)))
      .withColumn(arlasIdColumn, concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
  }

  def writeToElasticsearch(spark: SparkSession, dataModel: DataModel, target: String): Unit = {
    df.withColumn(arlasElasticsearchIdColumn, concat(col(dataModel.idColumn), lit("#"), col(arlasTimestampColumn)))
      .saveToEs(target, Map("es.mapping.id" -> arlasElasticsearchIdColumn))
  }

  /**
    * Write to multiple elasticsearch indices.
    * Eg. with target="my_index_{}/doc" and dynamicIndexColumn="month_col"
    * This will save the dataframe to indices like "my_index_201901", "my_index_201902" aso.
    * @param spark
    * @param esIdColName name of the ES ID column. It may be in a struct, eg. "in_struct.id"
    * @param target it should be like "mapping—{}/type", with "{}" to be replaced by the dynamicIndexColumn, eg. "my_mapping_{}/doc"
    * @param dynamicIndexColumn the column to use into the index_pattern, eg. with
    * @param mappingExcluded columns that should not be indexed
    */
  def writeToElasticsearch(spark: SparkSession,
                           esIdColName: String,
                           target: String,
                           dynamicIndexColumn: Column,
                           mappingExcluded: Seq[String] = Seq()): Unit = {

    df.withColumn("dynamicIndex", dynamicIndexColumn)
      .saveToEs(target.replace("{}", "{dynamicIndex}"),
                Map("es.mapping.id" -> esIdColName, "es.mapping.exclude" -> (mappingExcluded :+ "dynamicIndex").mkString(",")))
  }

  def writeToCsv(target: String, delimiter: String = ";", toSingleFile: Boolean = true, saveMode: SaveMode = SaveMode.Overwrite) = {

    val coalescedDF = if (toSingleFile) df.coalesce(1) else df

    //stringify array columns
    val arrayColumns = df.schema.fields.filter(_.dataType.isInstanceOf[ArrayType]).map(_.name)
    val withoutArrayDF = arrayColumns.foldLeft(coalescedDF) {
      case (df, c) => {
        df.withColumn(c, concat(lit("["), concat_ws(",", col(c)), lit("]")))
      }
    }

    withoutArrayDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .mode(saveMode)
      .save(target)
  }

}
