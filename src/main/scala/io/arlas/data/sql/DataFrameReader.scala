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

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DataFrameReader {

  def readFromCsv(spark: SparkSession, source: String, delimiter: String = ","): DataFrame = {
    readFromMulipleCsvPaths(spark, delimiter, true, None, source)
  }

  /**
  * Read from multiple CSV files
    * @param spark
    * @param delimiter column separator in the CSV
    * @param headers true if there are headers in CSV
    * @param schema optional schema
    * @param sources list of input files. ZIP files are supported, only if provided files names end with ".zip"
    * @return
    */
  def readFromMulipleCsvPaths(spark: SparkSession, delimiter: String, headers: Boolean, schema: Option[StructType], sources: String*): DataFrame = {

    import spark.implicits._
    val sourcesZipped: Seq[String] = sources.filter(_.toLowerCase().endsWith(".zip")).seq
    val isSourcesZip : Boolean     = sourcesZipped.nonEmpty

    if (isSourcesZip && sourcesZipped.length != sources.length) throw new RuntimeException("Cannot read from ZIP files among files of other type, the files " +
                                                                                           "must all be either ZIP, or other spark-supported formats")

    val sparkReader = spark.read
      .option("delimiter", delimiter)
      .option("header", headers)

    val sparkReaderWithPossiblySchema = if (schema.isDefined) sparkReader.schema(schema.get) else sparkReader

    def zippedSourcesToStringDataset = {
      spark.sparkContext.binaryFiles(sources.mkString(","))
        .flatMap { case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry)
            .takeWhile {
              case null => zis.close(); false
              case _ => true
            }
            .flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
        }
        .toDF().as[String]
    }

    if (isSourcesZip)
      sparkReaderWithPossiblySchema.csv(zippedSourcesToStringDataset)
    else
      sparkReaderWithPossiblySchema.csv(sources :_*)
  }

  def readFromParquet(spark: SparkSession, source: String) = {
    spark.read.parquet(source)
  }

  def readFromScyllaDB(spark: SparkSession, source: String): DataFrame = {
    val sourceKeyspace = source.split('.')(0)
    val sourceTable = source.split('.')(1)
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceTable, "keyspace" -> sourceKeyspace))
      .load()
  }
}
