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

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns.{arlasPartitionColumn, arlasPartitionFormat}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Compute a partition value based on the timestamp of observation. This partition is used to write df in parquet format
  *
  * @param timestampColumn Column containing the timestamp (unix) to use for partition
  * @param partitionFormat Format of the chosen partition, by default it's a daily partition ("yyyyMMdd")
  */
class WithArlasPartition(timestampColumn: String, partitionFormat: String = arlasPartitionFormat)
    extends ArlasTransformer(Vector(timestampColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset
      .toDF()
      .withColumn(arlasPartitionColumn, // compute new arlas_partition value
                  from_unixtime(col(timestampColumn), partitionFormat).cast(IntegerType))
  }

  override def transformSchema(schema: StructType): StructType = {
    checkSchema(schema)
      .add(StructField(arlasPartitionColumn, IntegerType, false))
  }
}
