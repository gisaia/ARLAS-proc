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

package io.arlas.data.transform.timeseries

import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.transform.ArlasTransformerColumns._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * For a ID that is hold by multiple rows, it updates the id value as follows
  * <ObjectIdColumn>#<earliest timestamp with this id>_<oldest timestamp with this id>
  * Requires the arlas_track_timestamp_start and arlas_track_timestamp_end columns, produced by the FlowFragmentMapper transformer
  * @param idColumn Column containing the identifier to update
  * @param objectIdColumn Column containing the Object Identifier
  */
class IdUpdater(idColumn: String, objectIdColumn: String)
    extends ArlasTransformer(Vector(idColumn, arlasTrackTimestampStart, arlasTrackTimestampEnd, objectIdColumn)) {

  override def transform(dataset: Dataset[_]): DataFrame = {

    val window = Window
      .partitionBy(idColumn)
      .orderBy(arlasTrackTimestampStart)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    dataset
      .toDF()
      .withColumn(idColumn,
                  concat(col(objectIdColumn),
                         lit("#"),
                         first(arlasTrackTimestampStart).over(window),
                         lit("_"),
                         last(arlasTrackTimestampEnd).over(window)))
  }
}
