/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform.features

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.RestTool
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import java.util.Locale

/**
  * Get geo data (address data) from geopoints.
  * Target (address) fields are to be specified.
  * A condition can be specified, in this case only rows matching this condition
  * will be searched. Existing values of other rows will be kept.
  * @param geoServiceUrl url pattern that support variable insertion for lat/lon/zoom (ex http://mygeoservice.com/path?lat=%2f&lon=%2f&zoom=%s)
  * @param latColumn
  * @param lonColumn
  * @param addressColumnsPrefix
  * @param conditionColumn optional column name that be checked; transformation will be applied to each row whose value is 'true'.
  *                        If no conditionColumn, then this is applied to every row.
  * @param zoomLevel zoomlevel of the geodata webservice
  */
class WithGeoData(geoServiceUrl: String,
                   latColumn: String,
                  lonColumn: String,
                  addressColumnsPrefix: String,
                  conditionColumn: Option[String] = None,
                  zoomLevel: Int = 10)
    extends ArlasTransformer(Vector(lonColumn, latColumn)) {

  @transient lazy val MAPPER = new ObjectMapper().registerModule(DefaultScalaModule)

  val tmpCityColumn = "tmp_city"
  val tmpCountryColumn = "tmp_country"
  val tmpStateColumn = "tmp_state"
  val tmpCountyColumn = "tmp_county"
  val tmpPostcodeColumn = "tmp_postcode"
  val tmpCountryCodeColumn = "tmp_country_code"
  val tmpAddressColumn = "tmp_address"

  val cityColumn = addressColumnsPrefix + WithGeoData.cityPostfix
  val countryColumn = addressColumnsPrefix + WithGeoData.countryPostfix
  val countryCodeColumn = addressColumnsPrefix + WithGeoData.countryCodePostfix
  val countyColumn = addressColumnsPrefix + WithGeoData.countyPostfix
  val postcodeColumn = addressColumnsPrefix + WithGeoData.postcodePostfix
  val stateColumn = addressColumnsPrefix + WithGeoData.statePostfix

  val getGeoDataUDF = udf((lat: Double, lon: Double) => {

    RestTool
      .getOrFailOnNotAvailable(getGeodataUrl(geoServiceUrl, lat, lon, zoomLevel))
      .map(response => {
        val geoData = MAPPER.readValue(response, classOf[GeoData])

        Option(geoData.address)
          .map(address =>
            Map(
              tmpCityColumn -> address.city,
              tmpCountryColumn -> address.country,
              tmpStateColumn -> address.state,
              tmpCountyColumn -> address.county,
              tmpPostcodeColumn -> address.postcode,
              tmpCountryCodeColumn -> address.country_code
          ))
          .getOrElse(Map())
      })
      .toOption
  })

  def getGeodataUrl(geoServiceUrl: String, lat: Double, lon: Double, zoomLevel: Int): String = {
    // geoServiceUrl support variable insertion for lat/lon/zoom http://mygeoservice.com/path?lat=%2f&lon=%2f&zoom=%s
    // %2f ensures doubles aren't formatted like an exponential
    s"${geoServiceUrl}"
      .formatLocal(Locale.ENGLISH, lat, lon, zoomLevel)
  }

  def whenConditionOtherwise(expr: Column, otherwise: Column = lit(null)) =
    if (conditionColumn.isDefined)
      when(col(conditionColumn.get).equalTo(lit(true)), expr).otherwise(otherwise)
    else expr

  override def transform(dataset: Dataset[_]): DataFrame = {

    //add all address fields to dataframe, if not present
    //this avoids a crash using a condition, if no existing (default) address can be found
    val withAddressDF =
      Seq(cityColumn, countryColumn, countryCodeColumn, countyColumn, postcodeColumn, stateColumn)
        .foldLeft(dataset.toDF()) { (newDF, col) =>
          if (!newDF.columns.contains(col)) newDF.withColumn(col, lit(null)) else newDF
        }

    withAddressDF
      .withColumn(
        tmpAddressColumn,
        //`explode(array(anUDF))` ensures that UDF is executed only once (see https://issues.apache.org/jira/browse/SPARK-17728)
        explode(array(whenConditionOtherwise(getGeoDataUDF(col(latColumn), col(lonColumn)))))
      )
      .withColumn(cityColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCityColumn), col(cityColumn)))
      .withColumn(countyColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountyColumn), col(countyColumn)))
      .withColumn(stateColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpStateColumn), col(stateColumn)))
      .withColumn(countryColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountryColumn), col(countryColumn)))
      .withColumn(countryCodeColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountryCodeColumn), col(countryCodeColumn)))
      .withColumn(postcodeColumn, whenConditionOtherwise(col(tmpAddressColumn + "." + tmpPostcodeColumn), col(postcodeColumn)))
      .drop(tmpAddressColumn)
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema)
      .add(StructField(cityColumn, StringType, true))
      .add(StructField(countyColumn, StringType, true))
      .add(StructField(stateColumn, StringType, true))
      .add(StructField(countryColumn, StringType, true))
      .add(StructField(countryCodeColumn, StringType, true))
      .add(StructField(postcodeColumn, StringType, true))
}

object WithGeoData {
  //make these properties public for the user to use it
  val cityPostfix = "city"
  val countryPostfix = "country"
  val statePostfix = "state"
  val countyPostfix = "county"
  val postcodePostfix = "postcode"
  val countryCodePostfix = "country_code"
}

@JsonIgnoreProperties(ignoreUnknown = true) case class GeoData(@JsonProperty address: Address)
@JsonIgnoreProperties(ignoreUnknown = true) case class Address(
    @JsonProperty city: String,
    @JsonProperty county: String,
    @JsonProperty state: String,
    @JsonProperty country: String,
    @JsonProperty postcode: String,
    @JsonProperty country_code: String
)
