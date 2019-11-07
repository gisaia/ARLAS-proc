package io.arlas.data.transform.features

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.arlas.data.transform.ArlasTransformer
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{expr, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import scala.util.Try

/**
  * Get geo data (address data) from geopoints.
  * Target (address) fields are to be specified.
  * A condition can be specified, in this case only rows matching this condition
  * will be searched. Existing values of other rows will be kept.
  * @param latColumn
  * @param lonColumn
  * @param addressColumnsPrefix
  * @param conditionColumn optional column name that be checked; transformation will be applied to each row whose value is 'true'.
  *                        If no conditionColumn, then this is applied to every row.
  * @param zoomLevel zoomlevel of the geodata webservice
  */
class WithGeoData(latColumn: String,
                  lonColumn: String,
                  addressColumnsPrefix: String,
                  conditionColumn: Option[String] = None,
                  zoomLevel: Int = 10)
    extends ArlasTransformer(Vector(lonColumn, latColumn)) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
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

  def getGeoData(lat: Double, lon: Double) = {
    val getGeoDataTry = Try {

      val response = scala.io.Source
        .fromURL(
          s"http://nominatim.services.arlas.io/reverse.php?format=json&lat=${lat}&lon=${lon}&zoom=${zoomLevel}")
        .mkString

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
    }
    if (getGeoDataTry.isFailure) {
      logger.info(this.getClass + " failed with " + getGeoDataTry.failed.get.getMessage)
    }
    getGeoDataTry.toOption
  }

  val getGeoDataUDF = udf(getGeoData _)

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
      .withColumn(cityColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCityColumn),
                                         col(cityColumn)))
      .withColumn(countyColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountyColumn),
                                         col(countyColumn)))
      .withColumn(stateColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpStateColumn),
                                         col(stateColumn)))
      .withColumn(countryColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountryColumn),
                                         col(countryColumn)))
      .withColumn(countryCodeColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountryCodeColumn),
                                         col(countryCodeColumn)))
      .withColumn(postcodeColumn,
                  whenConditionOtherwise(col(tmpAddressColumn + "." + tmpPostcodeColumn),
                                         col(postcodeColumn)))
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
