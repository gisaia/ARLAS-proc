package io.arlas.data.transform.features

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.arlas.data.transform.ArlasTransformer
import io.arlas.data.utils.RestTool
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Get port info (Port name, Port country) if available from a given location
  * @param latColumn Name of the column containing latitude
  * @param lonColumn Name of the column containing longitude
  * @param targetPortColumn Name of the column to store Port name
  * @param targetCountryColumn Name of the column to store Port country
  * @param portServerUrl Url of the ARLAS-Server containing Port information
  * @param conditionColumn optional column name that be checked; transformation will be applied to each row whose value is 'true'.
  *                        If no conditionColumn, then this is applied to every row.
  * @param halfWindowBbox Half width (in degree) of the box around the port centroid to be associated to the port
  */
class WithPortData(latColumn: String,
                   lonColumn: String,
                   targetPortColumn: String,
                   targetCountryColumn: String,
                   portServerUrl: String,
                   conditionColumn: Option[String] = None,
                   halfWindowBbox: Double = 0.2)
    extends ArlasTransformer(Vector(lonColumn, latColumn)) {

  @transient lazy val MAPPER = new ObjectMapper().registerModule(DefaultScalaModule)

  val tmpAddressColumn = "tmp_address"
  val tmpPortColumn = "tmp_port"
  val tmpCountryColumn = "tmp_country"

  val getGeoDataUDF = udf((lat: Double, lon: Double) => {

    RestTool
      .getOrFailOnNotAvailable(getPortUrl(lat, lon, halfWindowBbox))
      .map(response => {
        val geoData = MAPPER.readValue(response, classOf[Response])
//        println("response:", response)
//        println("geoData:", geoData.hits)
        if (geoData.nbhits > 0) {
          Option(geoData.hits.head.data.port)
            .map(
              port =>
                Map(
                  tmpPortColumn -> port.PORT_NAME,
                  tmpCountryColumn -> port.COUNTRY
              ))
            .getOrElse(Map())
        } else {
          Map(tmpPortColumn -> "UNKNOWN", tmpCountryColumn -> "Unknown")
        }
      })
      .toOption
  })

  def getPortUrl(lat: Double, lon: Double, halfWindowBbox: Double) = {
    val url = portServerUrl +
      "?f=port.location:within:%s,%s,%s,%s"
        .format(lon - halfWindowBbox,
                lat - halfWindowBbox,
                lon + halfWindowBbox,
                lat + halfWindowBbox) +
      "&pretty=false&flat=false&size=10&from=0" +
      "&sort=geodistance:%s%s%s".format(lat, "%20", lon)
    println("url:", url)
    url
  }

  def whenConditionOtherwise(expr: Column, otherwise: Column = lit(null)) =
    if (conditionColumn.isDefined)
      when(col(conditionColumn.get).equalTo(lit(true)), expr).otherwise(otherwise)
    else expr

  override def transform(dataset: Dataset[_]): DataFrame = {

    //add all address fields to dataframe, if not present
    //this avoids a crash using a condition, if no existing (default) address can be found
    val withAddressDF =
      Seq(targetPortColumn, targetCountryColumn)
        .foldLeft(dataset.toDF()) { (newDF, col) =>
          if (!newDF.columns.contains(col)) newDF.withColumn(col, lit(null)) else newDF
        }
        .withColumn(
          tmpAddressColumn,
          //`explode(array(anUDF))` ensures that UDF is executed only once (see https://issues.apache.org/jira/browse/SPARK-17728)
          explode(array(whenConditionOtherwise(getGeoDataUDF(col(latColumn), col(lonColumn)))))
        )
        .withColumn(targetPortColumn,
                    whenConditionOtherwise(col(tmpAddressColumn + "." + tmpPortColumn),
                                           col(targetPortColumn)))
        .withColumn(targetCountryColumn,
                    whenConditionOtherwise(col(tmpAddressColumn + "." + tmpCountryColumn),
                                           col(targetCountryColumn)))
        .drop(tmpAddressColumn)
    withAddressDF
  }

  override def transformSchema(schema: StructType): StructType =
    checkSchema(schema)
      .add(StructField(targetPortColumn, StringType, true))
      .add(StructField(targetCountryColumn, StringType, true))
}

object WithPortData {
  //make these properties public for the user to use it
  val portNamePostfix = "port_name"
  val countryPostfix = "country"
}

@JsonIgnoreProperties(ignoreUnknown = true) case class Response(@JsonProperty hits: List[Hit],
                                                                @JsonProperty nbhits: Int)
@JsonIgnoreProperties(ignoreUnknown = true) case class Hit(@JsonProperty data: Data)
@JsonIgnoreProperties(ignoreUnknown = true) case class Data(@JsonProperty port: Port)
@JsonIgnoreProperties(ignoreUnknown = true) case class Port(
    @JsonProperty PORT_NAME: String,
    @JsonProperty COUNTRY: String
)
