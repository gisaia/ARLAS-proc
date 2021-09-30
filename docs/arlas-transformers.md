# ARLAS TRANSFORMERS

## Usage
A processing pipeline relying on ARLAS-proc would look like :

```scala
import io.arlas.data.sql._
import io.arlas.data.model._
import io.arlas.data.transform._

val dataModel = DataModel(
  idColumn = "id",
  latColumn = "latitude",
  lonColumn = "longitude",
  timestampColumn = "timestamp",
  timeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
)

val actionable_data = readFromCsv(spark, ",", "/opt/proc/your-raw-data.csv")
  .asArlasFormattedData(dataModel)
  .process(new FlowFragmentMapper(dataModel, spark, dataModel.idColumn, List("speed", "bearing")))
.
..
```

The parameters of `process` method must extend `ArlasTransformer` abstract class.
With this method, you can chain as many transformers as you want to create a data processing pipeline.

ARLAS-proc library provides ready to use ArlasTransformers in `io.arlas.dfata.transform` package to easily build a data pipeline
able to transform raw geodata into rich trajectories.

You can also implement your own transformers by extending `ArlasTransformer` abstract class in your own project to fit your specific needs.
All you will need is to override `transform` method (and `transformSchema` method if you add/drop fields from the input dataframe).

## Models

ArlasTrasformers take Spark dataframes as input and output. Those dataframes can be filled with : 
- **geopoints** : each row represents a point
    - position (lat/lon)
    - date (timestamp)
    - object (id)
    - point metadatas
- **flow fragments** : each row represents a movement between 2 points
    - trail between 2 geopoints (geometry)
    - dates (departure and arrival timestamps)
    - object (id)
    - fragment metadatas
- **course fragments** : each row represents a movement between origin and destination locations
    - trail between several geopoints when object is moving (geometry)
    - dates (departure and arrival timestamps)
    - object (id)
    - course metadatas with origin and destination
- **mission fragments** : each row represents several movements and stops between origin and destination locations
    - trail between several geopoints when object is still or moving (geometry)
    - dates (departure and arrival timestamps)
    - object (id)
    - mission and course metadatas with origin and destination

## Scaladoc
The full scala documentation of ARLAS-proc is available [here](https://gisaia.github.io/ARLAS-proc).

## ARLAS-proc transformers

### Features - [io.arlas.data.transform.features._](https://gisaia.github.io/ARLAS-proc/latest/api/#io.arlas.data.transform.features.package)

| ArlasTransformer | input | output | transformation |
| ---------------- | ----- | ------ | -------------- |
| CourseExtractorTransformer | flow fragments with tempo | course fragments | Aggregate flow fragments of the same course in a single fragment |
| FlowFragmentMapper | geopoints | flow fragments | Aggregate two consecutive geopoints with the same id in a single fragment |
| MovingFragmentSampleSummarizer | flow fragments  | flow fragments | Aggregate all consecutive moving fragments with the same id in a several fragments sampled on a given duration | 
| StopPauseSummaryTransformer | flow fragments | flow fragments | Aggregate all consecutive still fragments with the same id in a single fragment |
| WithStandardTimestamp | geopoints/fragments | geopoints/fragments | Add a standard timestamp field |
| WithDurationFromId | geopoints/fragments | geopoints/fragments | Add a duration for consecutive geopoints/fragments with the same id |
| WithFragmentSampleId | flow fragments | flow fragments | Tag consecutive fragments with the same id so that the sum of fragments duration approximate a given sampling duration |
| WithFragmentVisibilityFromTempo | fragments | fragments | Tag consecutive fragments with visibility (appear/disappear/appear_disappear) according to tempo |
| WithGeoData | geopoints/fragments | geopoints/fragments | Add geodata from geopoint/fragment position thanks to an external service |
| WithGeohash | fragments | fragments | Add geohashes of every point from fragment trail |
| WithRoutingData | fragments | fragments | Add routing data from fragment trail |

### Machine Learning - [io.arlas.data.transform.ml._](https://gisaia.github.io/ARLAS-proc/latest/api/#io.arlas.data.transform.ml.package)

| ArlasTransformer | input | output | transformation |
| ---------------- | ----- | ------ | -------------- |
| HmmProcessor | geopoints/fragments | geopoints/fragments | Add HMM result from a given field of consecutive data with the same id |
| WithSupportValues | geopoints/fragments | geopoints/fragments | ? |

### Timeseries - [io.arlas.data.transform.timeseries._](https://gisaia.github.io/ARLAS-proc/latest/api/#io.arlas.data.transform.timeseries.package)

| ArlasTransformer | input | output | transformation |
| ---------------- | ----- | ------ | -------------- |
| IdUpdater | geopoints/fragments | geopoints/fragments | Update id to match id#earliestTimestamp_oldestTimestamp |
| WithStateId | geopoints/fragments | geopoints/fragments | Add the same id#earliestTimestamp for all consecutive rows with the same state for a same object |
| WithStateIdFromState | geopoints/fragments | geopoints/fragments | Add the same id for all consecutive rows between 2 occurrences of a given state for a same object |
| WithStateIdOnStateChange | geopoints/fragments | geopoints/fragments | Add the same id for all consecutive rows with the same state |
| WithTraversingMission | course fragments | course fragments | Add mission metadatas to all course fragments of the same mission |

### Tools - [io.arlas.data.transform.tools._](https://gisaia.github.io/ARLAS-proc/latest/api/#io.arlas.data.transform.tools.package)

| ArlasTransformer | input | output | transformation |
| ---------------- | ----- | ------ | -------------- |
| DataFrameFormatter | geopoints | geopoints | Check is mandatory columns exist and fix columns names and types |
| StaticColumnsStandardizer | geopoints | geopoints | Fill missing fields value in a geopoints sequence |
| WithGeometrySimplifier | geopoints/fragments | geopoints/fragments | Simplify a geometry field |