# ARLAS-proc
Spark Library to ingest and process geodata timeseries  

- [Overview](#overview)
  * [Versions used in this project](#versions-used-in-this-project)
- [Build and deploy application JAR](#build-and-deploy-application-jar)
  * [Build locally](#build-locally)
  * [Deploy JAR to Cloudsmith](#deploy-jar-to-cloudsmith)
  * [Release](#release)
- [User guide](#user-guide)
  * [Test locally throught spark-shell](#test-locally-throught-spark-shell)
  * [Unit tests with external API](#unit-tests-with-external-api)
    + [Capture external API](#capture-external-api)
    + [Use mock server from scala tests](#use-mock-server-from-scala-tests)
- [Contributing](#contributing)
- [Authors](#authors)
- [License](#license)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

# Overview

A docker-compose project to:
 - setup a standalone Apache Spark running one master and one/multiple workers

A spark java application to submit a spark job on our spark cluster:
- Load, filter, and clean csv files
- Store loaded data in ScyllaDB table
- Load bunch of the data (list of MMSIs) from ScyllaDB table and process them according to a given pipeline of transformations (Resampling, calculate average, calculate distance)

## Versions used in this project
It's very important to check the version of spark being used, here we are using the following:   
- Spark 2.3.3 for Hadoop 2.7 with OpenJDK 8 (Java 1.8.0)
- Scala 2.11.8
- ScyllaDB 2.2.0
- Spark-cassandra-connector: 2.3.1-S_2.11

# Build and deploy application JAR

## Build locally

```bash
# Build jar
sbt clean assembly
```

## Deploy JAR to Cloudsmith

If you have sufficient permissions to our Cloudsmith repository, you can publish a SNAPSHOT build jar to Cloudsmith.

You need to set up the following environment variables first:
- `CLOUDSMITH_USER`
- `CLOUDSMITH_API_KEY` (see [https://cloudsmith.io/user/settings/api/])

```bash
export CLOUDSMITH_USER="your-user"
export CLOUDSMITH_API_KEY="your-api-key"
``` 

As these values are personal, you may add them to your `.bash_profile` file. This way you won't need to define them again.


```bash
sbt clean publish
```

## Release

If you have sufficient permissions on Github repository, simply type:

`sbt clean release`

You will be asked for the versions to use for release & next version.

# User guide

## Test locally throught spark-shell

Start an interactive spark-shell session. For example :
```bash
sbt clean assembly
docker run -ti \
       -w /opt/work \
       -v ${PWD}:/opt/proc \
       -v $HOME/.m2:/root/.m2 \
       -v $HOME/.ivy2:/root/.ivy2 \
       -p "4040:4040" \
       gisaia/spark:2.3.3 \
       spark-shell \
        --packages org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
        --exclude-packages javax.media:jai_core \
        --repositories https://repo.osgeo.org/repository/release/,https://dl.cloudsmith.io/public/gisaia/public/maven/,https://repository.jboss.org/maven2/ \
        --jars /opt/proc/target/scala-2.11/arlas-proc-assembly-0.6.1-SNAPSHOT.jar \
        --conf spark.driver.allowMultipleContexts="true" \
        --conf spark.rpc.netty.dispatcher.numThreads="2"
```

Paste (using `:paste`) the following code snippet :
```scala
    import io.arlas.data.sql._
    import io.arlas.data.model._
    import io.arlas.data.transform._

    val dataModel = DataModel(
      idColumn = "mmsi",
      latColumn = "latitude",
      lonColumn = "longitude",
      timeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
    )
    val period = getPeriod(
      start = "2018-01-01T00:00:00+00:00",
      stop = "2018-01-02T00:00:00+00:00"
    )

    // extract and format raw data
    val data = readFromCsv(spark, ",", "/opt/proc/your-input.csv")
      .asArlasFormattedData(dataModel)
      

    // then apply the transformers to test    
```

## Unit tests with external API

External APIs are mocked using Wiremock. Wiremock has 2 benefits:

- using a JAR, we can capture every call to an API and save the results for further use
- then from scala tests, we can start a wiremock server and get these results.

### Capture external API 

Download the standalone JAR from `http://repo1.maven.org/maven2/com/github/tomakehurst/wiremock-standalone/2.25.1/wiremock-standalone-2.25.1.jar` and save it into the `src/test/resources/wiremock` folder.

Launch the JAR by replacing `https://external.api.com` with your own API: 

```bash
java -jar wiremock-standalone-2.25.1.jar --verbose --proxy-all="https://external.api.com" --record-mappings
```

Then in order to save the API results, change the API url to `http://localhost:8080` within the requests.

For example, to save nominatim results, you can do:
```bash
java -jar wiremock-standalone-2.25.1.jar --verbose --proxy-all="http://nominatim.services.arlas.io" --record-mappings
curl "http://localhost:8080/reverse.php?format=json&lat=41.270568&lon=6.6701225&zoom=10"
```

The results will be saved into the resources folder, which is used by scala tests.

### Use mock server from scala tests

A test class can extend the trait `ArlasMockServer`, which automatically starts and stops the mock server.

# Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting us pull requests.

# Authors

[List of contributors](https://github.com/gisaia/ARLAS-proc/graphs/contributors)

# License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
