# Spark Application for Ingestion & Processing data  

- [Overview](#overview)
  * [Versions used in this project](#versions-used-in-this-project)
- [Prerequisites](#prerequisites)
  * [Cloudsmith configuration](#cloudsmith-configuration)
    + [For Intellij to download from Cloudsmith](#for-intellij-to-download-from-cloudsmith)
- [Build and deploy application JAR](#build-and-deploy-application-jar)
  * [Build locally](#build-locally)
  * [Deploy JAR to Cloudsmith](#deploy-jar-to-cloudsmith)
  * [Release](#release)
- [User guide](#user-guide)
  * [Test locally throught spark-shell](#test-locally-throught-spark-shell)
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

# Prerequisites

## Cloudsmith configuration

You need to set up the following environment variables first:
- `CLOUDSMITH_USER`
- `CLOUDSMITH_API_KEY` (see [https://cloudsmith.io/user/settings/api/])
- `CLOUDSMITH_PRIVATE_TOKEN` (see [https://cloudsmith.io/~gisaia/repos/private/entitlements/])

```bash
export CLOUDSMITH_USER="your-user"
export CLOUDSMITH_API_KEY="your-api-key"
export CLOUDSMITH_PRIVATE_TOKEN="your-private-token"
``` 

As these value are personal, you may add them to your `.bash_profile` file. This way you won't need to define them again.

### For Intellij to download from Cloudsmith

For Intellij to be able to use these environment variables (in order to download `arlas-ml`), in a terminal you should:

- have the environment variables defined
- start Intellij from this terminal: `idea`

# Build and deploy application JAR

## Build locally

```bash
# Build jar
sbt clean assembly
```

## Deploy JAR to Cloudsmith

```bash
sbt clean publish
```

## Release

Simply type:

`sbt release`

You will be asked for the versions to use for release & next version.

# User guide

## Test locally throught spark-shell

Start an interactive spark-shell session. For example :
```bash
docker run -ti \
       --network gisaia-network \
       -w /opt/work \
       -v ${PWD}:/opt/proc \
       -v $HOME/.m2:/root/.m2 \
       -v $HOME/.ivy2:/root/.ivy2 \
       -p "4040:4040" \
       gisaia/spark:latest \
       spark-shell \
        --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
        --exclude-packages javax.media:jai_core \
        --repositories http://repo.boundlessgeo.com/main,http://download.osgeo.org/webdav/geotools/ \
        --jars /opt/proc/target/scala-2.11/arlas-proc-assembly-0.4.0-SNAPSHOT.jar \
        --conf spark.es.nodes="gisaia-elasticsearch" \
        --conf spark.es.index.auto.create="true" \
        --conf spark.cassandra.connection.host="gisaia-scylla-db" \
        --conf spark.driver.allowMultipleContexts="true" \
        --conf spark.rpc.netty.dispatcher.numThreads="2" \
        --conf spark.driver.CLOUDSMITH_ML_MODELS_TOKEN="$CLOUDSMITH_PRIVATE_TOKEN" \
        --conf spark.driver.CLOUDSMITH_ML_MODELS_REPO="gisaia/private"
```

`spark.driver.CLOUDSMITH_ML_MODELS_REPO` and `spark.driver.CLOUDSMITH_ML_MODELS_TOKEN` are required when using ML models from Cloudsmith. 

CLOUDSMITH_ML_MODELS_REPO is the repo hosting the models (e.g. `gisaia/private`), and CLOUDSMITH_ML_MODELS_TOKEN is the token to use to download from this repository.

Paste (using `:paste`) the following code snippet :
```scala
    import io.arlas.data.sql._
    import io.arlas.data.model._
    import io.arlas.data.transform._

    val dataModel = DataModel(
      idColumn = "mmsi",
      latColumn = "latitude",
      lonColumn = "longitude",
      speedColumn = "sog",
      dynamicFields = Array("latitude", "longitude", "sog", "cog", "heading", "rot", "draught"),
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
