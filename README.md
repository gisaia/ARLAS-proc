# Spark Application for Ingestion & Processing data  

- [Spark Application for Ingestion & Processing data](#spark-application-for-ingestion---processing-data)
- [Overview](#overview)
  * [Versions used in this project](#versions-used-in-this-project)
- [Prerequisites](#prerequisites)
  * [Cloudsmith configuration](#cloudsmith-configuration)
    + [For Intellij to download from Cloudsmith](#for-intellij-to-download-from-cloudsmith)
- [Build and deploy application JAR](#build-and-deploy-application-jar)
  * [Build locally](#build-locally)
  * [Deploy JAR to Cloudsmith](#deploy-jar-to-cloudsmith)
    + [Deploy thin JAR](#deploy-thin-jar)
    + [Deploy fat JAR](#deploy-fat-jar)
- [Integration tests](#integration-tests)
- [User guide](#user-guide)
  * [Start spark-shell locally](#start-spark-shell-locally)
  * [Start spark-shell on GCP](#start-spark-shell-on-gcp)
  * [Spark-shell example](#spark-shell-example)
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
- Spark 2.3.1 for Hadoop 2.7 with OpenJDK 8 (Java 1.8.0)
- Scala 2.11.2
- ScyllaDB 2.2.0
- Spark-cassandra-connector: 2.3.0-S_2.11 

[Check Spark Dockerfile](scripts/tests/spark/Dockerfile)

[Check Spark/ScyllaDB YAML](scripts/tests/docker-compose-standalone.yml)

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

### Deploy thin JAR

```bash
sbt clean publish
```

### Deploy fat JAR
 
This deploys a fat jar, ready to be used from GCP Dataproc to start processing.

```bash
sbt clean "project arlasProcAssembly" publish
```

## Release

Simply type:

`sbt release`

You will be asked for the versions to use for release & next version.

# Integration tests

```bash
./scripts/tests/test.sh [cluster]
```

# User guide

## Start spark-shell locally

Start ScyllaDB and Elasticsearch clusters. For example : 
```bash
docker-compose -f scripts/tests/docker-compose-standalone.yml up -d --force-recreate gisaia-scylla-db gisaia-elasticsearch
```

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
        --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.elasticsearch:elasticsearch-spark-20_2.11:6.4.0,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
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

## Start spark-shell on GCP

Once the cluster is started, open an SSH session to the master node.

First, [you should set `CLOUDSMITH_PRIVATE_TOKEN`](#prerequisites) in the master node.

Then copy-paste the following:

```bash
spark-shell \
        --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.elasticsearch:elasticsearch-spark-20_2.11:6.4.0,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
        --exclude-packages javax.media:jai_core \
        --repositories http://repo.boundlessgeo.com/main,http://download.osgeo.org/webdav/geotools/ \
        --jars https://dl.cloudsmith.io/CLOUDSMITH_PRIVATE_TOKEN/gisaia/arlas/maven/io/arlas/arlas-proc-assembly_2.11/0.4.0-SNAPSHOT/arlas-proc-assembly_2.11-0.4.0-SNAPSHOT.jar \
        --conf spark.es.nodes="gisaia-elasticsearch" \
        --conf spark.es.index.auto.create="true" \
        --conf spark.cassandra.connection.host="gisaia-scylla-db" \
        --conf spark.driver.allowMultipleContexts="true" \
        --conf spark.rpc.netty.dispatcher.numThreads="2" \
        --conf spark.driver.CLOUDSMITH_ML_MODELS_TOKEN="$CLOUDSMITH_PRIVATE_TOKEN" \
        --conf spark.driver.CLOUDSMITH_ML_MODELS_REPO="gisaia/private"
```

You may also use a specific hosted JAR, eg. `arlas-proc-assembly_2.11-0.4.0-20190717.101238-7.jar`

## Spark-shell example

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
    val data = readFromCsv(spark, "/opt/proc/scripts/tests/resources/ais-sample-data-1.csv")
      .asArlasFormattedData(dataModel)
      

    // transform and resample data
    val transformedData = data
      .asArlasVisibleSequencesFromTimestamp(dataModel, 120)
    
    // save result in ScyllaDB
    transformedData.writeToScyllaDB(spark, dataModel, "data.geo_points")

    // load ScyllaDB data to Elasticsearch
    val storedData = readFromScyllaDB(spark, "data.geo_points")
    storedData.writeToElasticsearch(spark, dataModel, "data/geo_points")
```

# Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting us pull requests.

# Authors

[List of contributors](https://github.com/gisaia/ARLAS-proc/graphs/contributors)

# License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
