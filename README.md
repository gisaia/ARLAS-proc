# Spark Application for Ingestion & Processing data  

- [Overview](#overview)
  * [Versions used in this project](#versions-used-in-this-project)
- [Build and deploy application jar](#build-and-deploy-application-jar)
- [Integration tests](#integration-tests)
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

# Build and deploy application jar

```bash
# Build jar
sbt package

# Deploy jar
# Ensure to provide your Google Cloud Storage credentials
# @see : https://github.com/Tapad/sbt-gcs#credentials
sbt [-DgcsProject=arlas-lsfp] [-DgcsBucket=arlas-proc] [-DgcsBucketPath=/artifacts] gcs:publish
```

# Integration tests

```bash
./scripts/tests/test.sh [cluster]
```

# User guide

### spark-shell example

Start ScyllaDB and Elasticsearch clusters. For example : 
```bash
docker-compose -f scripts/tests/docker-compose-standalone.yml up -d gisaia-scylla-db
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
        --repositories http://repo.boundlessgeo.com/main,http://download.osgeo.org/webdav/geotools/,http://central.maven.org/maven2/ \
        --jars /opt/proc/target/scala-2.11/arlas-proc_2.11-0.3.0-SNAPSHOT.jar \
        --conf spark.es.nodes="gisaia-elasticsearch" \
        --conf spark.es.index.auto.create="true" \
        --conf spark.cassandra.connection.host="gisaia-scylla-db" \
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
      dynamicFields = Array("latitude", "longitude", "sog", "cog", "heading", "rot", "draught"),
      timeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
      timeserieGap = 120
    )
    val period = getPeriod(
      start = "2018-01-01T00:00:00+00:00",
      stop = "2018-01-02T00:00:00+00:00"
    )

    // extract and format raw data
    val data = readFromCsv(spark, "/opt/proc/scripts/tests/resources/ais-sample-data-1.csv")
      .asArlasCleanedData(dataModel)
      

    // transform and resample data
    val transformedData = data
      .asArlasTimeSeries(dataModel)
      .asArlasMotions(dataModel)
      .asArlasResampledMotions(dataModel, spark)
    
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
