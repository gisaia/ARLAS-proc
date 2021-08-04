# ARLAS-proc
Spark Library to ingest and process geodata timeseries  

- [ARLAS-proc](#arlas-proc)
  * [Overview](#overview)
  * [Prerequisites](#prerequisites)
    + [Building](#building)
    + [Running](#running)
  * [Build](#build)
    + [JAR](#jar)
    + [Publish SNAPSHOT version to Cloudsmith](#publish-snapshot-version-to-cloudsmith)
  * [Release](#release)
  * [User guide](#user-guide)
    + [Add ARLAS-proc dependency](#add-arlas-proc-dependency)
    + [Test locally through spark-shell](#test-locally-through-spark-shell)
  * [Running tests](#running-tests)
    + [Run test suite](#run-test-suite)
    + [Unit tests relying on external API](#unit-tests-relying-on-external-api)
      - [Capture external API](#capture-external-api)
      - [Use mock server from scala tests](#use-mock-server-from-scala-tests)
  * [Contributing](#contributing)
  * [Authors :](#authors--)
  * [License](#license)
  * [Acknowledgments :](#acknowledgments--)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

## Overview

ARLAS-proc is a **toolbox** to transform raw geodata timeseries into enriched movement fragments and trajectories. It is packaged as a scala library for [Apache Spark](https://spark.apache.org/) developers.

## Prerequisites
### Building
- [Java](https://www.java.com/) JDK 8
- [Scala](https://www.scala-lang.org/) 2.12.10
- [SBT](https://www.scala-sbt.org/) 1.5.5

### Running
- [Apache Spark](https://spark.apache.org/) 3.1.2 (for Hadoop 2.7 with OpenJDK 8)
- [Elasticsearch](https://www.elastic.co/fr/elasticsearch/) 7.x

## Build
### JAR
```bash
docker run --rm \
        -w /opt/work \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        gisaia/sbt:1.5.5_jdk8 \
        sbt clean publishLocal
```

Now, you can add it as a local dependency in your own project

```scala
libraryDependencies += "io.arlas" % "arlas-proc" % "X.Y.Z-SNAPSHOT"
```

### Publish SNAPSHOT version to Cloudsmith

If you have sufficient permissions to our Cloudsmith repository, you can publish a SNAPSHOT build jar to Cloudsmith.

You need to set up the following environment variables first:
- `CLOUDSMITH_USER`
- `CLOUDSMITH_API_KEY` (see [https://cloudsmith.io/user/settings/api/])

```bash
export CLOUDSMITH_USER="your-user"
export CLOUDSMITH_API_KEY="your-api-key"

docker run --rm \
        -w /opt/work \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        -e CLOUDSMITH_USER=${CLOUDSMITH_USER} \
        -e CLOUDSMITH_API_KEY=${CLOUDSMITH_API_KEY} \
        gisaia/sbt:1.5.5_jdk8 \
        sbt clean publish
```

Now, you can add it as a remote dependency in your own project

```scala
resolvers += "gisaia-public" at "https://dl.cloudsmith.io/public/gisaia/public/maven/"
libraryDependencies += "io.arlas" % "arlas-proc" % "X.Y.Z-SNAPSHOT"
```

## Release

If you have sufficient permissions on Github repository, simply type:

```bash
docker run -ti \
        -w /opt/work \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        -e CLOUDSMITH_USER=${CLOUDSMITH_USER} \
        -e CLOUDSMITH_API_KEY=${CLOUDSMITH_API_KEY} \
        gisaia/sbt:1.5.5_jdk8 \
        sbt clean release
```

You will be asked for the versions to use for release & next version.

A jar artifact tagged in the released version will be automatically published to Cloudsmith.

## User guide

### Add ARLAS-proc dependency
To enable the retrieval of ARLAS-proc via sbt, add our Cloudsmith repository in your build.sbt file.

```scala
resolvers += "gisaia-public" at "https://dl.cloudsmith.io/public/gisaia/public/maven/"
```
Specify ARLAS-proc dependency in the dependencies section of your build.sbt file by adding the following line.
```scala
libraryDependencies += "io.arlas" % "arlas-proc" % "X.Y.Z"
```

### Test locally through spark-shell

Start an interactive spark-shell session. For example :
```bash
# Build fat jar
docker run --rm \
        -w /opt/work \
        -v ${PWD}:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        gisaia/sbt:1.5.5_jdk8 \
        /bin/bash -c 'sbt clean assembly; cp target/scala-2.12/arlas-proc-assembly*.jar target/scala-2.12/arlas-proc-assembly.jar'

# Build spark-shell
docker run -ti \
       -w /opt/work \
       -v ${PWD}:/opt/proc \
       -v $HOME/.m2:/root/.m2 \
       -v $HOME/.ivy2:/root/.ivy2 \
       -p "4040:4040" \
       gisaia/spark:3.1.2 \
       spark-shell \
        --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.13.4,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
        --exclude-packages javax.media:jai_core \
        --repositories https://repo.osgeo.org/repository/release/,https://dl.cloudsmith.io/public/gisaia/public/maven/,https://repository.jboss.org/maven2/ \
        --jars /opt/proc/target/scala-2.12/arlas-proc-assembly.jar
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

## Running tests
### Run test suite
```scala
docker run -ti \
        -w /opt/work \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        -e CLOUDSMITH_USER=${CLOUDSMITH_USER} \
        -e CLOUDSMITH_API_KEY=${CLOUDSMITH_API_KEY} \
        gisaia/sbt:1.5.5_jdk8 \
        sbt clean test
```

### Unit tests relying on external API

External APIs are mocked using Wiremock. Wiremock has 2 benefits:

- using a JAR, we can capture every call to an API and save the results for further use
- then from scala tests, we can start a wiremock server and get these results.

#### Capture external API 

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

#### Use mock server from scala tests

A test class can extend the trait `ArlasMockServer`, which automatically starts and stops the mock server.

## Contributing
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting us pull requests.

## Authors
- Gisaïa - *Initial work* - [Gisaïa](http://gisaia.com/)

See also the list of [contributors](https://github.com/gisaia/ARLAS-proc/graphs/contributors) who participated in this project.

## License
This project is licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.

## Acknowledgments
This project has been initiated and is maintained by [Gisaïa](http://gisaia.com/)