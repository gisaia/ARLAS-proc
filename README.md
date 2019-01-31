# Spark Application for Ingestion & Processing data  

- [Overview](#overview)
  * [Versions used in this project](#versions-used-in-this-project)
- [Build and deploy application jar](#build-and-deploy-application-jar)
- [Integration tests](#integration-tests)
- [Remarks](#remarks)
- [Contributing](#contributing)
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
sbt [-DgcsProject=arlas-lsfp] [-DgcsBucket=arlas-data] [-DgcsBucketPath=/artifacts] gcs:publish
```

# Integration tests

```bash
./scripts/tests/test.sh [cluster]
```
# Remarks
* We have used docker-compose files `.yml` in "yaml" folder.
 Each of the files define a number of services along with their configurations:
   For each of the services we specify the image, build, ports, volumes, environments, ...etc.

# Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting us pull requests.

# License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
