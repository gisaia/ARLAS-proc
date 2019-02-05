#!/usr/bin/env bash

set -e -o pipefail

if [[ "${DEBUG}" == true ]]; then
  set -x
fi

if [ "${1}" = "cluster" ]
then
    MODE="cluster"
    TOTAL_EXECUTOR_CORES=4
else
    MODE="standalone"
    TOTAL_EXECUTOR_CORES=2
fi

# CHECK ALV2 DISCLAIMER
if [ $(find ./*/src -name "*.scala" -exec grep -L Licensed {} \; | wc -l) -gt 0 ]; then
    echo "ALv2 disclaimer is missing in the following files :"
    find ./*/src -name "*.scala" -exec grep -L Licensed {} \;
    exit -1
fi

function docker_clean {
    echo "===> Shutdown docker stack"
    docker-compose -f scripts/tests/docker-compose-${MODE}.yml down --remove-orphans
    docker run --rm \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        gisaia/sbt:1.2.7_jdk8 \
        /bin/bash -c 'sbt clean; rm -rf target project/target project/project spark-warehouse tmp'
}

clean_exit() {
    ARG=$?
	echo "===> Exit stage ${STAGE} = ${ARG}"
    docker_clean
    exit $ARG
}
trap clean_exit EXIT

docker_clean

echo "===> Build sbt project"
docker run --rm \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        gisaia/sbt:1.2.7_jdk8 \
        /bin/bash -c 'sbt clean test package; mv target/scala-2.11/arlas-data*.jar target/arlas-data.jar'

echo "===> Start Spark and ScyllaDB clusters"
docker-compose -f scripts/tests/docker-compose-${MODE}.yml up -d

echo "===> Waiting for ScyllaDB"
docker run --network gisaia-network --rm busybox sh -c 'i=1; until nc -w 2 gisaia-scylla-db 9042; do if [ $i -lt 30 ]; then sleep 1; else break; fi; i=$(($i + 1)); done'
echo "===> ScyllaDB is up and running"

sparkJobSubmit() {
    echo "  => submit $1 from $2 to $3"
    docker run --rm \
       --network gisaia-network \
       -w /opt/work \
       -v ${PWD}:/opt/work \
       --link gisaia-spark-master \
       -p "4040:4040" \
       gisaia/spark:latest \
       spark-submit \
           --class "$1" \
           --deploy-mode "client" \
           --master "spark://gisaia-spark-master:7077" \
           --driver-memory "1g" \
           --executor-memory "512m" \
           --executor-cores "2" \
           --total-executor-cores "${TOTAL_EXECUTOR_CORES}" \
           --supervise \
           --conf spark.cassandra.connection.host="gisaia-scylla-db" \
           --conf spark.driver.allowMultipleContexts="true" \
           --conf spark.rpc.netty.dispatcher.numThreads="2" \
           --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 \
           /opt/work/target/arlas-data.jar \
            --source "$2" \
            --target "$3" \
            --start 2018-01-01T00:00:00+00:00\
            --stop 2018-01-01T12:00:00+00:00\
            --id mmsi \
            --lon longitude \
            --lat latitude \
            --gap 3600 \
            --dynamic latitude,longitude,sog,cog,heading,rot,draught \
            --timeformat "yyyy-MM-dd'T'HH:mm:ss.SSSX"
}
echo "===> CSV Extractor run"
sparkJobSubmit "io.arlas.data.extract.CSVExtractor" "/opt/work/scripts/tests/resources/*.csv" "/opt/work/target/tmp/parquet"

echo "===> Transformer run"
sparkJobSubmit "io.arlas.data.transform.Transformer" "/opt/work/target/tmp/parquet" "ais_ks.ais_table"

echo "===> Check result"
mkdir tmp
docker run --net gisaia-network --rm --entrypoint cqlsh scylladb/scylla:2.2.0 \
    -e 'SELECT COUNT(*) FROM ais_ks.ais_table' gisaia-scylla-db >>tmp/test-output.txt 2>&1
docker run --net gisaia-network --rm --entrypoint cqlsh scylladb/scylla:2.2.0 \
    -e 'SELECT * FROM ais_ks.ais_table' gisaia-scylla-db >>tmp/test-output.txt 2>&1
tail -n +0 tmp/test-output.txt
OUTPUT_DIFFS=`grep -v -f <(grep . < tmp/test-output.txt) ./scripts/tests/test-output.txt` || echo "test output checking"
if [ -z "${OUTPUT_DIFFS}" ]; then
   echo "test is OK"
else
   echo "test is KO"
   echo "Missing some lines in output :"
   echo ${OUTPUT_DIFFS}
   exit -1
fi