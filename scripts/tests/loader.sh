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

echo "===> Build sbt project"
docker run --rm \
        -v $PWD:/opt/work \
        -v $HOME/.m2:/root/.m2 \
        -v $HOME/.ivy2:/root/.ivy2 \
        gisaia/sbt:1.2.7_jdk8 \
        /bin/bash -c 'sbt clean test package; mv target/scala-2.11/arlas-data*.jar target/arlas-data.jar'


sparkJobSubmit() {
    echo "  => submit $1 from $2 to $3 between $4 to $5"
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
           --conf spark.es.nodes="gisaia-elasticsearch" \
           --conf spark.es.index.auto.create="true" \
           --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.elasticsearch:elasticsearch-spark-20_2.11:6.4.0 \
           /opt/work/target/arlas-data.jar \
            --source "$2" \
            --target "$3" \
            --start "$4" \
            --stop "$5" \
            --id mmsi \
            --lon longitude \
            --lat latitude \
            --gap 120 \
            --dynamic latitude,longitude,sog,cog,heading,rot,draught \
            --timeformat "yyyy-MM-dd'T'HH:mm:ss.SSSX"
}

echo "===> Elasticsearch loader run 1"
sparkJobSubmit "io.arlas.data.load.ESLoader" "ais_ks.ais_table" "ais_filtered_data/point" "2018-01-01T00:00:00+00:00" "2018-01-01T23:59:59+00:00"

echo "===> Elasticsearch loader run 2"
sparkJobSubmit "io.arlas.data.load.ESLoader" "ais_ks.ais_table" "ais_filtered_data/point" "2018-01-02T00:00:00+00:00" "2018-01-02T23:59:59+00:00"

echo "===> Check result"
mkdir -p tmp

docker exec -it gisaia-elasticsearch curl -X GET 'localhost:9200/ais_filtered_data/_search?pretty=true&filter_path=hits'  -H 'Content-Type: application/json' -d '{"size" : 100,"query" : {"match_all" : {}}, "sort" : [ { "_id" : {"order": "desc"}}]}' >>tmp/test-output-es.txt 2>&1

OUTPUT_DIFFS=`diff -bB tmp/test-output-es.txt ./scripts/tests/test-output-es.txt` || echo "test output checking"
if [ -z "${OUTPUT_DIFFS}" ]; then
   echo "test is OK"
else
   echo "test is KO"
   echo "Missing some lines in output :"
   echo ${OUTPUT_DIFFS}
fi

docker exec -it gisaia-elasticsearch curl -X DELETE "localhost:9200/ais_filtered_data"
rm -rf tmp