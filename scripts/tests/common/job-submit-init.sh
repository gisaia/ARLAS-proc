#!/usr/bin/env bash

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
        /bin/bash -c 'sbt clean test package; mv target/scala-2.11/arlas-proc*.jar target/arlas-proc.jar'

echo "===> Start Spark and ScyllaDB clusters"
docker-compose -f scripts/tests/docker-compose-${MODE}.yml up -d

echo "===> Waiting for ScyllaDB"
docker run --network gisaia-network --rm busybox sh -c 'i=1; until nc -w 2 gisaia-scylla-db 9042; do if [ $i -lt 30 ]; then sleep 1; else break; fi; i=$(($i + 1)); done'
echo "===> ScyllaDB is up and running"

submit_spark_job() {
    echo "  => submit $1 from $2 to $3 between $4 to $5"
    docker run --rm \
       --network gisaia-network \
       -w /opt/work \
       -v ${PWD}:/opt/work \
       -v ${HOME}/.m2:/root/.m2 \
       -v ${HOME}/.ivy2:/root/.ivy2 \
       --link gisaia-spark-master \
       -p "4040:4040" \
       gisaia/spark:2.3.1 \
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
           --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///opt/work/scripts/tests/spark/log4j.properties" \
           --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.elasticsearch:elasticsearch-spark-20_2.11:6.4.0,org.geotools:gt-referencing:20.1,org.geotools:gt-geometry:20.1,org.geotools:gt-epsg-hsql:20.1 \
           --exclude-packages javax.media:jai_core \
           --repositories http://repo.boundlessgeo.com/main,http://download.osgeo.org/webdav/geotools/,http://central.maven.org/maven2/ \
           /opt/work/target/arlas-proc.jar \
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