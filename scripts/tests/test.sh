#!/usr/bin/env bash

#######################################################################
# Script Initialization
#######################################################################

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

SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd)"
source "${SCRIPT_DIRECTORY}/common/job-submit-init.sh"

# CHECK ALV2 DISCLAIMER
if [ $(find ./*/src -name "*.scala" -exec grep -L Licensed {} \; | wc -l) -gt 0 ]; then
    echo "ALv2 disclaimer is missing in the following files :"
    find ./*/src -name "*.scala" -exec grep -L Licensed {} \;
    exit -1
fi

#######################################################################
# Submit jobs: Extractor, Transformer, then Elasticsearch
#######################################################################

echo "===> CSV Extractor run 1"
submit_spark_job \
            "io.arlas.data.extract.CSVExtractor" \
            "/opt/work/scripts/tests/resources/ais-sample-data-1.csv" \
            "/opt/work/target/tmp/parquet" \
            "2018-01-01T00:00:00+00:00" \
            "2018-01-01T23:59:59+00:00"

echo "===> Transformer run 1"
submit_spark_job \
            "io.arlas.data.transform.Transformer" \
            "/opt/work/target/tmp/parquet" \
            "ais_ks.ais_table" \
            "2018-01-01T00:00:00+00:00" \
            "2018-01-01T23:59:59+00:00"

echo "===> CSV Extractor run 2"
submit_spark_job \
            "io.arlas.data.extract.CSVExtractor" \
            "/opt/work/scripts/tests/resources/ais-sample-data-2.csv" \
            "/opt/work/target/tmp/parquet" \
            "2018-01-02T00:00:00+00:00" \
            "2018-01-02T23:59:59+00:00"

echo "===> Transformer run 2"
submit_spark_job \
            "io.arlas.data.transform.Transformer" \
            "/opt/work/target/tmp/parquet" \
            "ais_ks.ais_table" \
            "2018-01-02T00:00:00+00:00" \
            "2018-01-02T23:59:59+00:00"


echo "===> Elasticsearch loader run 1"
submit_spark_job \
            "io.arlas.data.load.ESLoader" \
            "ais_ks.ais_table" \
            "ais_filtered_data/point" \
            "2018-01-01T00:00:00+00:00" \
            "2018-01-01T23:59:59+00:00"

echo "===> Elasticsearch loader run 2"
submit_spark_job \
            "io.arlas.data.load.ESLoader" \
            "ais_ks.ais_table" \
            "ais_filtered_data/point" \
            "2018-01-02T00:00:00+00:00" \
            "2018-01-02T23:59:59+00:00"

#######################################################################
# Check the results of submitted jobs (Extractor and Transformer)
#######################################################################

echo "===> Check ScyllaDB results"
mkdir -p tmp
docker run --net gisaia-network --rm --entrypoint cqlsh scylladb/scylla:2.2.0 \
    -e 'SELECT COUNT(*) FROM ais_ks.ais_table' gisaia-scylla-db >>tmp/test-scylla-output.txt 2>&1
docker run --net gisaia-network --rm --entrypoint cqlsh scylladb/scylla:2.2.0 \
    -e 'SELECT * FROM ais_ks.ais_table' gisaia-scylla-db >>tmp/test-scylla-output.txt 2>&1
tail -n +0 tmp/test-scylla-output.txt
OUTPUT_DIFFS=`grep -v -f <(grep . < tmp/test-scylla-output.txt) ./scripts/tests/output/scylladb.txt` || echo "test output checking"
if [ -z "${OUTPUT_DIFFS}" ]; then
   echo "ScyllaDB test is OK"
else
   echo "ScyllaDB test is KO"
   echo "Missing some lines in output :"
   echo ${OUTPUT_DIFFS}
fi

#######################################################################
# Check the results of submitted jobs (Elasticsearch)
#######################################################################
echo "===> Check Elasticsearch results"

docker exec -it gisaia-elasticsearch curl -X GET 'localhost:9200/ais_filtered_data/_search?pretty=true&filter_path=hits'  -H 'Content-Type: application/json' -d '{"size" : 100,"query" : {"match_all" : {}}, "sort" : [ { "_id" : {"order": "desc"}}]}' >>tmp/test-elasticsearch-output.txt 2>&1
head -100 tmp/test-elasticsearch-output.txt
OUTPUT_DIFFS=`diff -bB tmp/test-elasticsearch-output.txt ./scripts/tests/output/elasticsearch.txt` || echo "test output checking"
if [ -z "${OUTPUT_DIFFS}" ]; then
   echo "Elasticsearch test is OK"
else
   echo "Elasticsearch test is KO"
   echo "Missing some lines in output :"
   echo ${OUTPUT_DIFFS}
fi