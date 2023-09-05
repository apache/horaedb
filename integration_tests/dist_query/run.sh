#!/usr/bin/env bash

# This bash supports these settings by enviroment variables:
# - RESULT_FILE
# - DATA_FILE
# - LOG_DIR
# - CERESDB_CONFIG_FILE
# - CERESDB_ADDR
# - WRITE_WORKER_NUM
# - WRITE_BATCH_SIZE

export CURR_DIR=$(pwd)
export DEFAULT_RESULT_FILE=${CURR_DIR}/output/result.md
export RESULT_FILE=${RESULT_FILE:-${DEFAULT_RESULT_FILE}}
export CERESDB_CONFIG_FILE=${CERESDB_CONFIG_FILE:-docs/minimal.toml}
export LOG_DIR=${LOG_DIR:-${CURR_DIR}/output}
export CERESDB_ADDR=${CERESDB_ADDR:-127.0.0.1:8831}
export CERESDB_HTTP_ADDR=${CERESDB_HTTP_ADDR:-127.0.0.1:5440}
export CERESDB_PID_FILE=${CURR_DIR}/ceresdb-server.pid
export WRITE_WORKER_NUM=${WRITE_WORKER_NUM:-36}
export WRITE_BATCH_SIZE=${WRITE_BATCH_SIZE:-500}
# Where generated data stored
export DATA_FILE=${DATA_FILE:-${CURR_DIR}/dist-query-testing/tsbs-cpu-only/data.out}
# How many values in host tag
export HOST_NUM=${HOST_NUM:-10000}

# Used for `generate_queries.sh` start.
export TS_START="2022-09-05T00:00:00Z"
export TS_END="2022-09-05T01:00:01Z"
export EXE_FILE_NAME=${CURR_DIR}/tsbs/tsbs_generate_queries
# where generated queries stored
export BULK_DATA_DIR=${CURR_DIR}/dist-query-testing/tsbs-cpu-only
export FORMATS=ceresdb
# Used for `generate_queries.sh` end.
export QUERY_TYPES="\
single-groupby-1-1-1 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-8-1"
export QUERIES=20

export QUERY_RESPONSE_FILE=${CURR_DIR}/output/resp.txt

set -x

# Init
trap cleanup EXIT
cleanup() {
  ls -lha ${LOG_DIR}
  ls -lha ${CURR_DIR}/tsbs
  ls -lha ${BULK_DATA_DIR}
  curl -XPOST "${CERESDB_HTTP_ADDR}/sql" -d 'DROP TABLE `cpu`'
}

mkdir -p ${LOG_DIR}

# Prepare components
## tsbs
git clone -b support-partitioned-table --depth 1 --single-branch https://github.com/Rachelint/tsbs.git
## data
git clone -b main --depth 1 --single-branch https://github.com/CeresDB/dist-query-testing.git
## build tsbs bins
cd tsbs
go build ./cmd/tsbs_generate_data
go build ./cmd/tsbs_load_ceresdb
go build ./cmd/tsbs_generate_queries
go build ./cmd/tsbs_run_queries_ceresdb

# Write data to ceresdb
${CURR_DIR}/tsbs/tsbs_load_ceresdb --ceresdb-addr=${CERESDB_ADDR} --file ${DATA_FILE} --batch-size ${WRITE_BATCH_SIZE} --workers ${WRITE_WORKER_NUM}  --access-mode proxy --partition-keys hostname | tee ${LOG_DIR}/write.log

# Run queries against ceresdb
# TODO: support more kinds of queries besides 5-8-1.
cat ${BULK_DATA_DIR}/single-groupby-5-8-1-queries.gz | gunzip | ${CURR_DIR}/tsbs/tsbs_run_queries_ceresdb --ceresdb-addr=${CERESDB_ADDR} --print-responses true --access-mode proxy --responses-file ${QUERY_RESPONSE_FILE} | tee ${LOG_DIR}/5-8-1.log
