#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# This bash supports these settings by enviroment variables:
# - RESULT_FILE
# - DATA_FILE
# - LOG_DIR
# - HORAEDB_CONFIG_FILE
# - HORAEDB_ADDR
# - WRITE_WORKER_NUM
# - WRITE_BATCH_SIZE

export CURR_DIR=$(pwd)
export DEFAULT_RESULT_FILE=${CURR_DIR}/tsbs/result.md
export RESULT_FILE=${RESULT_FILE:-${DEFAULT_RESULT_FILE}}
export HORAEDB_CONFIG_FILE=${HORAEDB_CONFIG_FILE:-docs/minimal.toml}
export LOG_DIR=${LOG_DIR:-${CURR_DIR}/logs}
export HORAEDB_ADDR=${HORAEDB_ADDR:-127.0.0.1:8831}
export HORAEDB_PID_FILE=${CURR_DIR}/horaedb-server.pid
export WRITE_WORKER_NUM=${WRITE_WORKER_NUM:-36}
export WRITE_BATCH_SIZE=${WRITE_BATCH_SIZE:-500}
# Where generated data stored
export DATA_FILE=${DATA_FILE:-data.out}
# How many values in host tag
export HOST_NUM=${HOST_NUM:-10000}

# Used for `generate_queries.sh` start.
export TS_START="2022-09-05T00:00:00Z"
export TS_END="2022-09-05T12:00:01Z"
export EXE_FILE_NAME=${CURR_DIR}/tsbs/tsbs_generate_queries
# where generated queries stored
export BULK_DATA_DIR=${CURR_DIR}/tsbs/data
export FORMATS=ceresdb
export QUERY_TYPES="\
single-groupby-1-1-1 \
single-groupby-1-1-12 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1"
# Used for `generate_queries.sh` end.

set -x

kill_ceresdb_server() {
  if [ -f ${HORAEDB_PID_FILE} ]; then
    pid=$(cat ${HORAEDB_PID_FILE})
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
    fi
  fi
}

trap cleanup EXIT
cleanup() {
  ls -lha ${LOG_DIR}
  ls -lha ${CURR_DIR}/tsbs
  ls -lha ${BULK_DATA_DIR}

  kill_ceresdb_server
}

mkdir -p ${LOG_DIR}

kill_ceresdb_server
nohup ./target/release/horaedb-server -c ${HORAEDB_CONFIG_FILE} > ${LOG_DIR}/server.log & echo $! > ${HORAEDB_PID_FILE}

git clone -b feat-ceresdb --depth 1 --single-branch https://github.com/CeresDB/tsbs.git

cd tsbs
go build ./cmd/tsbs_generate_data
go build ./cmd/tsbs_load_ceresdb
go build ./cmd/tsbs_generate_queries
go build ./cmd/tsbs_run_queries_ceresdb

if [ ! -f ${DATA_FILE} ]; then
  # Generate benchmark data if it does not exist.
  ./tsbs_generate_data \
    --use-case="cpu-only" \
    --seed=123 \
    --initial-scale=${HOST_NUM} \
    --scale=${HOST_NUM} \
    --timestamp-start="${TS_START}" \
    --timestamp-end="${TS_END}" \
    --log-interval="60s" \
    --format="${FORMATS}" > ${DATA_FILE}
fi


# Write data to horaedb
./tsbs_load_ceresdb --ceresdb-addr=${HORAEDB_ADDR} --file ${DATA_FILE} --batch-size ${WRITE_BATCH_SIZE} --workers ${WRITE_WORKER_NUM} | tee ${LOG_DIR}/write.log

# Generate queries for query
./scripts/generate_queries.sh

# Run queries against horaedb
# TODO: support more kinds of queries besides 5-8-1.
cat ${BULK_DATA_DIR}/ceresdb-single-groupby-5-8-1-queries.gz | gunzip | ./tsbs_run_queries_ceresdb --ceresdb-addr=${HORAEDB_ADDR} | tee ${LOG_DIR}/5-8-1.log

# Clean the result file
rm ${RESULT_FILE}

# Output write & query result
echo '# Write' >> ${RESULT_FILE}
echo '```bash' >> ${RESULT_FILE}
cat ${LOG_DIR}/write.log >> ${RESULT_FILE}
echo '```' >> ${RESULT_FILE}

echo '# Query' >> ${RESULT_FILE}
echo '```bash' >> ${RESULT_FILE}
cat ${LOG_DIR}/5-8-1.log >> ${RESULT_FILE}
echo '```' >> ${RESULT_FILE}
