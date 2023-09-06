#!/usr/bin/env bash

# Get current dir
export CURR_DIR=$(pwd)

# Related components
TSBS_REPO_PATH=${CURR_DIR}/tsbs
DATA_REPO_PATH=${CURR_DIR}/dist-query-testing
# Case contexts
CASE_DIR=tsbs-cpu-only
CASE_DATASOURCE=data.out
CASE_QUERY=single-groupby-5-8-1-queries.gz
CASE_QUERY_RESULT=queries.result

# Test params
export RESULT_FILE=${RESULT_FILE:-${DEFAULT_RESULT_FILE}}
export OUTPUT_DIR=${OUTPUT_DIR:-${CURR_DIR}/output}
export CERESDB_ADDR=${CERESDB_ADDR:-127.0.0.1:8831}
export CERESDB_HTTP_ADDR=${CERESDB_HTTP_ADDR:-127.0.0.1:5440}
export WRITE_WORKER_NUM=${WRITE_WORKER_NUM:-36}
export WRITE_BATCH_SIZE=${WRITE_BATCH_SIZE:-500}
## Where generated data stored
export DATA_FILE=${DATA_FILE:-${CURR_DIR}/dist-query-testing/${CASE_DIR}/${CASE_DATASOURCE}}
## How many values in host tag
export HOST_NUM=${HOST_NUM:-10000}
export BULK_DATA_DIR=${CURR_DIR}/dist-query-testing/${CASE_DIR}
## Used for `generate_queries.sh` end.
export QUERY_TYPES="\
single-groupby-1-1-1 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-8-1"
## Where query results stored
export QUERY_RESULTS_FILE=${CURR_DIR}/output/queries.reuslt.tmp
export QUERY_EXPECTED_RESULTS_FILE=${QUERY_EXPECTED_RESULTS_FILE:-${CURR_DIR}/dist-query-testing/${CASE_DIR}/${CASE_QUERY_RESULT}}

set -x

mkdir -p ${OUTPUT_DIR}

# Prepare components
## Tsbs
if [[ -d ${TSBS_REPO_PATH} ]] && [[ ${UPDATE_REPOS_TO_LATEST} == 'true' ]] && [[ ${DIST_QUERY_TEST_NO_INIT} == 'false' ]]; then
  echo "Remove old tsbs..."
  rm -rf ${TSBS_REPO_PATH}
fi

if [[ ! -d ${TSBS_REPO_PATH} ]] && [[ ${DIST_QUERY_TEST_NO_INIT} == 'false' ]]; then
    echo "Pull tsbs repo..."
    git clone -b support-partitioned-table --depth 1 --single-branch https://github.com/Rachelint/tsbs.git
fi
## Data
if [[ -d ${DATA_REPO_PATH} ]] && [[ $UPDATE_REPOS_TO_LATEST == 'true' ]] && [[ ${DIST_QUERY_TEST_NO_INIT} == 'false' ]]; then
  echo "Remove old dist query testing..."
  rm -rf ${DATA_REPO_PATH}
fi

echo ${DATA_REPO_PATH}
if [[ ! -d ${DATA_REPO_PATH} ]] && [[ ${DIST_QUERY_TEST_NO_INIT} == 'false' ]]; then
    echo "Pull dist query testing repo..."
    git clone -b main --depth 1 --single-branch https://github.com/CeresDB/dist-query-testing.git
fi
## Build tsbs bins
if [[ ${DIST_QUERY_TEST_NO_INIT} == 'false' ]]; then 
    cd tsbs
    go build ./cmd/tsbs_generate_data
    go build ./cmd/tsbs_load_ceresdb
    go build ./cmd/tsbs_generate_queries
    go build ./cmd/tsbs_run_queries_ceresdb
fi

# Clean old table if exist
curl -XPOST "${CERESDB_HTTP_ADDR}/sql" -d 'DROP TABLE IF EXISTS `cpu`'

# Write data to ceresdb
${CURR_DIR}/tsbs/tsbs_load_ceresdb --ceresdb-addr=${CERESDB_ADDR} --file ${DATA_FILE} --batch-size ${WRITE_BATCH_SIZE} --workers ${WRITE_WORKER_NUM}  --access-mode proxy --partition-keys hostname --update-mode APPEND | tee ${OUTPUT_DIR}/${CASE_DIR}-${CASE_DATASOURCE}.log

# Run queries against ceresdb
# TODO: support more kinds of queries besides 5-8-1.
cat ${BULK_DATA_DIR}/${CASE_QUERY} | gunzip | ${CURR_DIR}/tsbs/tsbs_run_queries_ceresdb --ceresdb-addr=${CERESDB_ADDR} --print-responses true --access-mode proxy --responses-file ${QUERY_RESULTS_FILE} | tee ${OUTPUT_DIR}/${CASE_DIR}-${CASE_QUERY}.log

# Diff the results
python3 ${CURR_DIR}/diff.py --expected ${QUERY_EXPECTED_RESULTS_FILE} --actual ${QUERY_RESULTS_FILE}
