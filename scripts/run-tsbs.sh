#!/usr/bin/env bash

export CURR_DIR=$(pwd)
# used to create issue in https://github.com/CeresDB/tsbs/labels
export ISSUE_FILE=${CURR_DIR}/tsbs/issue.md
export CONFIG_FILE=${CONFIG_FILE:-docs/minimal.toml}
export LOG_DIR=${CURR_DIR}/logs
# where generated data stored
export DATA_FILE=${DATA_FILE:-data.out}
# how many values in host tag
export HOST_NUM=${HOST_NUM:-10000}
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

set -x
trap cleanup EXIT
cleanup() {
  ls -lha ${LOG_DIR}
  ls -lha ${CURR_DIR}/tsbs
  ls -lha ${BULK_DATA_DIR}
}

mkdir -p ${LOG_DIR}
make build
nohup ./target/release/ceresdb-server -c ${CONFIG_FILE} > ${LOG_DIR}/server.log &

git clone -b feat-ceresdb --depth 1 --single-branch https://github.com/CeresDB/tsbs.git

cd tsbs
go build ./cmd/tsbs_generate_data
go build ./cmd/tsbs_load_ceresdb
go build ./cmd/tsbs_generate_queries
go build ./cmd/tsbs_run_queries_ceresdb

# generate benchmark data
./tsbs_generate_data --use-case="cpu-only" --seed=123 --initial-scale=${HOST_NUM} --scale=${HOST_NUM} \
                     --timestamp-start="${TS_START}" \
                     --timestamp-end="${TS_END}" \
                     --log-interval="60s" --format="${FORMATS}" > ${DATA_FILE}


# write data to ceresdb
./tsbs_load_ceresdb --file ${DATA_FILE} | tee ${LOG_DIR}/write.log

# generate queries
./scripts/generate_queries.sh

# run queries against ceresdb
# Note: only run 5-8-1 now
cat ${BULK_DATA_DIR}/ceresdb-single-groupby-5-8-1-queries.gz | gunzip | ./tsbs_run_queries_ceresdb | tee ${LOG_DIR}/5-8-1.log

# Output write & query result to issue
echo '# Write' >> ${ISSUE_FILE}
echo '```bash' >> ${ISSUE_FILE}
cat ${LOG_DIR}/write.log >> ${ISSUE_FILE}
echo '```' >> ${ISSUE_FILE}

echo '# Query' >> ${ISSUE_FILE}
echo '```bash' >> ${ISSUE_FILE}
cat ${LOG_DIR}/5-8-1.log >> ${ISSUE_FILE}
echo '```' >> ${ISSUE_FILE}
