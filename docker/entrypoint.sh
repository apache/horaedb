#!/usr/bin/env bash

set -exo pipefail

## init varibles
USER="ceres"
DATA_DIR="/home/${USER}/data"
DATA_PATH="${DATA_DIR}/ceresdb"
CONFIG_FILE="/etc/ceresdb/ceresdb.toml"

# use echo to trim spaces
LOCAL_IP=$(echo -n $(hostname -I))

# enable jemalloc heap profiling
export MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:19"

if [ "${CERESDB_HTTP_PORT}" == "" ]; then
    CERESDB_HTTP_PORT="5440"
fi
if [ "${CERESDB_GRPC_PORT}" == "" ]; then
    CERESDB_GRPC_PORT="8831"
fi
if [ "${CERESDB_ENABLE_DATANODE}" != "true" ]; then
    CERESDB_ENABLE_DATANODE="false"
fi

## config file
# Do not use `/` for sed, because there are `/` in path args
sed -i 's|${HTTP_PORT}|'${CERESDB_HTTP_PORT}'|g' ${CONFIG_FILE}
sed -i 's|${GRPC_PORT}|'${CERESDB_GRPC_PORT}'|g' ${CONFIG_FILE}
sed -i 's|${DATA_PATH}|'${DATA_PATH}'|g' ${CONFIG_FILE}
sed -i 's|${NODE_ADDR}|'${LOCAL_IP}'|g' ${CONFIG_FILE}

## data dir
mkdir -p ${DATA_DIR}
chmod +777 -R ${DATA_DIR}
chown -R ${USER}.${USER} ${DATA_DIR}

exec /usr/bin/ceresdb-server --config ${CONFIG_FILE}
