#!/usr/bin/env bash

set -exo pipefail

## init varibles
USER="ceres"
DATA_DIR="/home/${USER}/data"
DATA_PATH="${DATA_DIR}/ceresdb"
CONFIG_FILE="/etc/ceresdb/ceresdb.toml"

# enable jemalloc heap profiling
export MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:19"

## data dir
mkdir -p ${DATA_DIR}
chmod +777 -R ${DATA_DIR}
chown -R ${USER}.${USER} ${DATA_DIR}

server=/usr/bin/ceresdb-server
cmd="/tini -- $server"

config="--config ${CONFIG_FILE}"

if [ -v 1 ]; then
  args="$@"
  if echo "$@" | grep -q -- "--help\|-h"; then
    cmd=$server
  fi
  if ! echo "$@" | grep -q -- "--config\|-c"; then
    args="$config $args"
  fi
else
  args="$config"
fi

exec $cmd $args
