#!/usr/bin/env bash

set -exo pipefail

## init varibles
USER="ceres"
DATA_DIR="/tmp/ceresmeta0"
CONFIG_FILE="/etc/ceresmeta/ceresmeta.toml"

## data dir
mkdir -p ${DATA_DIR}
chmod +777 -R ${DATA_DIR}
chown -R ${USER}.${USER} ${DATA_DIR}

exec /usr/bin/ceresmeta-server --config ${CONFIG_FILE}
