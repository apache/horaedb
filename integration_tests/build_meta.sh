#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
TARGET="${SCRIPT_DIR}/../target"

META_BIN_PATH=${META_BIN_PATH:-""}

if [[ -z "${META_BIN_PATH}" ]]; then
  echo "Build horaemeta-server..."
  make -C "${SCRIPT_DIR}/../horaemeta" build
  META_BIN_PATH="${SCRIPT_DIR}/../horaemeta/bin/horaemeta-server"
fi

mkdir -p ${TARGET}
cp ${META_BIN_PATH} ${TARGET}/horaemeta-server
