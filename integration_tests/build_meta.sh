#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

CERESMETA_BIN_PATH=${CERESMETA_BIN_PATH:-""}

if [[ -z "${CERESMETA_BIN_PATH}" ]]; then
    echo "Fetch and install ceresmeta-server..."
    go install -a github.com/CeresDB/ceresmeta/cmd/ceresmeta-server@main
    CERESMETA_BIN_PATH="$(go env GOPATH)/bin/ceresmeta-server"
fi

TARGET=$(pwd)/ceresmeta
mkdir -p ${TARGET}
cp ${CERESMETA_BIN_PATH} ${TARGET}/ceresmeta-server
