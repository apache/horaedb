#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

META_BIN_PATH=${META_BIN_PATH:-""}

if [[ -z "${META_BIN_PATH}" ]]; then
    echo "Fetch and install ceresmeta-server..."
    go install -a github.com/CeresDB/horaemeta/cmd/ceresmeta-server@main
    META_BIN_PATH="$(go env GOPATH)/bin/ceresmeta-server"
fi

TARGET=$(pwd)/ceresmeta
mkdir -p ${TARGET}
cp ${META_BIN_PATH} ${TARGET}/ceresmeta-server
