#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

META_BIN_PATH=${META_BIN_PATH:-""}

if [[ -z "${META_BIN_PATH}" ]]; then
    echo "Fetch and install horaemeta-server..."
    go install -a github.com/CeresDB/horaemeta/cmd/horaemeta-server@main
    META_BIN_PATH="$(go env GOPATH)/bin/horaemeta-server"
fi

TARGET=$(pwd)/horaemeta
mkdir -p ${TARGET}
cp ${META_BIN_PATH} ${TARGET}/horaemeta-server
