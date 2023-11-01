#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

BIN_PATH=${BIN_PATH:-""}
if [[ -z "${BIN_PATH}" ]]; then
  echo "Fetch and install ceresmeta-server..."
  go install github.com/CeresDB/ceresmeta/cmd/ceresmeta-server@HEAD
  BIN_PATH="$(go env GOPATH)/bin/ceresmeta-server"
fi

TARGET=$(pwd)/ceresmeta
mkdir -p ${TARGET}
cp ${BIN_PATH} ${TARGET}/ceresmeta-server
