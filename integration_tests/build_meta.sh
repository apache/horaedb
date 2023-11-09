#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

BIN_PATH=${BIN_PATH:-""}

echo "Fetch and install ceresmeta-server..."
go install -a github.com/CeresDB/ceresmeta/cmd/ceresmeta-server@HEAD
BIN_PATH="$(go env GOPATH)/bin/ceresmeta-server"

TARGET=$(pwd)/ceresmeta
mkdir -p ${TARGET}
cp ${BIN_PATH} ${TARGET}/ceresmeta-server
