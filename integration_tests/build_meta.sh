#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

BIN_PATH=${BIN_PATH:-""}
if [[ -z "${BIN_PATH}" ]]; then
  echo "Fetch and install ceresmeta-server..."
  COMMIT=$(gh api -H "Accept: application/vnd.github.VERSION.sha" repos/CeresDB/ceresmeta/commits/main)
  go install github.com/CeresDB/ceresmeta/cmd/ceresmeta-server@${COMMIT}
  BIN_PATH="$(go env GOPATH)/bin/ceresmeta-server"
fi

TARGET=$(pwd)/ceresmeta
mkdir -p ${TARGET}
cp ${BIN_PATH} ${TARGET}/ceresmeta-server
