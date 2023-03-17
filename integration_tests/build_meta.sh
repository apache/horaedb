#!/usr/bin/env bash

set -exo pipefail

if [ ! -d ceresmeta ]; then
    git clone https://github.com/ceresdb/ceresmeta.git
fi
cd ceresmeta
go build -o ceresmeta ./cmd/meta/...
mv ceresmeta $1
