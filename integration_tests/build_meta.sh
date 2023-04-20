#!/usr/bin/env bash

set -exo

if [ ! -d ceresmeta ]; then
    git clone --depth 1 https://github.com/ceresdb/ceresmeta.git --branch refactor_cluster
fi
cd ceresmeta
go build -o ceresmeta ./cmd/meta/...
