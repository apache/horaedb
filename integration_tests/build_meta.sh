#!/usr/bin/env bash

set -exo

if [ -d ceresmeta ]; then
  echo "Remove old meta..."
  rm -r ceresmeta
fi

git clone --depth 1 https://github.com/ceresdb/ceresmeta.git
cd ceresmeta
go build -o ceresmeta ./cmd/meta/...
