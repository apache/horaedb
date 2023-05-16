#!/usr/bin/env bash

set -exo

SRC=/tmp/ceresmeta-src
TARGET=$(pwd)/ceresmeta

if [ -d $SRC ]; then
  echo "Remove old meta..."
  rm -rf $SRC
fi

git clone --depth 1 https://github.com/ceresdb/ceresmeta.git ${SRC}
cd ${SRC}
go build -o ${TARGET}/ceresmeta ./cmd/meta/...
