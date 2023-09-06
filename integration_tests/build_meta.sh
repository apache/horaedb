#!/usr/bin/env bash

set -e

SRC=/tmp/ceresmeta-src
TARGET=$(pwd)/ceresmeta

if [[ -d $SRC ]] && [[ $UPDATE_REPOS_TO_LATEST == 'true' ]]; then
  echo "Remove old meta..."
  rm -rf $SRC
fi

if [[ ! -d $SRC ]]; then
  echo "Pull meta repo..."
  git clone --depth 1 https://github.com/ceresdb/ceresmeta.git ${SRC}
fi

cd ${SRC}
go build -o ${TARGET}/ceresmeta ./cmd/meta/...
