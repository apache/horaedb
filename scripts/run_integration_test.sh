#!/usr/bin/env bash

set -exo

INTEGRATION_TEST_PATH=/tmp/ceresmeta_integration_test
CERESDB_INTEGRATION_PATH=${INTEGRATION_TEST_PATH}"/ceresdb/integration_tests"
CERESMETA_INTEGRATION_TEST_PATH=${CERESDB_INTEGRATION_PATH}"/ceresmeta"
CERESMETA_DIR_PATH=$(pwd)

rm -rf $INTEGRATION_TEST_PATH

mkdir -p $INTEGRATION_TEST_PATH

# Download CeresDB Code.
cd $INTEGRATION_TEST_PATH
git clone --depth 1 https://github.com/ceresdb/ceresdb.git --branch main

# Make Build Meta Script Blank.
echo > $CERESDB_INTEGRATION_PATH/build_meta.sh

# Copy Meta Binary to CeresDB Execution Directory.
cd $CERESDB_INTEGRATION_PATH
mkdir ceresmeta
cp $CERESMETA_DIR_PATH/bin/ceresmeta-server $CERESMETA_INTEGRATION_TEST_PATH/ceresmeta

# Run integration_test.
make run-cluster
