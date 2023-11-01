#!/usr/bin/env bash

set -exo

CERESMETA_BIN_PATH="$(pwd)/bin/ceresmeta-server"
INTEGRATION_TEST_PATH=$(mktemp -d)

# Download CeresDB Code
cd $INTEGRATION_TEST_PATH
git clone --depth 1 https://github.com/ceresdb/ceresdb.git --branch main

# Run integration_test
cd ceresdb/integration_tests
BIN_PATH=${CERESMETA_BIN_PATH} make run-cluster
