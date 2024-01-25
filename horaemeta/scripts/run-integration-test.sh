#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.




set -exo

META_BIN_PATH="$(pwd)/bin/horaemeta-server"
INTEGRATION_TEST_PATH=$(mktemp -d)

# Download HoraeDB Code
cd $INTEGRATION_TEST_PATH
git clone --depth 1 https://github.com/apache/incubator-horaedb.git horaedb --branch main

# Run integration_test
cd horaedb/integration_tests

META_BIN_PATH=$META_BIN_PATH make run-cluster
