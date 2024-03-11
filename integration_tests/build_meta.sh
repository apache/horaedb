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


set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
TARGET="${SCRIPT_DIR}/../target"

META_BIN_PATH=${META_BIN_PATH:-""}

if [[ -z "${META_BIN_PATH}" ]]; then
  echo "Build horaemeta-server..."
  make -C "${SCRIPT_DIR}/../horaemeta" build
  META_BIN_PATH="${SCRIPT_DIR}/../horaemeta/bin/horaemeta-server"
fi

mkdir -p ${TARGET}
cp ${META_BIN_PATH} ${TARGET}/horaemeta-server
