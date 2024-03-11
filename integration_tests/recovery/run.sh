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


set -e

ROOT=`pwd`
# For compatibility in macos, so convert to milliseconds by adding 3 zeros.
NOW=`date +%s000`
BINARY_PATH=${ROOT}/../../target/debug/horaedb-server
SERVER_HTTP_ENDPOINT=127.0.0.1:5440

CONFIG_FILE=${ROOT}/../../docs/minimal.toml
if [ ${1} == 'shard_based' ]; then
    CONFIG_FILE=${ROOT}/../config/shard-based-recovery.toml
fi

echo "Run with config: ${CONFIG_FILE}"
echo "First check..."
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW} -i

echo "Restart and check..."
killall horaedb-server | true
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW}

echo "Flush, restart and check..."
curl -XPOST ${SERVER_HTTP_ENDPOINT}/debug/flush_memtable
echo "\nFlush finish..."
killall horaedb-server | true
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW}
echo "All finish..."
