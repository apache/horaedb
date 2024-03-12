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


set -exo pipefail

## init varibles
USER="horae"
DATA_DIR="/home/${USER}/data"
DATA_PATH="${DATA_DIR}/horaedb"
CONFIG_FILE="/etc/horaedb/horaedb.toml"

# enable jemalloc heap profiling
export MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:19"

## data dir
mkdir -p ${DATA_DIR}
chmod +777 -R ${DATA_DIR}
chown -R ${USER}.${USER} ${DATA_DIR}

exec /usr/bin/horaedb-server --config ${CONFIG_FILE}
