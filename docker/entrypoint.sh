#!/usr/bin/env bash
# Copyright 2022 The HoraeDB Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



set -exo pipefail

## init varibles
USER="horae"
DATA_DIR="/tmp/horaemeta0"
CONFIG_FILE="/etc/horaemeta/horaemeta.toml"

## data dir
mkdir -p ${DATA_DIR}
chmod +777 -R ${DATA_DIR}
chown -R ${USER}.${USER} ${DATA_DIR}

exec /usr/bin/horaemeta-server --config ${CONFIG_FILE}
