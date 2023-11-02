# Copyright 2022 The CeresDB Authors
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

## Builder
ARG GOLANG_VERSION=1.21.3
FROM golang:${GOLANG_VERSION}-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes gcc g++ libssl-dev pkg-config cmake && rm -rf /var/lib/apt/lists/*

COPY . /ceresmeta
WORKDIR /ceresmeta

RUN make build

## CeresMeta
FROM ubuntu:20.04

RUN useradd -m -s /bin/bash ceres

RUN apt update && \
    apt install --yes curl gdb iotop cron vim less net-tools && \
    apt clean

COPY --from=build /ceresmeta/bin/ceresmeta-server /usr/bin/ceresmeta-server
RUN chmod +x /usr/bin/ceresmeta-server

COPY ./docker/entrypoint.sh /entrypoint.sh
COPY ./config/example-standalone.toml /etc/ceresmeta/ceresmeta.toml

COPY ./docker/tini /tini
RUN chmod +x /tini

ARG USER ceres

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]
