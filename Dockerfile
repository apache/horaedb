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

## Builder
ARG GOLANG_VERSION=1.21.3
FROM golang:${GOLANG_VERSION}-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes gcc g++ libssl-dev pkg-config cmake && rm -rf /var/lib/apt/lists/*

COPY . /horaemeta
WORKDIR /horaemeta

RUN make build

## HoraeMeta
FROM ubuntu:20.04

RUN useradd -m -s /bin/bash horae

RUN apt update && \
    apt install --yes curl gdb iotop cron vim less net-tools && \
    apt clean

COPY --from=build /horaemeta/bin/horaemeta-server /usr/bin/horaemeta-server
RUN chmod +x /usr/bin/horaemeta-server

COPY ./docker/entrypoint.sh /entrypoint.sh
COPY ./config/example-standalone.toml /etc/horaemeta/horaemeta.toml

COPY ./docker/tini /tini
RUN chmod +x /tini

ARG USER horae

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]
