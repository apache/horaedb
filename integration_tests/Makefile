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

MODE ?= debug
ROOT = $(shell pwd)
HORAEDB_DATA_DIR = /tmp/horaedb
HORAEDB_DATA_DIR_0 = /tmp/horaedb0
HORAEDB_DATA_DIR_1 = /tmp/horaedb1
HORAEMETA_DATA_DIR = /tmp/horaemeta

export HORAEDB_TEST_CASE_PATH ?= $(ROOT)/cases/env
export HORAEDB_TEST_BINARY ?= $(ROOT)/../target/$(MODE)/horaedb-test

# Environment variables for standalone
export HORAEDB_SERVER_GRPC_ENDPOINT ?= 127.0.0.1:8831
export HORAEDB_SERVER_HTTP_ENDPOINT ?= 127.0.0.1:5440
export HORAEDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/horaedb-server
export HORAEDB_STDOUT_FILE ?= /tmp/horaedb-stdout.log
export HORAEDB_CONFIG_FILE ?= $(ROOT)/../docs/minimal.toml

# Environment variables for cluster
export HORAEMETA_BINARY_PATH ?= $(ROOT)/../target/horaemeta-server
export HORAEMETA_CONFIG_PATH ?= $(ROOT)/config/horaemeta.toml
export HORAEMETA_STDOUT_FILE ?= /tmp/horaemeta-stdout.log
export HORAEDB_CONFIG_FILE_0 ?= $(ROOT)/config/horaedb-cluster-0.toml
export HORAEDB_CONFIG_FILE_1 ?= $(ROOT)/config/horaedb-cluster-1.toml
export CLUSTER_HORAEDB_STDOUT_FILE_0 ?= /tmp/horaedb-stdout-0.log
export CLUSTER_HORAEDB_STDOUT_FILE_1 ?= /tmp/horaedb-stdout-1.log
export RUST_BACKTRACE=1

# Whether update related repos
# We don't want to rebuild the binaries and data on sometimes(e.g. debugging in local),
# and we can set it to false.
export UPDATE_REPOS_TO_LATEST ?= true

clean:
	rm -rf $(HORAEDB_DATA_DIR) $(HORAEDB_DATA_DIR_0) $(HORAEDB_DATA_DIR_1) $(HORAEMETA_DATA_DIR)

build-meta:
	./build_meta.sh

build-horaedb:
	cd .. && cargo build --bin horaedb-server --features wal-table-kv,wal-message-queue,wal-rocksdb,wal-local-storage

build-test:
	cargo build

build: build-horaedb build-test

kill-old-horaemeta:
	killall horaemeta-server | true

kill-old-horaedb:
	killall horaedb-server | true

kill-old-process: kill-old-horaemeta kill-old-horaedb

prepare: clean build kill-old-process

run-horaemeta: build-meta
	nohup $(HORAEMETA_BINARY_PATH) --config ${HORAEMETA_CONFIG_PATH} > /tmp/horaemeta-stdout.log 2>&1 &
	sleep 10

run-horaedb-cluster: build-horaedb
	nohup ${HORAEDB_BINARY_PATH} --config ${HORAEDB_CONFIG_FILE_0} > ${CLUSTER_HORAEDB_STDOUT_FILE_0} 2>&1 &
	nohup ${HORAEDB_BINARY_PATH} --config ${HORAEDB_CONFIG_FILE_1} > ${CLUSTER_HORAEDB_STDOUT_FILE_1} 2>&1 &
	sleep 30

run: prepare build-meta
	$(HORAEDB_TEST_BINARY)

run-local: prepare
	HORAEDB_ENV_FILTER=local $(HORAEDB_TEST_BINARY)

run-cluster: prepare build-meta
	HORAEDB_ENV_FILTER=cluster $(HORAEDB_TEST_BINARY)

run-java:
	java -version
	cd sdk/java && MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn clean compile exec:java

run-go:
	cd sdk/go && go run .

run-rust:
	cd sdk/rust && cargo run

run-mysql:
	cd mysql && ./basic.sh

run-postgresql:
	cd postgresql && ./basic.sh

run-prom:
	cd prom && ./run-tests.sh

run-opentsdb:
	cd opentsdb && ./run-tests.sh

run-recovery: clean build-horaedb kill-old-process
	cd recovery && ./run.sh && ./run.sh shard_based

run-dist-query: prepare build-meta
	HORAEDB_INTEGRATION_TEST_BIN_RUN_MODE=build_cluster $(HORAEDB_TEST_BINARY)
	cd dist_query && ./run.sh
