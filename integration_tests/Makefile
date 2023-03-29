MODE ?= debug
ROOT = $(shell pwd)
CERESDB_DATA_DIR = /tmp/ceresdb
CERESDB_DATA_DIR_0 = /tmp/ceresdb0
CERESDB_DATA_DIR_1 = /tmp/ceresdb1
CERESMETA_DATA_DIR = /tmp/ceresmeta

export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases/env
export CERESDB_TEST_BINARY ?= $(ROOT)/../target/$(MODE)/ceresdb-test

# environment variables for standalone
export CERESDB_SERVER_GRPC_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_SERVER_HTTP_ENDPOINT ?= 127.0.0.1:5440
export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_STDOUT_FILE ?= /tmp/ceresdb-stdout.log
export CERESDB_CONFIG_FILE ?= $(ROOT)/../docs/minimal.toml

# environment variables for cluster
export CERESMETA_BINARY_PATH ?= $(ROOT)/ceresmeta/ceresmeta
export CERESMETA_CONFIG_PATH ?= $(ROOT)/config/ceresmeta.toml
export CERESMETA_STDOUT_FILE ?= /tmp/ceresmeta-stdout.log
export CERESDB_CONFIG_FILE_0 ?= $(ROOT)/config/ceresdb-cluster-0.toml
export CERESDB_CONFIG_FILE_1 ?= $(ROOT)/config/ceresdb-cluster-1.toml
export CLUSTER_CERESDB_STDOUT_FILE_0 ?= /tmp/ceresdb-stdout-0.log
export CLUSTER_CERESDB_STDOUT_FILE_1 ?= /tmp/ceresdb-stdout-1.log

clean:
	rm -rf $(CERESDB_DATA_DIR) $(CERESDB_DATA_DIR_0) $(CERESDB_DATA_DIR_1) $(CERESMETA_DATA_DIR)

build-meta:
	sh ./build_meta.sh

build-ceresdb:
	cd .. && cargo build --bin ceresdb-server

build-test:
	cargo build

build: build-ceresdb build-test build-meta

kill-old-process:
	killall ceresdb-server | true
	killall ceresmeta | true

run: clean build kill-old-process
	RUST_BACKTRACE=1 $(CERESDB_TEST_BINARY)

run-java:
	java -version
	cd sdk/java && MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn clean compile exec:java
