MODE ?= debug
ROOT = $(shell pwd)
DATA_DIR = /tmp/ceresdb

export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases/env
export CERESDB_TEST_BINARY ?= $(ROOT)/../target/$(MODE)/ceresdb-test

# environment variables for standalone
export CERESDB_SERVER_GRPC_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_SERVER_HTTP_ENDPOINT ?= 127.0.0.1:5440
export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_STDOUT_FILE ?= /tmp/ceresdb-stdout.log
export CERESDB_CONFIG_FILE ?= $(ROOT)/config/ceresdb-standalone.toml

# environment variables for cluster
export CERESMETA_BINARY_PATH ?= $(ROOT)/ceresmeta/ceresmeta
export CERESMETA_CONFIG_PATH ?= $(ROOT)/config/ceresmeta.toml
export CERESMETA_STDOUT_FILE ?= /tmp/ceresmeta-stdout.log
export CERESDB_CONFIG_FILE_0 ?= $(ROOT)/config/ceresdb-cluster-0.toml
export CERESDB_CONFIG_FILE_1 ?= $(ROOT)/config/ceresdb-cluster-1.toml
export CLUSTER_CERESDB_STDOUT_FILE_0 ?= /tmp/ceresdb-stdout-0.log
export CLUSTER_CERESDB_STDOUT_FILE_1 ?= /tmp/ceresdb-stdout-1.log

clean:
	rm -rf $(DATA_DIR)

build-meta:
	sh ./build_meta.sh $(CERESMETA_BINARY_PATH)

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