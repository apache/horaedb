
MODE ?= debug
ROOT = $(shell pwd)
DATA_DIR = /tmp/ceresdb

export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_SERVER_GRPC_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_SERVER_HTTP_ENDPOINT ?= 127.0.0.1:5440
export CERESDB_CLUSTER_SERVER_ENDPOINT ?= 127.0.0.1:8832
export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases/env
export CERESDB_TEST_BINARY ?= $(ROOT)/../target/$(MODE)/ceresdb-test
export CERESDB_STDOUT_FILE ?= /tmp/ceresdb-stdout.log
export CERESDB_STDERR_FILE ?= /tmp/ceresdb-stderr.log

clean:
	rm -rf $(DATA_DIR)

build-ceresdb:
	cd .. && cargo build --bin ceresdb-server

build-test:
	cargo build

build: build-ceresdb build-test

# rename ${case}.output to ${case}.result automatically. fd is required
# https://github.com/sharkdp/fd
fix:
	fd -t f output --exec mv {} {.}.result \;

run: clean build
	$(CERESDB_TEST_BINARY)
