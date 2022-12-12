
MODE ?= debug
ROOT = $(shell pwd)
DATA_DIR = /tmp/ceresdb

export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_CONFIG_PATH ?= $(ROOT)/../docs/sqlness.toml
export CERESDB_SERVER_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases
export CERESDB_TEST_SQLNESS ?= $(ROOT)/../target/$(MODE)/ceresdb-test-sqlness
export CERESDB_STDOUT_FILE ?= /tmp/ceresdb-stdout.log
export CERESDB_STDERR_FILE ?= /tmp/ceresdb-stderr.log

clean:
	rm -rf $(DATA_DIR)

build-ceresdb:
	cd .. && cargo build --bin ceresdb-server

build-sqlness:
	cd sqlness && cargo build

build: build-ceresdb build-sqlness

# rename ${case}.out to ${case}.result automatically. fd is required
# https://github.com/sharkdp/fd
fix:
	fd -t f out --exec mv {} {.}.result \;

run: clean build
	cd sqlness && $(CERESDB_TEST_SQLNESS)
