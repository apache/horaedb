
MODE ?= debug
ROOT = $(shell pwd)

export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_SERVER_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases
export CERESDB_TEST_HARNESS ?= $(ROOT)/../target/$(MODE)/ceresdb-test

run:
	cd harness && $(CERESDB_TEST_HARNESS)
