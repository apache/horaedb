
MODE ?= debug
ROOT = $(shell pwd)

export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_SERVER_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases
export CERESDB_TEST_HARNESS ?= $(ROOT)/harness/target/$(MODE)/ceresdb-test

run:
	echo $(CERESDB_TEST_CASE_PATH)
	cd harness && $(CERESDB_TEST_HARNESS)
