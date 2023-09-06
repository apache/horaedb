MODE ?= debug
ROOT = $(shell pwd)
CERESDB_DATA_DIR = /tmp/ceresdb
CERESDB_DATA_DIR_0 = /tmp/ceresdb0
CERESDB_DATA_DIR_1 = /tmp/ceresdb1
CERESMETA_DATA_DIR = /tmp/ceresmeta

export CERESDB_TEST_CASE_PATH ?= $(ROOT)/cases/env
export CERESDB_TEST_BINARY ?= $(ROOT)/../target/$(MODE)/ceresdb-test

# Environment variables for standalone
export CERESDB_SERVER_GRPC_ENDPOINT ?= 127.0.0.1:8831
export CERESDB_SERVER_HTTP_ENDPOINT ?= 127.0.0.1:5440
export CERESDB_BINARY_PATH ?= $(ROOT)/../target/$(MODE)/ceresdb-server
export CERESDB_STDOUT_FILE ?= /tmp/ceresdb-stdout.log
export CERESDB_CONFIG_FILE ?= $(ROOT)/../docs/minimal.toml

# Environment variables for cluster
export CERESMETA_BINARY_PATH ?= $(ROOT)/ceresmeta/ceresmeta
export CERESMETA_CONFIG_PATH ?= $(ROOT)/config/ceresmeta.toml
export CERESMETA_STDOUT_FILE ?= /tmp/ceresmeta-stdout.log
export CERESDB_CONFIG_FILE_0 ?= $(ROOT)/config/ceresdb-cluster-0.toml
export CERESDB_CONFIG_FILE_1 ?= $(ROOT)/config/ceresdb-cluster-1.toml
export CLUSTER_CERESDB_STDOUT_FILE_0 ?= /tmp/ceresdb-stdout-0.log
export CLUSTER_CERESDB_STDOUT_FILE_1 ?= /tmp/ceresdb-stdout-1.log
export RUST_BACKTRACE=1

# Whether update related repos
export UPDATE_REPOS_TO_LATEST ?= true

# Used in dist query test, we don't want to rebuild the binarie and data in sometimes(e.g. debugging),
# and we can set it to true.
export DIST_QUERY_TEST_NO_INIT ?= false

clean:
	rm -rf $(CERESDB_DATA_DIR) $(CERESDB_DATA_DIR_0) $(CERESDB_DATA_DIR_1) $(CERESMETA_DATA_DIR)

build-meta:
	sh ./build_meta.sh

build-ceresdb:
	cd .. && cargo build --bin ceresdb-server

build-test:
	cargo build

build: build-ceresdb build-test

kill-old-process:
	killall ceresdb-server | true
	killall ceresmeta | true

prepare: clean build kill-old-process

run: prepare build-meta
	$(CERESDB_TEST_BINARY)

run-local: prepare
	CERESDB_ENV_FILTER=local $(CERESDB_TEST_BINARY)

run-cluster: prepare build-meta
	CERESDB_ENV_FILTER=cluster $(CERESDB_TEST_BINARY)

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

run-recovery: clean build-ceresdb kill-old-process
	cd recovery && ./run.sh && ./run.sh shard_based

run-dist-query: prepare build-meta
	CERESDB_INTEGRATION_TEST_BIN_RUN_MODE=build_cluster $(CERESDB_TEST_BINARY)
	cd dist_query && ./run.sh
