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

SHELL = /bin/bash

DIR=$(shell pwd)

init:
	echo "init"
	echo "Git branch: $GITBRANCH"

build-debug:
	ls -alh
	cd $(DIR); cargo build $(CARGO_FEATURE_FLAGS)

build:
	ls -alh
	cd $(DIR); cargo build --release $(CARGO_FEATURE_FLAGS)

test:
	cd $(DIR); cargo test --workspace -- --test-threads=4

fmt:
	cd $(DIR); cargo fmt -- --check

sort:
	cd $(DIR); cargo sort --workspace --check

check-asf-header:
	cd $(DIR); hawkeye check

udeps:
	cd $(DIR); cargo udeps --all-targets --all-features --workspace

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings -D clippy::dbg-macro -A clippy::too-many-arguments \
	-A dead_code

ensure-disk-quota:
	bash ./scripts/free-disk-space.sh


fix:
	cargo fmt
	cargo sort --workspace
	cargo clippy --fix --allow-staged --all-targets --all-features --workspace -- -D warnings

update-licenses:
	cargo install --locked cargo-deny && cargo deny list -f tsv -l crate > DEPENDENCIES.tsv
