SHELL = /bin/bash

DIR=$(shell pwd)

init:
	echo "init"
	echo "Git branch: $GITBRANCH"

build-debug:
	ls -alh
	cd $(DIR); cargo build

build:
	ls -alh
	cd $(DIR); cargo build --release

build-slim:
	ls -alh
	cd $(DIR); cargo build --profile release-slim

build-asan:
	ls -alh
	export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
	cd $(DIR); cargo build -Zbuild-std --target x86_64-unknown-linux-gnu --release

build-arm64:
	ls -alh
	cd $(DIR); cargo build --release --no-default-features

test:
	cd $(DIR); cargo test --workspace -- --test-threads=4

integration-test:
	cd $(DIR)/integration_tests; make run

# grcov needs build first, then run test
build-ut:
	echo $(CARGO_INCREMENTAL)
	echo $(RUSTFLAGS)
	echo $(RUSTDOCFLAGS)
	cd $(DIR); cargo build --workspace

test-ut:
	echo $(CARGO_INCREMENTAL)
	echo $(RUSTFLAGS)
	echo $(RUSTDOCFLAGS)
	#cd $(DIR); cargo test --workspace -- -Z unstable-options --format json | tee results.json; \
	#cat results.json | cargo2junit > ${WORKSPACE}/testresult/TEST-all.xml
	cargo test --workspace

fmt:
	cd $(DIR); cargo fmt -- --check

check-cargo-toml:
	cd $(DIR); cargo sort --workspace --check

check-license:
	cd $(DIR); sh scripts/check-license.sh

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings

# test with address sanitizer
asan-test:
	export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
	cd $(DIR); cargo test -Zbuild-std --target x86_64-unknown-linux-gnu --workspace

# test with address sanitizer under release mode to workaround `attempt to create unaligned or null slice`
# error in parquet crate.
asan-test-release:
	export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
	cd $(DIR); cargo test -Zbuild-std --target x86_64-unknown-linux-gnu --release --workspace

# test with memory sanitizer
mem-test:
	export RUSTFLAGS=-Zsanitizer=memory RUSTDOCFLAGS=-Zsanitizer=memory
	cd $(DIR); cargo test -Zbuild-std --target x86_64-unknown-linux-gnu --workspace

# test with miri.
# only list packages will be tested.
miri:
	cd $(DIR); cargo miri test --package arena

ensure-disk-quota:
	# ensure the target directory not to exceed 40GB
	python3 ./scripts/clean-large-folder.py ./target 42949672960

tsbs:
	cd $(DIR); sh scripts/run-tsbs.sh
