SHELL = /bin/bash

DIR=$(shell pwd)

init:
	echo "init"
	echo "Git branch: $GITBRANCH"

build:
	ls -alh
	cd $(DIR); cargo build --release

build-asan:
	ls -alh
	export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
	cd $(DIR); cargo build -Zbuild-std --target x86_64-unknown-linux-gnu --release

build-arm64:
	ls -alh
	cd $(DIR); cargo build --release --no-default-features

test:
	cd $(DIR); cargo test --workspace -- --test-threads=4

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
#	cd $(DIR); cargo test --workspace -- -Z unstable-options --format json | tee results.json; \
# 	cat results.json | cargo2junit > ${WORKSPACE}/testresult/TEST-all.xml
	cd $(DIR); cargo test --workspace

fmt:
	cd $(DIR); cargo fmt -- --check

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
