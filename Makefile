# Refers to https://github.com/tikv/pd/blob/master/Makefile

default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@which golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.46.2
	@grep '_' tools.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

META_PKG := github.com/CeresDB/ceresmeta
PACKAGES := $(shell go list ./... | tail -n +2) 
PACKAGE_DIRECTORIES := $(subst $(META_PKG)/,,$(PACKAGES))

check: install-tools
	@ echo "gofmt ..."
	@ gofmt -s -l -d $(PACKAGE_DIRECTORIES) 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ echo "golangci-lint ..."
	@ golangci-lint run $(PACKAGE_DIRECTORIES)
	@ echo "revive ..."
	@ revive -formatter friendly -config revive.toml $(PACKAGES)

# TODO: support build rule
build: check
