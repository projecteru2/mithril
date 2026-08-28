.PHONY: all build test lint fmt fmt-check clean help

REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILTAT := $(shell date +%Y-%m-%dT%H:%M:%S)
VERSION := $(shell git describe --tags $(shell git rev-list --tags --max-count=1 2>/dev/null) 2>/dev/null || echo dev)

export MITHRIL_VERSION := $(VERSION)
export MITHRIL_REVISION := $(REVISION)
export MITHRIL_BUILTAT := $(BUILTAT)

all: build ## default: release build

build: ## release binary at target/release/mithril
	cargo build --release

test: ## unit tests
	cargo test

lint: ## clippy with warnings denied
	cargo clippy --all-targets -- -D warnings

fmt: ## format sources
	cargo fmt

fmt-check: ## fail on unformatted sources
	cargo fmt --check

clean:
	cargo clean

help: ## show targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
