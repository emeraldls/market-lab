VERSION := $(shell awk '$$0 == "[package]" { in_package = 1; next } /^\[/ && $$0 != "[package]" { in_package = 0 } in_package && $$1 == "version" { gsub(/"/, "", $$3); print $$3; exit }' Cargo.toml)

.PHONY: build build-release version package release-tag

build:
	cargo build

build-release:
	cargo build --release

version:
	@echo $(VERSION)

package: build-release
	scripts/package-release.sh

release-tag:
	scripts/release-tag.sh
