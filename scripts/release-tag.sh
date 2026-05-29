#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VERSION="$(
  awk '
    $0 == "[package]" { in_package = 1; next }
    /^\[/ && $0 != "[package]" { in_package = 0 }
    in_package && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' Cargo.toml
)"

if [[ -z "$VERSION" ]]; then
  echo "failed to read package version from Cargo.toml" >&2
  exit 1
fi

TAG="v$VERSION"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "git working tree is dirty; commit or stash changes before tagging" >&2
  exit 1
fi

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "tag already exists: $TAG" >&2
  exit 1
fi

cargo test

VERSION_OUTPUT="$(cargo run -- --version)"
EXPECTED_OUTPUT="mlab $VERSION"

if [[ "$VERSION_OUTPUT" != "$EXPECTED_OUTPUT" ]]; then
  echo "version check failed: expected '$EXPECTED_OUTPUT', got '$VERSION_OUTPUT'" >&2
  exit 1
fi

git tag "$TAG"
git push origin main
git push origin "$TAG"

echo "released $TAG"
