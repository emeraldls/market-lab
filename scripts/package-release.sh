#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
HOST_TARGET="$(rustc -vV | sed -n 's/^host: //p')"

resolve_version() {
  awk '
    $0 == "[package]" { in_package = 1; next }
    /^\[/ && $0 != "[package]" { in_package = 0 }
    in_package && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$ROOT_DIR/Cargo.toml"
}

resolve_binary_path() {
  local target="$1"
  if [[ "$target" == "$HOST_TARGET" ]]; then
    echo "target/release/mlab"
  else
    echo "target/$target/release/mlab"
  fi
}

if [[ $# -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 1
fi

TARGET="${TARGET:-$HOST_TARGET}"
VERSION="$(resolve_version)"
BINARY_PATH="$(resolve_binary_path "$TARGET")"

if [[ -z "${VERSION:-}" ]]; then
  echo "failed to resolve package version from Cargo.toml" >&2
  exit 1
fi

PACKAGE_DIR="$DIST_DIR/mlab-${VERSION}-${TARGET}"
ARCHIVE_PATH="$DIST_DIR/mlab-${VERSION}-${TARGET}.tar.gz"

if [[ ! -f "$BINARY_PATH" ]]; then
  echo "binary not found: $BINARY_PATH" >&2
  exit 1
fi

rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR"

cp "$BINARY_PATH" "$PACKAGE_DIR/mlab"

if [[ -f "$ROOT_DIR/LICENSE" ]]; then
  cp "$ROOT_DIR/LICENSE" "$PACKAGE_DIR/LICENSE"
fi

chmod +x "$PACKAGE_DIR/mlab"
mkdir -p "$DIST_DIR"

tar -C "$DIST_DIR" -czf "$ARCHIVE_PATH" "mlab-${VERSION}-${TARGET}"

echo "$ARCHIVE_PATH"
