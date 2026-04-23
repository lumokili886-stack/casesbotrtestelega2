#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
TARGET="$ROOT_DIR/versions/$STAMP"
mkdir -p "$TARGET"
cp "$ROOT_DIR/cs2-miniapp.html" "$TARGET/"
cp "$ROOT_DIR/server.js" "$TARGET/"
cp "$ROOT_DIR/README.md" "$TARGET/"
cp "$ROOT_DIR/package.json" "$TARGET/"
cp "$ROOT_DIR/.env.example" "$TARGET/"
echo "$STAMP" > "$ROOT_DIR/VERSION"
echo "Saved snapshot to $TARGET"
