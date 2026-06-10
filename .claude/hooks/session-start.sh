#!/bin/bash
# Installs build prerequisites for Legion so cargo build/test work in
# Claude Code on the web sessions. Currently: protoc, required by build.rs
# (tonic-build compiles proto/legion.proto). Idempotent and non-interactive.
set -euo pipefail

# Only needed in the remote (web) environment; local machines manage their own.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

if command -v protoc >/dev/null 2>&1; then
  exit 0
fi

if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y >/dev/null 2>&1 || apt-get update -y >/dev/null 2>&1 || true
  sudo apt-get install -y protobuf-compiler >/dev/null 2>&1 \
    || apt-get install -y protobuf-compiler >/dev/null 2>&1
fi

protoc --version
