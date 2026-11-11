#!/bin/bash
# First-boot setup for the sandbox-boot build box: the Rust toolchain and the
# musl target for the static guest agent.
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q build-essential pkg-config protobuf-compiler clang libclang-dev musl-tools rsync jq
snap install aws-cli --classic
sudo -u ubuntu bash -c 'curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.90.0 --profile minimal && ~/.cargo/bin/rustup target add x86_64-unknown-linux-musl'
touch /var/tmp/build-ready
