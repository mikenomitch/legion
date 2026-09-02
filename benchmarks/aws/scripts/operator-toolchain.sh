#!/usr/bin/env bash
# Everything the operator box needs installed, and the ONE copy of that
# list. Two callers embed this file verbatim into EC2 user-data:
#
#   bake-operator-ami.sh        runs it once, on a throwaway instance,
#                               and freezes the result into an AMI
#   bootstrap-operator-box.sh   runs it at first boot, but only when no
#                               baked AMI exists (the slow path)
#
# It is one file because the two paths must install the same thing. A
# second copy of this list is the failure this split is most likely to
# cause: the baked image quietly lags a tool the fallback path already
# has, and the box that launches from it fails hours later inside a
# cargo build. bootstrap-operator-box.sh additionally compares this
# file's hash against the tag on the AMI and refuses a stale image, so
# adding a tool here cannot be forgotten — it just costs a re-bake.
#
# Contract for anything added below, in exchange for that:
#
#   - It runs as root on a fresh Ubuntu 24.04 amd64 image, non-interactive.
#   - It must be instance-INDEPENDENT. Whatever this file does ends up
#     inside a disk image that every future operator box starts from, so
#     no hostnames, no addresses, no keys, no credentials, nothing
#     unique to one box. Per-instance setup — the rig's SSH key pair
#     above all — lives in bootstrap-operator-box.sh's user-data instead.
#   - It must be re-runnable, because on the slow path it runs on a box
#     that may already be half set up by a previous cloud-init attempt.
#
# Why this is worth a separate image at all: measured on the rig,
# `cloud-init analyze blame` put modules-final/config-scripts_user at
# 3245 seconds (54 minutes) on one launch and ~14 minutes on another —
# same instance type, same region, same script. It is nearly all
# apt-get, and it is paid before a single line of Legion compiles.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
# clang/libclang-dev are not optional: rusqlite is built with the
# `session` feature, which forces libsqlite3-sys to run bindgen, and
# bindgen needs libclang plus clang's own builtin headers (without them
# it fails on `'stdarg.h' file not found`). CI does not surface this —
# GitHub's ubuntu-latest runners ship clang preinstalled, a bare Ubuntu
# AMI does not.
apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev protobuf-compiler \
    clang libclang-dev \
    git jq curl unzip tmux rsync ca-certificates gnupg \
    python3 python3-pip python3-venv

# HashiCorp apt repo — terraform, pinned to the same >= 1.6 the rig's
# terraform block requires.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://apt.releases.hashicorp.com/gpg |
    gpg --batch --yes --dearmor -o /etc/apt/keyrings/hashicorp.gpg
echo "deb [signed-by=/etc/apt/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
    >/etc/apt/sources.list.d/hashicorp.list
apt-get update && apt-get install -y terraform

# AWS CLI v2. Ubuntu's packaged awscli is v1 and misses commands the
# campaign scripts use. `--update` so a re-run over an existing install
# succeeds instead of failing on "already installed".
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q -o /tmp/awscliv2.zip -d /tmp && /tmp/aws/install --update && rm -rf /tmp/aws /tmp/awscliv2.zip

sudo -u ubuntu bash <<'ASUSER'
set -euxo pipefail
cd "$HOME"

# `-y` alone re-installs happily, but the appended .bashrc line would
# then arrive twice on a re-run, so add it only when it is missing.
curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
grep -qF 'source "$HOME/.cargo/env"' "$HOME/.bashrc" ||
    echo 'source "$HOME/.cargo/env"' >>"$HOME/.bashrc"
ASUSER

# /usr/local/bin is on the default PATH for a non-interactive `ssh host
# "cmd"`; ~/.cargo/bin is not, because rustup adds it via ~/.bashrc and
# Ubuntu's .bashrc returns early for non-interactive shells. Every remote
# command the campaign scripts issue arrives that way, so expose the
# toolchain somewhere those shells can actually see it.
for b in cargo rustc rustup; do
    ln -sf /home/ubuntu/.cargo/bin/$b /usr/local/bin/$b
done
