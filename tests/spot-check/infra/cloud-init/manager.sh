#!/usr/bin/env bash
# First-boot setup for a spot-check manager (Ubuntu 24.04, x86_64). Legion is
# installed later by install.sh.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -q
apt-get install -y -q curl jq
mkdir -p /opt/legion/bin /opt/legion/configs/workers /var/lib/legion
echo "spot-check manager bootstrap done"
