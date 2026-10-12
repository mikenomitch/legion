#!/usr/bin/env bash
# Builds spot-check images for Apple Silicon and loads them into Apple
# `container`'s local image store as spot-check-<name>:dev, for the macOS jobs.
#
#   tests/spot-check/mac/build-local.sh            # web and agent
#   tests/spot-check/mac/build-local.sh web
#
# `container build` works too, but its builder needs Rosetta
# (`softwareupdate --install-rosetta`). This path uses Docker buildx (OrbStack
# or Docker Desktop) to write an OCI archive instead, so it needs neither
# Rosetta nor a registry login.
set -euo pipefail
dir="$(cd "$(dirname "$0")/.." && pwd)"
out="$dir/.state/oci"
mkdir -p "$out"

images=("$@")
[ ${#images[@]} -gt 0 ] || images=(web agent)
for image in "${images[@]}"; do
    echo "==> spot-check-$image:dev"
    docker buildx build --platform linux/arm64 \
        --output "type=oci,dest=$out/$image.tar,name=spot-check-$image:dev,compression=gzip" \
        "$dir/images/$image"
    container image load --input "$out/$image.tar"
done
container image list | grep spot-check
