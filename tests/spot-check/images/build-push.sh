#!/usr/bin/env bash
# Builds the spot-check images for linux/amd64 (the AWS workers) and
# linux/arm64 (Apple Silicon, for local dev), and pushes them to the rig's
# private ECR repositories that up.sh created.
#
#   tests/spot-check/images/build-push.sh            # all images
#   tests/spot-check/images/build-push.sh web api    # some
#
# Uses a private DOCKER_CONFIG under .state, so the ECR login and the
# buildx builder never touch ~/.docker.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"

export DOCKER_CONFIG="$SC_STATE/docker"
mkdir -p "$DOCKER_CONFIG/cli-plugins"
for plugin in "$HOME"/.docker/cli-plugins/docker-buildx /usr/local/lib/docker/cli-plugins/docker-buildx; do
    [ -e "$plugin" ] && ln -sf "$plugin" "$DOCKER_CONFIG/cli-plugins/docker-buildx" && break
done

aws ecr get-login-password | docker login --username AWS --password-stdin "$SC_ECR" >/dev/null
# A docker-container builder can push multi-platform manifests on any engine.
docker buildx inspect sc-builder >/dev/null 2>&1 || docker buildx create --name sc-builder --driver docker-container >/dev/null

images=("$@")
[ ${#images[@]} -gt 0 ] || images=(web api frontend agent db)
for image in "${images[@]}"; do
    sc_log "Building spot-check/$image"
    # gzip layers: Legion rejects zstd-compressed layers.
    docker buildx build --builder sc-builder --platform linux/amd64,linux/arm64 \
        --output "type=image,name=$SC_ECR/spot-check/$image:latest,push=true,compression=gzip,oci-mediatypes=true" \
        "$SC_DIR/images/$image"
done
sc_log "Pushed: ${images[*]}"
