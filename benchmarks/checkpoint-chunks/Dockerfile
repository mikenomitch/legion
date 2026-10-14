FROM debian:bookworm
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
    qemu-system-arm qemu-utils linux-image-cloud-arm64 initramfs-tools python3 e2fsprogs procps \
    && rm -rf /var/lib/apt/lists/*
