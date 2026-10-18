#!/bin/sh
# Rebuild bpf/legion_net.bpf.o from legion_net.bpf.c. The object is checked
# in, so `cargo build` needs only protoc; CI runs this and fails when the
# result differs from the committed object (`bpf/build.sh --check`).
#
# Needs clang with the bpf target and the kernel's asm/types.h
# (linux-libc-dev). linux/bpf.h is vendored in include/ (copied from Ubuntu's
# linux-libc-dev 6.8.0-147) so a distro header update cannot change the
# object's debug info and fail the check. The programs use only stable UAPI
# context fields, so the object loads on any kernel with the features
# `netKernel` probes for.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
clang=${CLANG:-clang}
arch_include=/usr/include/$(uname -m)-linux-gnu
out="$here/legion_net.bpf.o"
if [ "${1:-}" = "--check" ]; then
	out=$(mktemp)
	trap 'rm -f "$out"' EXIT
fi
# -g emits BTF (the loader needs it for the maps). Compiling from the
# repository root with relative paths and a fixed compilation directory keeps
# the object independent of where the repository is checked out and of the
# directory the script is run from.
cd "$here/.."
"$clang" -O2 -g -target bpf -Wall -Werror -fdebug-compilation-dir=. \
	-Ibpf/include -I"$arch_include" -Ibpf \
	-c bpf/legion_net.bpf.c -o "$out"
if [ "${1:-}" = "--check" ]; then
	if ! cmp -s "$out" "$here/legion_net.bpf.o"; then
		echo "bpf/legion_net.bpf.o is stale: run bpf/build.sh and commit the result" >&2
		exit 1
	fi
	echo "bpf/legion_net.bpf.o is current"
fi
