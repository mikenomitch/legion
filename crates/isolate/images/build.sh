#!/usr/bin/env bash
# Build Legion's isolate language images (README.md beside this file).
#
#   crates/isolate/images/build.sh [--out DIR] [--no-manifest] [js|python]...
#
# Writes js.wasm and python.wasm (default: both) and manifest.json into DIR
# (default: <workspace>/target/isolate-images). The toolchains are pinned
# (js/package.json + js/package-lock.json, requirements.txt) and installed
# into a cache directory on first use:
#
#   LEGION_IMAGES_TOOLS     where the toolchains are installed
#                           (default: <workspace>/target/isolate-images-tools)
#   LEGION_COMPONENTIZE_PY  an existing componentize-py binary to use instead
#                           (it must report the pinned version)
#   LEGION_JS_NODE_MODULES  an existing node_modules directory holding the
#                           pinned @bytecodealliance/componentize-js
#
# The manifest step runs the images under the host library
# (`cargo test -p legion-isolate --test images manifest_write_test`): it
# lists the Python modules that import inside the image and measures the
# idle memory per instance. --no-manifest skips it.
set -euo pipefail

COMPONENTIZE_PY_VERSION=0.25.1
COMPONENTIZE_JS_VERSION=0.23.0

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
out="$root/target/isolate-images"
tools="${LEGION_IMAGES_TOOLS:-$root/target/isolate-images-tools}"
manifest=1
images=()

die() {
    echo "build.sh: $*" >&2
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --out)
            [ $# -ge 2 ] || die "--out needs a directory"
            out="$2"
            shift 2
            ;;
        --out=*)
            out="${1#--out=}"
            shift
            ;;
        --no-manifest)
            manifest=0
            shift
            ;;
        js | python)
            images+=("$1")
            shift
            ;;
        -h | --help)
            sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) die "unknown argument '$1' (expected --out DIR, --no-manifest, js, python)" ;;
    esac
done
[ ${#images[@]} -gt 0 ] || images=(python js)
mkdir -p "$out"
out="$(cd "$out" && pwd)"

# One WIT directory for both toolchains: the images' worlds, the WASI
# packages, and legion:isolate from the host crate (the one source of truth).
wit="$out/.wit"
rm -rf "$wit"
mkdir -p "$wit/deps"
cp "$here/wit/images.wit" "$wit/"
cp "$here"/wit/deps/*.wit "$wit/deps/"
cp "$here/../wit/legion-isolate.wit" "$wit/deps/"

python_tool() {
    if [ -n "${LEGION_COMPONENTIZE_PY:-}" ]; then
        cpy="$LEGION_COMPONENTIZE_PY"
        [ -x "$cpy" ] || die "LEGION_COMPONENTIZE_PY=$cpy is not an executable"
    else
        cpy="$tools/py/bin/componentize-py"
        if [ ! -x "$cpy" ]; then
            command -v python3 >/dev/null 2>&1 ||
                die "python3 not found: the python image needs Python 3 with venv and pip to install componentize-py $COMPONENTIZE_PY_VERSION (or set LEGION_COMPONENTIZE_PY)"
            echo "installing componentize-py $COMPONENTIZE_PY_VERSION into $tools/py"
            rm -rf "$tools/py"
            python3 -m venv "$tools/py" ||
                die "python3 -m venv failed: install the venv module (Debian/Ubuntu: apt install python3-venv)"
            "$tools/py/bin/python" -m pip --version >/dev/null 2>&1 ||
                die "pip is missing from the venv: install pip for python3 (Debian/Ubuntu: apt install python3-pip)"
            "$tools/py/bin/python" -m pip install --quiet --require-hashes -r "$here/requirements.txt" ||
                die "installing componentize-py from requirements.txt failed"
        fi
    fi
    local v
    v="$("$cpy" --version 2>&1 || true)"
    case "$v" in
        *"$COMPONENTIZE_PY_VERSION"*) ;;
        *) die "$cpy reports '$v', expected componentize-py $COMPONENTIZE_PY_VERSION" ;;
    esac
}

js_tool() {
    command -v node >/dev/null 2>&1 ||
        die "node not found: the js image needs Node.js 20 or newer (and npm) to run componentize-js $COMPONENTIZE_JS_VERSION"
    if [ -n "${LEGION_JS_NODE_MODULES:-}" ]; then
        node_modules="$LEGION_JS_NODE_MODULES"
    else
        node_modules="$tools/js/node_modules"
        if [ ! -f "$node_modules/@bytecodealliance/componentize-js/package.json" ] ||
            ! cmp -s "$here/js/package-lock.json" "$tools/js/package-lock.json"; then
            command -v npm >/dev/null 2>&1 ||
                die "npm not found: it installs componentize-js $COMPONENTIZE_JS_VERSION (or set LEGION_JS_NODE_MODULES)"
            echo "installing componentize-js $COMPONENTIZE_JS_VERSION into $tools/js"
            mkdir -p "$tools/js"
            cp "$here/js/package.json" "$here/js/package-lock.json" "$tools/js/"
            (cd "$tools/js" && npm ci --no-audit --no-fund --loglevel=error) ||
                die "npm ci failed in $tools/js"
        fi
    fi
    local pkg="$node_modules/@bytecodealliance/componentize-js/package.json"
    [ -f "$pkg" ] || die "$node_modules has no @bytecodealliance/componentize-js"
    local v
    v="$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$pkg")"
    [ "$v" = "$COMPONENTIZE_JS_VERSION" ] ||
        die "$pkg is componentize-js $v, expected $COMPONENTIZE_JS_VERSION"
}

for image in "${images[@]}"; do
    case "$image" in
        python)
            python_tool
            echo "building python.wasm (componentize-py $COMPONENTIZE_PY_VERSION)"
            # componentize-py bundles every file under -p; build from a copy
            # holding only the runner and the legion module.
            src="$out/.python-src"
            rm -rf "$src"
            mkdir -p "$src"
            cp "$here/python/_legion_runner.py" "$here/python/legion.py" "$src/"
            (cd "$src" && PYTHONDONTWRITEBYTECODE=1 "$cpy" -d "$wit" -w python componentize _legion_runner -p . -o "$out/python.wasm.tmp") ||
                die "componentize-py failed"
            mv "$out/python.wasm.tmp" "$out/python.wasm"
            ;;
        js)
            js_tool
            echo "building js.wasm (componentize-js $COMPONENTIZE_JS_VERSION)"
            node "$here/js/build.mjs" "$node_modules" "$here/js/runner.js" "$wit" "$out/js.wasm.tmp" ||
                die "componentize-js failed"
            mv "$out/js.wasm.tmp" "$out/js.wasm"
            ;;
    esac
done

for image in "${images[@]}"; do
    sha="$(sha256sum "$out/$image.wasm" | cut -d' ' -f1)"
    echo "$image.wasm: $(stat -c %s "$out/$image.wasm") bytes, sha256 $sha"
done

if [ "$manifest" = 1 ]; then
    command -v cargo >/dev/null 2>&1 || die "cargo not found: the manifest step runs the images under the host library (pass --no-manifest to skip it)"
    echo "writing $out/manifest.json (runs the images under the host library)"
    (cd "$root" && LEGION_ISOLATE_IMAGES="$out" LEGION_ISOLATE_MANIFEST_WRITE=1 \
        cargo test -p legion-isolate -j "${CARGO_BUILD_JOBS:-3}" --test images -- \
        --ignored --exact manifest_write_test --nocapture) ||
        die "the manifest step failed"
fi
