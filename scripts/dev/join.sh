#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: mise run join -- [options] [join args]

Options:
  --linux                 Run worker(s) in Linux containers via Apple `container`.
  --count <n>             Number of workers to start in Linux mode (default: 1).
  --no-log-tail           Skip printing startup logs after each Linux worker starts.
  --log-tail-lines <n>    Number of startup log lines to print per worker (default: 30).
  --manager-addr <addr>   Manager address workers should connect to.
                          Defaults:
                          - local mode: http://127.0.0.1:4748
                          - linux mode: http://192.168.64.1:4748
  --name-prefix <prefix>  Linux container name prefix (default: legion-dev-worker).
  --platform <platform>   Container platform in Linux mode (default: linux/arm64).
  -h, --help              Show this help.

Any remaining arguments are forwarded to `cargo run -- join ...`.
If no join args are provided, `-a` is used.
USAGE
}

linux_mode=false
count=1
manager_addr=""
name_prefix="legion-dev-worker"
platform="linux/arm64"
log_tail_lines=30
tail_logs=true
declare -a join_args=()

while (($# > 0)); do
    case "$1" in
    --linux)
        linux_mode=true
        ;;
    --count)
        shift
        count="${1:-}"
        ;;
    --manager-addr)
        shift
        manager_addr="${1:-}"
        ;;
    --no-log-tail)
        tail_logs=false
        ;;
    --log-tail-lines)
        shift
        log_tail_lines="${1:-}"
        ;;
    --name-prefix)
        shift
        name_prefix="${1:-}"
        ;;
    --platform)
        shift
        platform="${1:-}"
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    --)
        shift
        while (($# > 0)); do
            join_args+=("$1")
            shift
        done
        break
        ;;
    *)
        join_args+=("$1")
        ;;
    esac
    shift
done

if [[ -z "$manager_addr" ]]; then
    if [[ "$linux_mode" == true ]]; then
        manager_addr="http://192.168.64.1:4748"
    else
        manager_addr="http://127.0.0.1:4748"
    fi
fi

if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
    echo "--count must be a positive integer" >&2
    exit 1
fi

if ! [[ "$log_tail_lines" =~ ^[0-9]+$ ]]; then
    echo "--log-tail-lines must be a non-negative integer" >&2
    exit 1
fi

if ((${#join_args[@]} == 0)); then
    join_args=(-a)
fi

if [[ "$linux_mode" != true ]]; then
    LEGION_ADDR="$manager_addr" cargo run -- join "${join_args[@]}"
    exit 0
fi

if ! command -v container >/dev/null 2>&1; then
    echo "apple 'container' is required for --linux mode" >&2
    echo "Install and start it first: https://github.com/apple/container" >&2
    exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

container build \
    --file "$repo_root/docker/dev/worker.Dockerfile" \
    --platform "$platform" \
    --tag legion-dev-worker:latest \
    "$repo_root"

started_at="$(date +%s)"
for ((i = 1; i <= count; i++)); do
    container_name="${name_prefix}-${started_at}-${i}"

    quoted_join_args=""
    for arg in "${join_args[@]}"; do
        quoted_join_args+=" $(printf %q "$arg")"
    done

    container run -d --rm \
        --name "$container_name" \
        --platform "$platform" \
        --env "LEGION_ADDR=$manager_addr" \
        --volume "$repo_root:/workspace/legion" \
        --volume "legion-worker-cargo-registry:/usr/local/cargo/registry" \
        --volume "legion-worker-cargo-git:/usr/local/cargo/git" \
        --volume "legion-worker-target:/workspace/legion/target" \
        --workdir /workspace/legion \
        legion-dev-worker:latest \
        bash -lc "cargo run -- join${quoted_join_args}"

    echo "Started $container_name"

    if [[ "$tail_logs" == true ]] && ((log_tail_lines > 0)); then
        echo "--- startup logs: $container_name (tail -n $log_tail_lines) ---"
        if ! container logs --tail "$log_tail_lines" "$container_name"; then
            echo "unable to fetch logs for $container_name yet; retry with: container logs --follow $container_name"
        fi
        echo "--- end logs: $container_name ---"
    fi
done

echo "Workers will connect to $manager_addr"
echo "Use: container logs --follow <container-name>"
echo "Stop all with: container delete --force ${name_prefix}-${started_at}-1 ... ${name_prefix}-${started_at}-${count}"
