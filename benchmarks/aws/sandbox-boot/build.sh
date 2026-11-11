#!/usr/bin/env bash
# Builds Linux binaries on the rig's build box and installs them on every rig
# box, straight from S3.
#
#   benchmarks/aws/sandbox-boot/build.sh sync     # this checkout (or SRC=<dir>)
#   benchmarks/aws/sandbox-boot/build.sh build    # legion, legion-loadgen, legion-guest
#   benchmarks/aws/sandbox-boot/build.sh push     # build box -> s3://<bucket>/<tag>/
#   benchmarks/aws/sandbox-boot/build.sh deploy   # every box pulls <tag> into /opt/legion/bin
#   benchmarks/aws/sandbox-boot/build.sh all      # sync + build + push + deploy
#   benchmarks/aws/sandbox-boot/build.sh stop|start
#
# TAG (default `current`) names the build, so a baseline and a candidate can
# sit side by side: TAG=base SRC=../legion-base build.sh all.
set -euo pipefail
source "$(dirname "$0")/env.sh"

SRC="${SRC:-$SB_REPO}"
TAG="${TAG:-current}"
bin_list="legion legion-loadgen"

bb_id() {
    aws ec2 describe-instances --filters "Name=tag:Name,Values=sbx-build" "Name=tag:Project,Values=$SB_PROJECT" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text
}

case "${1:-}" in
sync)
    files="$(mktemp)"
    git -C "$SRC" ls-files -co --exclude-standard | grep -v -E '^(ui/|target)' >"$files"
    sb_ssh sbx-build "mkdir -p src-$TAG && cd src-$TAG && rm -rf src tests docs proto guest configs plugins benchmarks"
    rsync -az -e "ssh -i $SB_KEY -o UserKnownHostsFile=$SB_STATE/known_hosts -o LogLevel=ERROR" \
        --files-from="$files" "$SRC/" "ubuntu@$(sb_public_ip sbx-build):src-$TAG/"
    rm -f "$files"
    ;;
build)
    # One shared target dir, so a baseline and a candidate reuse each
    # other's dependency builds.
    sb_ssh sbx-build "cd src-$TAG && source ~/.cargo/env && export CARGO_TARGET_DIR=\$HOME/target &&
        cargo build --release --locked --features loadgen --bin legion &&
        cargo build -p legion-guest --profile dist --locked --target x86_64-unknown-linux-musl &&
        mkdir -p \$HOME/out-$TAG && cp \$HOME/target/release/legion \
          \$HOME/target/x86_64-unknown-linux-musl/dist/legion-guest \$HOME/out-$TAG/ &&
        { cargo build --release --locked --features loadgen --bin legion-loadgen &&
          cp \$HOME/target/release/legion-loadgen \$HOME/out-$TAG/ || echo 'loadgen did not build; skipped'; }"
    ;;
push)
    creds="$(aws sts get-session-token --duration-seconds 3600 --output json)"
    sb_ssh sbx-build "export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$creds") \
        AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$creds") \
        AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$creds") AWS_DEFAULT_REGION=$AWS_REGION &&
        aws s3 cp --quiet --recursive \$HOME/out-$TAG s3://$SB_BUCKET/$TAG/ && echo pushed $TAG"
    ;;
deploy)
    for name in $(sb_hosts | awk '$1 != "sbx-build" { print $1 }'); do
        cmd=""
        for bin in $bin_list legion-guest; do
            aws s3 ls "s3://$SB_BUCKET/$TAG/$bin" >/dev/null 2>&1 || continue
            url="$(aws s3 presign "s3://$SB_BUCKET/$TAG/$bin" --expires-in 3600)"
            cmd+="sudo curl -fsS -o /opt/legion/bin/$bin.new '$url' && sudo install -m 0755 /opt/legion/bin/$bin.new /opt/legion/bin/$bin && sudo rm /opt/legion/bin/$bin.new && "
        done
        sb_ssh "$name" "${cmd}sudo ln -sf /opt/legion/bin/legion /usr/local/bin/legion && { [ -x /opt/legion/bin/legion-loadgen ] && sudo ln -sf /opt/legion/bin/legion-loadgen /usr/local/bin/legion-loadgen || true; }" &
    done
    wait
    sb_log "Deployed $TAG"
    ;;
all) "$0" sync && "$0" build && "$0" push && "$0" deploy ;;
stop) aws ec2 stop-instances --instance-ids "$(bb_id)" >/dev/null && echo stopping ;;
start)
    aws ec2 start-instances --instance-ids "$(bb_id)" >/dev/null
    aws ec2 wait instance-running --instance-ids "$(bb_id)"
    until sb_ssh sbx-build true 2>/dev/null; do sleep 5; done
    echo started
    ;;
*) sed -n '2,13p' "$0"; exit 1 ;;
esac
