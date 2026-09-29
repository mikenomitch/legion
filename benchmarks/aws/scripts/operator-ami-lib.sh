#!/usr/bin/env bash
# The contract between the two halves of the baked-AMI path: what
# bake-operator-ami.sh publishes, and what bootstrap-operator-box.sh
# looks for. Sourced by both, never run on its own.
#
# It is three constants and two functions, and it exists because they
# are exactly the things that break silently when they disagree — a
# bootstrap searching for a tag the bake script no longer writes finds
# nothing, says "no baked AMI", and spends 54 minutes in user-data while
# a perfectly good image sits in the account.
#
# Everything here is read-only against AWS.

# The AMI *name* has to be unique per bake (AWS rejects a duplicate), so
# the stable handle is a tag instead. Both sides use this one.
OPERATOR_AMI_NAME_TAG="${OPERATOR_AMI_NAME_TAG:-legion-bench-operator-ami}"

# Fingerprint of operator-toolchain.sh at bake time. This is the drift
# guard: the toolchain list lives in one file, and this tag records
# which version of that file is actually inside the image, so a
# bootstrap can tell "the AMI has the toolchain" from "the AMI has AN
# older toolchain".
OPERATOR_AMI_TOOLCHAIN_TAG="LegionToolchain"

OPERATOR_SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_TOOLCHAIN_SCRIPT="$OPERATOR_SCRIPTS_DIR/operator-toolchain.sh"

# Short content hash of the toolchain script. Truncated to 12 hex chars
# because it is a cache key printed in log lines and AMI names, not a
# security boundary — and it goes in an AMI name, which allows a narrow
# character set.
#
# sha256sum is coreutils (the operator box, CI); shasum is what macOS
# ships. bootstrap-operator-box.sh runs on a laptop, so both matter.
operator_toolchain_hash() {
    local sum
    if command -v sha256sum >/dev/null 2>&1; then
        sum=$(sha256sum "$OPERATOR_TOOLCHAIN_SCRIPT")
    else
        sum=$(shasum -a 256 "$OPERATOR_TOOLCHAIN_SCRIPT")
    fi
    printf '%.12s\n' "${sum%% *}"
}

# operator_find_baked_ami <region> [toolchain-hash] -> ami-id, or nothing
#
# Newest first, and only images this account owns: `--owners self` keeps
# a public image that happens to carry the same tag from ever being
# launched with the rig's IAM role attached.
#
# `state=available` is what makes a failed or still-pending bake
# invisible here, which is why bake-operator-ami.sh can leave a broken
# image behind without poisoning the next bootstrap.
operator_find_baked_ami() {
    local region="$1" want="${2:-}" out
    local -a filters
    filters=(
        "Name=tag:Name,Values=$OPERATOR_AMI_NAME_TAG"
        "Name=state,Values=available"
        # The stock-AMI path resolves Canonical's amd64 pointer, so the
        # whole operator box is x86_64 either way.
        "Name=architecture,Values=x86_64"
    )
    if [ -n "$want" ]; then
        filters+=("Name=tag:$OPERATOR_AMI_TOOLCHAIN_TAG,Values=$want")
    fi

    out=$(aws --region "$region" ec2 describe-images --owners self \
        --filters "${filters[@]}" \
        --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
        --output text 2>/dev/null) || out=""
    # `--output text` prints the string None for a null JMESPath result.
    case "$out" in
    None | "") return 0 ;;
    esac
    echo "$out"
}
