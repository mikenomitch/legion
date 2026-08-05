#!/usr/bin/env bash
# Bake the operator box's toolchain into an AMI, once, so every future
# `bootstrap-operator-box.sh create` skips it.
#
# The problem, measured on this rig: the operator box installs its whole
# toolchain in user-data, and `cloud-init analyze blame` put
# modules-final/config-scripts_user at **3245 seconds (54 minutes)** on
# one launch and ~14 minutes on another — same instance type, same
# region, same script. It is nearly all `apt-get`, whose throughput on a
# given day is not something the rig controls. That is setup tax paid
# before any Legion code compiles, and it is paid again on every box.
#
# So pay it once, into a disk image. This script:
#
#   1. launches a throwaway instance from Canonical's stock Ubuntu 24.04
#   2. runs scripts/operator-toolchain.sh on it — the SAME file the slow
#      path runs, which is the whole point of that file existing
#   3. waits for it to power itself off, which is the completion signal
#   4. snapshots it into a tagged AMI and waits for the image
#   5. terminates the throwaway instance, whatever happened
#
# Usage:
#   benchmarks/aws/scripts/bake-operator-ami.sh                 # bake if needed
#   benchmarks/aws/scripts/bake-operator-ami.sh --region us-west-2
#   benchmarks/aws/scripts/bake-operator-ami.sh --show          # what is baked
#   benchmarks/aws/scripts/bake-operator-ami.sh --force         # bake anyway
#
# Re-running is cheap and safe: an image whose toolchain fingerprint
# already matches scripts/operator-toolchain.sh is reported and left
# alone, and nothing is launched. `--force` bakes a second one; the
# newest always wins the lookup, and old ones are yours to deregister.
#
# AMIs are regional. Bake in the region you will run the campaign in.
#
# Re-bake when: operator-toolchain.sh changes (bootstrap refuses the
# stale image and says so), or the image is old enough that you would
# rather pay for a bake than have every box `apt-get upgrade` its way
# through months of security updates — call that a few months.
#
# Cost: one c7i.large for the length of the install (~$0.09/hr, so cents)
# plus ~$0.05/month for the ~5 GB snapshot the AMI holds.
#
# Requires locally: aws CLI with credentials. No jq, no ssh — the bake
# instance is never logged into, and gets no key pair on purpose (see
# run_bake_instance).
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./operator-ami-lib.sh
. "$SCRIPTS_DIR/operator-ami-lib.sh"

REGION="${AWS_REGION:-us-east-1}"
# The bake is network- and unpack-bound (apt, rustup, an AWS CLI zip),
# not CPU-bound, so it does not want the operator box's 8 vCPU. c7i
# rather than t3 because burstable credits could throttle the one thing
# that is CPU-heavy here, dpkg unpacking.
BAKE_TYPE="c7i.large"
# Small on purpose, and it MUST stay under bootstrap-operator-box.sh's
# DISK_GB (100): an AMI's snapshot is the floor for any volume launched
# from it, so a fat bake volume would silently raise the operator box's
# disk bill and could not be shrunk. cloud-init's growpart expands the
# root filesystem up to whatever the launch asks for.
BAKE_DISK_GB=20
# Generous, because the thing being escaped is a 54-minute apt run: this
# has to be longer than the worst observed slow path or the bake times
# out on exactly the day it was most needed.
BAKE_TIMEOUT=5400
FORCE=false
SUBNET_ID="${OPERATOR_SUBNET_ID:-}"

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }
die() {
    echo "error: $*" >&2
    exit 1
}

aws_() { aws --region "$REGION" "$@"; }

# The throwaway instance, recorded the moment RunInstances returns so
# the trap below can always find it.
TEMP_INSTANCE=""
cleanup() {
    [ -n "$TEMP_INSTANCE" ] || return 0
    # Deliberately unconditional — success and failure both end here.
    # The instance has no further use once the image is `available`, and
    # a bake that died halfway must not leave an instance billing for a
    # toolchain nobody will ever launch.
    log "terminating bake instance $TEMP_INSTANCE"
    aws_ ec2 terminate-instances --instance-ids "$TEMP_INSTANCE" >/dev/null 2>&1 ||
        log "  could not terminate $TEMP_INSTANCE — terminate it by hand, it is billing"
    TEMP_INSTANCE=""
}
trap cleanup EXIT

# --- the bake instance's user-data --------------------------------------

# The shared toolchain script, plus an epilogue that only ever runs
# during a bake. Nothing in the epilogue may install anything: the whole
# value of operator-toolchain.sh is that the baked image and the slow
# path install one identical list.
render_bake_user_data() {
    cat "$OPERATOR_TOOLCHAIN_SCRIPT"
    cat <<'BAKE'

# --- bake epilogue: runs only on the throwaway bake instance ------------

# What actually landed, written where a stopped instance can still be
# asked for it. bake-operator-ami.sh reads this off the EC2 console and
# tags the AMI with it, so an image can be traced to its toolchain
# without booting it. /dev/console rather than stdout because cloud-init
# tees stdout to a log file inside the image, which is precisely the
# place the bake script cannot read.
#
# Every capture is `|| =unknown`: `set -o pipefail` is on, and a version
# probe that trips over a broken pipe would otherwise abort a bake that
# had already done all of its work.
rust_v=$(sudo -u ubuntu /home/ubuntu/.cargo/bin/rustc --version 2>/dev/null | awk '{print $2}') || rust_v=unknown
tf_v=$(terraform version 2>/dev/null | awk 'NR==1{print $2}') || tf_v=unknown
aws_v=$(aws --version 2>&1 | awk '{print $1}' | cut -d/ -f2) || aws_v=unknown
protoc_v=$(protoc --version 2>/dev/null | awk '{print $2}') || protoc_v=unknown
os_v=$(lsb_release -rs 2>/dev/null) || os_v=unknown
echo "LEGION-BAKE-VERSIONS ubuntu=$os_v rust=$rust_v terraform=$tf_v awscli=$aws_v protoc=$protoc_v" >/dev/console

# Un-personalize the image before it is snapshotted.
#
#   ssh host keys — regenerated by cloud-init on first boot. Left in
#     place, every operator box ever launched from this AMI would share
#     one host identity, which is a real problem for a box reached with
#     StrictHostKeyChecking=accept-new.
#   cloud-init state — `clean` makes the next boot look like a first
#     boot, so the per-instance user-data (the rig's SSH key pair) is
#     actually run rather than skipped as already-done.
#   apt lists — 40-odd MB of index that is stale the moment it is baked.
cloud-init clean --logs
rm -f /etc/ssh/ssh_host_*
apt-get clean
rm -rf /var/lib/apt/lists/*

# The completion signal, and the only thing that stops this instance.
# `set -e` means any failure above leaves it running instead, which is
# exactly how bake-operator-ami.sh tells a finished bake from a failed
# one — it waits for the state, not for a log line it might not be able
# to read. The sleep gives the console write time to flush.
echo "LEGION-BAKE-OK" >/dev/console
sync
sleep 5
poweroff
BAKE
}

# --- launching it -------------------------------------------------------

# Older AZs do not carry newer instance families, so intersect the AZs
# that offer this type with the default subnets that exist. Same reason
# bootstrap-operator-box.sh does it: finding out at RunInstances costs a
# create-and-destroy cycle for a fact one API call away.
pick_subnet() { # pick_subnet -> "<subnet-id> <az>"
    local azs vpc subnets sn az
    azs=" $(aws_ ec2 describe-instance-type-offerings \
        --location-type availability-zone \
        --filters "Name=instance-type,Values=$BAKE_TYPE" \
        --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ' ') "
    [ "$azs" != "  " ] || die "$BAKE_TYPE is not offered in any AZ in $REGION (try --type)"

    vpc=$(aws_ ec2 describe-vpcs --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text)
    [ "$vpc" != "None" ] || die "no default VPC in $REGION.
  Create one (free, instant):     aws ec2 create-default-vpc --region $REGION
  Or name a public subnet:        OPERATOR_SUBNET_ID=subnet-... $0"

    subnets=$(aws_ ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$vpc" "Name=default-for-az,Values=true" \
        --query 'Subnets[].[SubnetId,AvailabilityZone]' --output text)
    while read -r sn az; do
        [ -n "$sn" ] || continue
        case "$azs" in *" $az "*)
            echo "$sn $az"
            return 0
            ;;
        esac
    done <<<"$subnets"
    die "no default subnet in $REGION sits in an AZ that offers $BAKE_TYPE.
  Offered in:$azs
  Use --type <other-type>, or OPERATOR_SUBNET_ID for a public subnet in one of those AZs."
}

run_bake_instance() { # run_bake_instance <stock-ami> <subnet> -> instance id
    local ami="$1" subnet="$2" instance_id err
    err=$(mktemp)

    # No key pair, and none is wanted: an AMI baked from an instance
    # that had one carries that key in /home/ubuntu/.ssh/authorized_keys
    # and hands it to every box launched from the image afterwards.
    # Nothing ever logs into this instance — it reports through its power
    # state and the console — so there is nothing to give up.
    #
    # No security group either: the VPC's default group allows all
    # egress, which is all apt and rustup need, and creating one would
    # be another resource to leak on a failed bake.
    #
    # A public IP IS needed (apt, rustup, awscli over the internet). A
    # default subnet assigns one anyway; asking explicitly covers a
    # subnet passed via OPERATOR_SUBNET_ID with MapPublicIpOnLaunch off.
    # If that subnet has no route to an internet gateway the bake simply
    # hangs until --timeout, and the console output says where.
    instance_id=$(aws_ ec2 run-instances \
        --image-id "$ami" \
        --instance-type "$BAKE_TYPE" \
        --subnet-id "$subnet" \
        --associate-public-ip-address \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$BAKE_DISK_GB,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
        --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
        --instance-initiated-shutdown-behavior stop \
        --user-data "$(render_bake_user_data)" \
        --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=legion-bench-operator-ami-bake},{Key=Project,Value=legion-bench},{Key=Purpose,Value=benchmark-operator-ami-bake}]" \
        --query 'Instances[0].InstanceId' --output text 2>"$err") || {
        local msg
        msg=$(cat "$err")
        rm -f "$err"
        case "$msg" in
        *VcpuLimitExceeded* | *InstanceLimitExceeded*)
            die "$BAKE_TYPE exceeds this account's on-demand vCPU quota in $REGION:
  $msg
  A campaign already running eats that quota — bake before bringup, or use
  --type t3.medium (slower bake, same image)."
            ;;
        *)
            die "RunInstances failed: $msg"
            ;;
        esac
    }
    rm -f "$err"
    echo "$instance_id"
}

instance_state() { # instance_state <id>
    aws_ ec2 describe-instances --instance-ids "$1" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo unknown
}

# The instance stopping itself is the completion signal — see the bake
# epilogue. `aws ec2 wait instance-stopped` is not usable here: its
# built-in waiter gives up after 40 attempts (10 minutes), and the whole
# subject of this script is an install that has been measured at 54.
wait_for_stop() { # wait_for_stop <id>
    local id="$1" start now state last_log
    start=$(date +%s)
    last_log=$start
    log "installing the toolchain on $id — this is the slow part, 15-55 min"
    while :; do
        state=$(instance_state "$id")
        now=$(date +%s)
        case "$state" in
        stopped) return 0 ;;
        running | pending | stopping) ;;
        *)
            die "bake instance went to '$state' — it was expected to stop itself.
  Console output:
$(console_output "$id" | tail -30)"
            ;;
        esac
        if ((now - start > BAKE_TIMEOUT)); then
            die "bake did not finish within ${BAKE_TIMEOUT}s (state: $state).
  The install failed, or the instance has no route to the internet.
  Console output:
$(console_output "$id" | tail -40)
  Raise the budget with --timeout <seconds> if the box was simply slow."
        fi
        if ((now - last_log >= 120)); then
            log "  still installing ($((now - start))s elapsed, state $state)"
            last_log=$now
        fi
        sleep 15
    done
}

console_output() { # console_output <id>
    aws_ ec2 get-console-output --instance-id "$1" \
        --query 'Output' --output text 2>/dev/null || true
}

# --- baking -------------------------------------------------------------

cmd_show() {
    local hash current any
    hash=$(operator_toolchain_hash)
    current=$(operator_find_baked_ami "$REGION" "$hash")
    any=$(operator_find_baked_ami "$REGION")

    echo "region:             $REGION"
    echo "toolchain script:   $OPERATOR_TOOLCHAIN_SCRIPT"
    echo "toolchain hash:     $hash"
    if [ -z "$any" ]; then
        echo "baked AMI:          none — every bootstrap pays the 15-55 min install"
        echo
        echo "Bake one:  $0 --region $REGION"
        return 0
    fi
    aws_ ec2 describe-images --image-ids "$any" \
        --query 'Images[0].{ami:ImageId,name:Name,created:CreationDate,tags:Tags}' \
        --output table
    if [ -n "$current" ]; then
        echo "status:             current — bootstrap will use $current"
    else
        echo "status:             STALE. $any was baked from a different"
        echo "                    operator-toolchain.sh, so bootstrap will ignore"
        echo "                    it and install in user-data instead."
        echo
        echo "Re-bake:  $0 --region $REGION"
    fi
}

cmd_bake() {
    command -v aws >/dev/null || die "the aws CLI is required"
    [ -f "$OPERATOR_TOOLCHAIN_SCRIPT" ] || die "missing $OPERATOR_TOOLCHAIN_SCRIPT"
    aws_ sts get-caller-identity >/dev/null || die "no usable AWS credentials"

    local hash existing
    hash=$(operator_toolchain_hash)
    existing=$(operator_find_baked_ami "$REGION" "$hash")
    if [ -n "$existing" ] && [ "$FORCE" != true ]; then
        log "$REGION already has $existing for toolchain $hash — nothing to do"
        log "bootstrap-operator-box.sh will launch from it. Re-bake anyway: $0 --force"
        return 0
    fi

    # Canonical's Ubuntu 24.04 amd64 pointer — the same source
    # bootstrap-operator-box.sh and terraform/instances.tf use, so the
    # baked image and the fallback path start from the same distro.
    local stock
    stock=$(aws_ ssm get-parameters \
        --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query 'Parameters[0].Value' --output text)
    [ -n "$stock" ] && [ "$stock" != None ] ||
        die "could not resolve the stock Ubuntu 24.04 AMI in $REGION"

    # `die` inside the process substitution below kills only that
    # subshell, so an empty result is how a failed pick_subnet arrives
    # here. Without this guard it would reach RunInstances as an empty
    # --subnet-id and fail somewhere less obvious.
    local subnet="" az=""
    if [ -n "$SUBNET_ID" ]; then
        subnet="$SUBNET_ID"
        az="(from OPERATOR_SUBNET_ID)"
    else
        read -r subnet az < <(pick_subnet) || true
        [ -n "$subnet" ] || die "could not pick a subnet in $REGION (see above)"
    fi
    log "baking from $stock on a $BAKE_TYPE in $subnet $az"

    TEMP_INSTANCE=$(run_bake_instance "$stock" "$subnet")
    log "bake instance $TEMP_INSTANCE launched (terminated on exit, however this ends)"
    wait_for_stop "$TEMP_INSTANCE"

    # Soft confirmation only. The power state above is the real signal;
    # console retention for a stopped instance is a property of the
    # hypervisor generation, so a missing sentinel is worth a warning
    # and not worth throwing away a good image over.
    local console versions
    console=$(console_output "$TEMP_INSTANCE")
    case "$console" in
    *LEGION-BAKE-OK*) log "toolchain install reported OK" ;;
    *) log "warning: no LEGION-BAKE-OK on the console (it may not be retained for a stopped instance)" ;;
    esac
    # Squeezed to one word: the value goes into --tag-specifications
    # shorthand, whose parser is comma- and space-sensitive, so
    # `rust=1.85.0;terraform=1.9.8` is a tag and `rust=1.85.0 terraform…`
    # is a parse error at the end of a 20-minute bake.
    versions=$(echo "$console" | sed -n 's/.*LEGION-BAKE-VERSIONS //p' | tail -1 |
        tr -d '\r' | tr -s ' ' ';' | tr -cd 'A-Za-z0-9=.;_-')
    [ -n "$versions" ] || versions="unknown"
    log "toolchain: $versions"

    # AMI names must be unique in the account+region and allow a narrow
    # character set, so the name carries the fingerprint and a timestamp
    # while the *tag* stays the stable handle the lookup searches on.
    local stamp name image_id
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    name="legion-bench-operator-$hash-$stamp"
    log "creating image $name"
    image_id=$(aws_ ec2 create-image \
        --instance-id "$TEMP_INSTANCE" \
        --name "$name" \
        --description "Legion benchmark operator toolchain, baked $stamp from $stock" \
        --tag-specifications \
        "ResourceType=image,Tags=[{Key=Name,Value=$OPERATOR_AMI_NAME_TAG},{Key=Project,Value=legion-bench},{Key=Purpose,Value=benchmark-operator-ami},{Key=$OPERATOR_AMI_TOOLCHAIN_TAG,Value=$hash},{Key=LegionToolchainVersions,Value=$versions},{Key=BakedAt,Value=$stamp},{Key=SourceAmi,Value=$stock}]" \
        "ResourceType=snapshot,Tags=[{Key=Name,Value=$OPERATOR_AMI_NAME_TAG},{Key=Project,Value=legion-bench}]" \
        --query 'ImageId' --output text)
    log "image $image_id registering — waiting for it to become available"

    # The instance stays until the image reports `available`, which is
    # AWS's own guidance for create-image: the snapshot is taken from
    # the instance's root volume, and there is no reason to race it. The
    # trap does the terminating, after this returns.
    wait_for_image "$image_id"

    cat >&2 <<EOF

Baked: $image_id  (toolchain $hash, $versions)

bootstrap-operator-box.sh now finds it by tag and skips the install:

  benchmarks/aws/scripts/bootstrap-operator-box.sh launch --profile <name>

Re-bake when scripts/operator-toolchain.sh changes — bootstrap compares
the fingerprint and falls back to the slow path rather than launching a
box that is missing a tool you just added. Older images are left alone;
the newest matching one wins. To clear them out:

  aws ec2 deregister-image --region $REGION --image-id <old-ami>
EOF
}

# `aws ec2 wait image-available` has the same 10-minute ceiling as the
# instance waiter, and registration from a 20 GB volume can outrun it.
wait_for_image() { # wait_for_image <ami>
    local ami="$1" start now state last_log
    start=$(date +%s)
    last_log=$start
    while :; do
        state=$(aws_ ec2 describe-images --image-ids "$ami" \
            --query 'Images[0].State' --output text 2>/dev/null || echo pending)
        now=$(date +%s)
        case "$state" in
        available) return 0 ;;
        pending) ;;
        *)
            die "image $ami went to '$state'.
  Anything but 'available' is invisible to bootstrap-operator-box.sh, so it
  would use the slow path. Deregister this one and retry:
    aws ec2 deregister-image --region $REGION --image-id $ami"
            ;;
        esac
        if ((now - start > BAKE_TIMEOUT)); then
            die "image $ami still $state after ${BAKE_TIMEOUT}s.
  Deregister it if it never lands:
    aws ec2 deregister-image --region $REGION --image-id $ami"
        fi
        if ((now - last_log >= 60)); then
            log "  image $state ($((now - start))s)"
            last_log=$now
        fi
        sleep 15
    done
}

# --- dispatch -----------------------------------------------------------

CMD=bake
while [ $# -gt 0 ]; do
    case "$1" in
    --show)
        CMD=show
        shift
        ;;
    --force)
        FORCE=true
        shift
        ;;
    --region)
        REGION="$2"
        shift 2
        ;;
    --type)
        BAKE_TYPE="$2"
        shift 2
        ;;
    --disk-gb)
        BAKE_DISK_GB="$2"
        shift 2
        ;;
    --timeout)
        BAKE_TIMEOUT="$2"
        shift 2
        ;;
    --subnet-id)
        SUBNET_ID="$2"
        shift 2
        ;;
    # Credentials for this script's AWS calls, same spellings as
    # bootstrap-operator-box.sh so the two are muscle-memory compatible.
    --access-key-id)
        export AWS_ACCESS_KEY_ID="$2"
        shift 2
        ;;
    --secret-access-key)
        export AWS_SECRET_ACCESS_KEY="$2"
        shift 2
        ;;
    --session-token)
        export AWS_SESSION_TOKEN="$2"
        shift 2
        ;;
    --profile)
        export AWS_PROFILE="$2"
        shift 2
        ;;
    -h | --help)
        sed -n '/^# Usage:/,/^# Requires/p' "$0" | sed 's/^# \{0,1\}//' >&2
        exit 0
        ;;
    *) die "unknown argument: $1" ;;
    esac
done

case "$CMD" in
show) cmd_show ;;
bake) cmd_bake ;;
esac
