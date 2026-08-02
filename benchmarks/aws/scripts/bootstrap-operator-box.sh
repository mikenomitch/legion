#!/usr/bin/env bash
# Stand up the box you drive a campaign FROM — step 0 of the runbook in
# ../README.md, before terraform has run and before anything else here
# works.
#
# Why a dedicated box at all, when the rig's own loadgen box exists:
#
#   - The rig has to be built somewhere. `build-and-upload.sh` compiles
#     the dist-profile binary and the plugin dylibs for the campaign's
#     instances, which are Linux x86_64. A macOS/arm64 laptop cannot
#     produce that artifact, and the loadgen box does not exist yet.
#   - `admin_cidr` (terraform/security.tf) pins every ingress rule to one
#     CIDR. A laptop's egress IP rotates; this box gets an Elastic IP, so
#     the CIDR you write into terraform.tfvars stays true for the life of
#     the campaign.
#   - Bringup is terraform, and terraform state is local by default
#     (terraform/main.tf). Keeping state on a box that outlives your
#     laptop's sleep cycle is the difference between `teardown.sh` and a
#     manual console hunt for orphaned resources.
#
# It is deliberately NOT part of the rig's terraform. It must exist
# before that terraform runs and must survive `teardown.sh` — which
# destroys everything terraform manages, state included. So this uses
# plain AWS CLI calls and its own tiny state file.
#
# Usage:
#   # One command: build the box, configure the rig, run phase 1 remotely.
#   bootstrap-operator-box.sh launch --access-key-id AKIA... --secret-access-key ...
#   bootstrap-operator-box.sh launch --profile my-aws-profile
#
#   bootstrap-operator-box.sh logs -f         # tail the running phase
#   bootstrap-operator-box.sh status          # box + phase state
#   bootstrap-operator-box.sh ssh [cmd...]    # shell in (or run one command)
#   bootstrap-operator-box.sh stop            # ~$0.37/hr -> ~$0.016/hr
#   bootstrap-operator-box.sh start           # back up, same Elastic IP
#   bootstrap-operator-box.sh destroy         # this box only (not the rig)
#
# Stop it between sessions. Idle, it is ~$268/mo running and ~$12/mo
# stopped, and the Elastic IP means the address — hence admin_cidr in the
# rig's terraform.tfvars — survives the stop.
#
#   # The pieces `launch` chains, if you want to drive them yourself:
#   bootstrap-operator-box.sh create [--region us-east-1] [--type c7i.2xlarge]
#   bootstrap-operator-box.sh push            # rsync this repo onto the box
#   bootstrap-operator-box.sh configure       # write terraform.tfvars on the box
#   bootstrap-operator-box.sh run [phase1 args…]
#
# Credentials are used HERE, to build the box. They are never copied onto
# it: the box carries an IAM instance role instead, so no long-lived key
# ever sits on an internet-facing instance that the campaign then leaves
# running overnight. Any of these work, in this order:
#   --access-key-id/--secret-access-key [--session-token], --profile,
#   or whatever your environment already has.
#
# Requires locally: aws CLI with credentials, jq, ssh, rsync.
set -euo pipefail

AWS_BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$AWS_BENCH_ROOT/../.." && pwd)"

# Deliberately not sourcing env.sh: that file's first act is to resolve
# rig topology from terraform state or the on-rig manifest, and neither
# exists yet at step 0.
STATE_FILE="${OPERATOR_STATE_FILE:-$AWS_BENCH_ROOT/.operator-box.json}"

NAME="${OPERATOR_NAME:-legion-bench-operator}"
REGION="${AWS_REGION:-us-east-1}"
# 8 vCPU / 16 GiB. Sized for the one job that actually needs it: a
# `cargo build --profile dist` with LTO, which is slow enough on 2 cores
# to dominate the setup. ~$0.36/hr on-demand, and `stop` between sessions
# costs only the EBS volume.
INSTANCE_TYPE="c7i.2xlarge"
DISK_GB=100
ADMIN_CIDR=""     # who may SSH to THIS box; default is your current IP /32
KEY_PATH="$HOME/.ssh/${NAME}"
ARCHIVE_BUCKET=""       # "" = create one; "none" = skip the durable mirror
# A plain string, not an array: macOS still ships bash 3.2, where
# expanding an empty array under `set -u` is an error.
PHASE1_ARGS=""
REMOTE_SESSION=phase1
REMOTE_LOG=/home/ubuntu/phase1.log

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }
die() {
    echo "error: $*" >&2
    exit 1
}

aws_() { aws --region "$REGION" "$@"; }

state_get() { # state_get <jq-path>
    [ -f "$STATE_FILE" ] || die "no operator box recorded at $STATE_FILE — run: $0 create"
    jq -r "$1 // empty" "$STATE_FILE"
}

# `create` builds six billable things across six API calls. The state
# file is written BEFORE the first one and appended after each, so a
# failure anywhere in that window still leaves `destroy` able to clean
# up. Recording only at the end — the obvious way — means a mid-way
# failure orphans a security group, an IAM role, and a billing Elastic
# IP with nothing left that knows they exist.
state_init() {
    jq -n --arg name "$NAME" --arg region "$REGION" --arg key "$KEY_PATH" \
        --arg admin_cidr "$ADMIN_CIDR" \
        '{name:$name, region:$region, key_path:$key, admin_cidr:$admin_cidr}' \
        >"$STATE_FILE"
}

state_put() { # state_put <field> <value>
    local tmp="$STATE_FILE.tmp"
    jq --arg k "$1" --arg v "$2" '.[$k] = $v' "$STATE_FILE" >"$tmp"
    mv "$tmp" "$STATE_FILE"
}

# Printed on any non-clean exit between the first resource and the last.
CREATE_IN_PROGRESS=false
create_failed_hint() {
    [ "$CREATE_IN_PROGRESS" = true ] || return 0
    cat >&2 <<EOF

create failed partway. Resources it got as far as making are recorded in
$STATE_FILE — clean them up before retrying, or they bill quietly:

  $0 destroy
EOF
}
trap create_failed_hint EXIT

# --- launch: the whole thing, one command --------------------------------

cmd_launch() {
    # Re-runnable: a launch that died after the box was built should not
    # have to build another one. phase1.sh's checkpoints make the same
    # promise for the steps after this.
    if [ -f "$STATE_FILE" ] && [ -n "$(jq -r '.instance_id // empty' "$STATE_FILE")" ]; then
        REGION=$(jq -r .region "$STATE_FILE")
        log "reusing the operator box already recorded in $STATE_FILE"
        cmd_push
    else
        cmd_create
    fi
    cmd_configure
    cmd_run
    local ip
    ip=$(state_get .public_ip)
    cat >&2 <<EOF

Phase 1 is running on $ip, under tmux, independent of this terminal.
It is hours long: a debug build and smoke test, then terraform, then a
dist build with LTO, then the install and the campaign.

  $0 logs -f      tail it
  $0 status       where it is now
  $0 ssh          get on the box

Once the rig's bucket exists (after bringup), the campaign publishes to
S3 too, readable from anywhere with credentials and no SSH:

  benchmarks/aws/scripts/campaign-status.sh watch --bucket \$(...)

When you are done — two separate teardowns, on purpose:
  $0 ssh 'cd ~/legion/benchmarks/aws && scripts/teardown.sh'   # the rig
  $0 destroy                                                   # this box
EOF
}

# --- create -------------------------------------------------------------

# A key just for this box. Never the rig key: the rig's key pair is
# generated ON the box at first boot (see render_user_data), so the
# private half of the rig's trust domain never touches your laptop.
#
# Existence is not validity. A plain `[ -f ]` test would accept a
# zero-byte or truncated file, skip generation, fail to import, and then
# surface sixty seconds later as a RunInstances error blaming IAM
# propagation. Check the key actually parses.
ensure_local_key() {
    if [ -e "$KEY_PATH" ] || [ -e "${KEY_PATH}.pub" ]; then
        [ -s "$KEY_PATH" ] && [ -s "${KEY_PATH}.pub" ] &&
            ssh-keygen -l -f "${KEY_PATH}.pub" >/dev/null 2>&1 ||
            die "$KEY_PATH exists but is not a usable key pair.
  Remove both halves and re-run:  rm -f $KEY_PATH ${KEY_PATH}.pub"
        log "reusing operator key at $KEY_PATH"
        return 0
    fi
    log "generating operator key at $KEY_PATH"
    ssh-keygen -t ed25519 -N "" -f "$KEY_PATH" -C "$NAME" >/dev/null
}

# Tolerate exactly one failure — the key pair already exists, which is
# what a re-run hits — and treat every other failure as fatal. A blanket
# `|| true` here also swallows malformed key material, and the only
# symptom is RunInstances failing later for a reason that names neither
# the key nor this call.
import_key_pair() {
    local err
    if err=$(aws_ ec2 import-key-pair --key-name "$NAME" \
        --public-key-material "fileb://${KEY_PATH}.pub" 2>&1 >/dev/null); then
        state_put key_pair_created true
        return 0
    fi
    case "$err" in
    *InvalidKeyPair.Duplicate*)
        # Deliberately not re-imported: another instance may be using it.
        # If it was imported from a different key, SSH fails later with a
        # clear permission error, which is the better failure to have.
        log "key pair $NAME already exists in $REGION — reusing it"
        ;;
    *)
        die "import-key-pair failed: $err"
        ;;
    esac
}

# Older AZs do not carry newer instance families: us-east-1e has no
# c7i at all, and every region has some equivalent. Taking the first
# default subnet and finding out at RunInstances costs a full
# create-and-destroy cycle for a fact that is one API call away, so
# intersect the AZs that offer this type with the subnets available.
instance_type_azs() {
    aws_ ec2 describe-instance-type-offerings \
        --location-type availability-zone \
        --filters "Name=instance-type,Values=$INSTANCE_TYPE" \
        --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ' '
}

az_offers() { # az_offers <az> <space-separated az list>
    case " $2 " in *" $1 "*) return 0 ;; esac
    return 1
}

pick_subnet() { # pick_subnet <vpc-id> <az-list> -> "<subnet-id> <az>"
    local vpc="$1" azs="$2" subnets sn az
    subnets=$(aws_ ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$vpc" "Name=default-for-az,Values=true" \
        --query 'Subnets[].[SubnetId,AvailabilityZone]' --output text)
    while read -r sn az; do
        [ -n "$sn" ] || continue
        if az_offers "$az" "$azs"; then
            echo "$sn $az"
            return 0
        fi
    done <<<"$subnets"
    die "no default subnet in $REGION sits in an AZ that offers $INSTANCE_TYPE.
  Offered in: $azs
  Use --type <other-type>, or OPERATOR_SUBNET_ID for a public subnet in one of those AZs."
}

cmd_create() {
    for tool in aws jq ssh rsync; do
        command -v "$tool" >/dev/null || die "$tool is required locally"
    done
    aws_ sts get-caller-identity >/dev/null || die "no usable AWS credentials"

    if [ -f "$STATE_FILE" ]; then
        local existing
        existing=$(jq -r '.instance_id // empty' "$STATE_FILE")
        if [ -n "$existing" ]; then
            die "an operator box is already recorded ($existing). Use '$0 status', or '$0 destroy' first."
        fi
        die "$STATE_FILE records a create that failed before the instance existed.
  Clean up what it did make, then retry:  $0 destroy"
    fi

    if [ -z "$ADMIN_CIDR" ]; then
        ADMIN_CIDR="$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '\n')/32"
        log "SSH ingress locked to your current address: $ADMIN_CIDR"
    fi

    ensure_local_key

    # Plenty of accounts have no default VPC — newer ones, and any where
    # it was deleted. OPERATOR_SUBNET_ID is the escape hatch, so it has to
    # be read BEFORE the default-VPC check rather than after it, and the
    # VPC derived from the subnet. The subnet must be public: the box is
    # reached over SSH from admin_cidr, and an Elastic IP does not help
    # without a route to an internet gateway.
    local vpc_id subnet_id azs subnet_az
    azs=$(instance_type_azs)
    [ -n "$azs" ] || die "$INSTANCE_TYPE is not offered in any AZ in $REGION"

    if [ -n "${OPERATOR_SUBNET_ID:-}" ]; then
        subnet_id="$OPERATOR_SUBNET_ID"
        read -r vpc_id subnet_az < <(aws_ ec2 describe-subnets --subnet-ids "$subnet_id" \
            --query 'Subnets[0].[VpcId,AvailabilityZone]' --output text 2>/dev/null || true)
        [ -n "${vpc_id:-}" ] && [ "$vpc_id" != None ] ||
            die "subnet $subnet_id not found in $REGION"
        az_offers "$subnet_az" "$azs" ||
            die "subnet $subnet_id is in $subnet_az, which does not offer $INSTANCE_TYPE.
  Offered in: $azs
  Pick a subnet in one of those AZs, or use --type."
        log "using subnet $subnet_id ($subnet_az, VPC $vpc_id) from OPERATOR_SUBNET_ID"
    else
        vpc_id=$(aws_ ec2 describe-vpcs --filters Name=isDefault,Values=true \
            --query 'Vpcs[0].VpcId' --output text)
        [ "$vpc_id" != "None" ] || die "no default VPC in $REGION.
  Create one (free, instant):     aws ec2 create-default-vpc --region $REGION
  Or use an existing public subnet: OPERATOR_SUBNET_ID=subnet-... $0 $CMD"
        read -r subnet_id subnet_az < <(pick_subnet "$vpc_id" "$azs")
        log "using default VPC $vpc_id / subnet $subnet_id ($subnet_az)"
    fi

    log "creating key pair, security group, and IAM role"
    state_init
    CREATE_IN_PROGRESS=true
    import_key_pair

    local sg_id
    sg_id=$(aws_ ec2 create-security-group --group-name "$NAME" \
        --description "Legion benchmark operator box" --vpc-id "$vpc_id" \
        --query 'GroupId' --output text)
    state_put security_group_id "$sg_id"
    aws_ ec2 authorize-security-group-ingress --group-id "$sg_id" \
        --protocol tcp --port 22 --cidr "$ADMIN_CIDR" >/dev/null

    create_iam_role
    local eip_alloc eip_ip
    read -r eip_alloc eip_ip < <(aws_ ec2 allocate-address --domain vpc \
        --query '[AllocationId,PublicIp]' --output text)
    state_put eip_allocation_id "$eip_alloc"
    state_put public_ip "$eip_ip"
    log "elastic IP $eip_ip allocated — this is the rig's admin_cidr"

    # Canonical's Ubuntu 24.04 amd64 pointer, same source terraform uses
    # (terraform/instances.tf) so the operator box and the rig run the
    # same distro. Resolved at run time, never a hardcoded AMI id.
    local ami
    ami=$(aws_ ssm get-parameters \
        --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query 'Parameters[0].Value' --output text)

    log "launching $INSTANCE_TYPE from $ami"
    local instance_id user_data
    user_data=$(render_user_data)
    # The instance profile was created seconds ago and IAM is eventually
    # consistent, so RunInstances can fail with "Invalid IAM Instance
    # Profile name" on a race that resolves itself. Retry THAT and only
    # that. Retrying every failure — the obvious loop — spends 80 seconds
    # hiding the real error behind a message naming the one cause it is
    # not, which is worse than failing immediately.
    local attempt err errfile
    errfile=$(mktemp)
    for attempt in $(seq 1 12); do
        instance_id=$(aws_ ec2 run-instances \
            --image-id "$ami" \
            --instance-type "$INSTANCE_TYPE" \
            --subnet-id "$subnet_id" \
            --security-group-ids "$sg_id" \
            --key-name "$NAME" \
            --iam-instance-profile "Name=$NAME" \
            --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$DISK_GB,\"VolumeType\":\"gp3\"}}]" \
            --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
            --user-data "$user_data" \
            --tag-specifications \
            "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=Project,Value=legion-bench},{Key=Purpose,Value=benchmark-operator}]" \
            --query 'Instances[0].InstanceId' --output text 2>"$errfile") && break
        err=$(cat "$errfile")
        case "$err" in
        *"Invalid IAM Instance Profile"* | *iamInstanceProfile*)
            log "  waiting for the IAM instance profile to propagate, retry $attempt/12"
            sleep 5
            ;;
        *VcpuLimitExceeded* | *InstanceLimitExceeded*)
            rm -f "$errfile"
            die "$INSTANCE_TYPE exceeds this account's on-demand vCPU quota in $REGION:
  $err
  Raise 'Running On-Demand Standard instances' (quota L-1216C47A), or use a
  smaller box: $0 $CMD --type c7i.large   (cheaper, much slower dist build)"
            ;;
        *)
            rm -f "$errfile"
            die "RunInstances failed: $err"
            ;;
        esac
    done
    rm -f "$errfile"
    [ -n "${instance_id:-}" ] || die "RunInstances kept failing after IAM-propagation retries: ${err:-unknown}"
    state_put instance_id "$instance_id"

    aws_ ec2 wait instance-running --instance-ids "$instance_id"
    aws_ ec2 associate-address --instance-id "$instance_id" \
        --allocation-id "$eip_alloc" >/dev/null
    log "instance $instance_id running at $eip_ip"
    # Every billable resource is now recorded, so a later failure is a
    # `destroy` away rather than a console hunt.
    CREATE_IN_PROGRESS=false

    wait_for_ssh "$eip_ip"
    log "waiting for setup to finish (rust, terraform, aws cli — a few minutes)"
    ssh_ "cloud-init status --wait >/dev/null 2>&1 || true"
    ssh_ "test -f /home/ubuntu/.setup-complete" ||
        die "setup did not complete; check: $0 ssh 'sudo tail -50 /var/log/cloud-init-output.log'"

    # `.setup-complete` proves cloud-init ran. It does NOT prove the tools
    # are reachable the way the campaign scripts reach them — over a
    # non-interactive ssh, which skips ~/.bashrc and so misses anything
    # rustup put there. Check the way the caller will call.
    local missing
    missing=$(ssh_ 'for t in cargo rustc clang terraform aws tmux python3 protoc git rsync; do
                        command -v $t >/dev/null 2>&1 || echo -n " $t"
                    done')
    [ -z "$missing" ] ||
        die "the box came up but these are not on a non-interactive PATH:$missing
  Check: $0 ssh 'sudo tail -50 /var/log/cloud-init-output.log'"

    log "box is ready"
    cmd_push

    # `launch` continues into configure + run and prints its own summary;
    # a bare `create` stops here, so hand back the manual runbook.
    [ "$CMD" = create ] || return 0
    echo
    cmd_tfvars
    cat >&2 <<EOF

Next, from the box ($0 ssh):

  cd ~/legion/benchmarks/aws
  scripts/phase1.sh          # steps 1-6, checkpointed — or run them by hand:

  scripts/local-smoke.sh                              # proves the loop, no AWS cost
  vi terraform/terraform.tfvars                       # paste the two lines above
  scripts/bringup.sh && scripts/build-and-upload.sh
  scripts/install-legion-a.sh
  experiments/packing-density.sh
  scripts/run-campaign.sh --system legion-a --smoke

This box is not managed by the rig's terraform, so scripts/teardown.sh
leaves it alone. Destroy it separately when the campaign is over:
  $0 destroy
EOF
}

# Terraform here builds a VPC, EC2 instances, an ASG, an S3 bucket, and
# IAM roles for the rig's own boxes. PowerUserAccess covers all of that
# except IAM, which is deliberately added back narrowed to the rig's
# name prefix — a benchmark box has no business minting arbitrary roles.
#
# Idempotent on every call: `destroy`'s IAM deletes are best-effort, so a
# retry after a failed create routinely finds some of these already
# there. EntityAlreadyExists is the expected case, not an error — but it
# is the ONLY tolerated one, so a genuine permissions failure here still
# stops the run instead of surfacing later as an unexplained
# RunInstances error.
iam_ok() { # iam_ok <aws iam args...>
    local err
    err=$(aws iam "$@" 2>&1 >/dev/null) && return 0
    case "$err" in
    *EntityAlreadyExists* | *LimitExceeded.DuplicateName*) return 0 ;;
    *) die "iam $1 failed: $err" ;;
    esac
}

create_iam_role() {
    state_put iam_role_created true
    iam_ok create-role --role-name "$NAME" \
        --assume-role-policy-document '{
          "Version":"2012-10-17",
          "Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]
        }'
    iam_ok attach-role-policy --role-name "$NAME" \
        --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
    iam_ok put-role-policy --role-name "$NAME" --policy-name rig-iam \
        --policy-document '{
          "Version":"2012-10-17",
          "Statement":[
            {
              "Sid":"ManageRigRoles",
              "Effect":"Allow",
              "Action":["iam:CreateRole","iam:DeleteRole","iam:GetRole","iam:TagRole",
                        "iam:PutRolePolicy","iam:DeleteRolePolicy","iam:GetRolePolicy",
                        "iam:ListRolePolicies","iam:ListAttachedRolePolicies",
                        "iam:AttachRolePolicy","iam:DetachRolePolicy",
                        "iam:CreateInstanceProfile","iam:DeleteInstanceProfile",
                        "iam:GetInstanceProfile","iam:AddRoleToInstanceProfile",
                        "iam:RemoveRoleFromInstanceProfile","iam:PassRole"],
              "Resource":["arn:aws:iam::*:role/legion-bench*",
                          "arn:aws:iam::*:instance-profile/legion-bench*"]
            },
            {
              "Sid":"SpotAndAsgServiceLinkedRoles",
              "Effect":"Allow",
              "Action":"iam:CreateServiceLinkedRole",
              "Resource":"*",
              "Condition":{"StringEquals":{"iam:AWSServiceName":[
                "autoscaling.amazonaws.com","spot.amazonaws.com","spotfleet.amazonaws.com"]}}
            }
          ]
        }'
    iam_ok create-instance-profile --instance-profile-name "$NAME"
    iam_ok add-role-to-instance-profile --instance-profile-name "$NAME" \
        --role-name "$NAME"

    # Verify rather than assume. If the role is not actually on the
    # profile, RunInstances fails with "Invalid IAM Instance Profile
    # name" forever — indistinguishable from the propagation race it
    # retries, so it would burn the whole retry budget and then blame
    # propagation for a permanent condition.
    local attached
    attached=$(aws iam get-instance-profile --instance-profile-name "$NAME" \
        --query 'InstanceProfile.Roles[].RoleName' --output text 2>/dev/null || true)
    case "$attached" in
    *"$NAME"*) ;;
    *) die "instance profile $NAME has no role attached (got '${attached:-none}').
  Delete it and retry:  $0 destroy" ;;
    esac
}

# Everything the runbook shells out to, installed once at first boot.
# `protobuf-compiler` and the Rust toolchain are for build-and-upload.sh;
# terraform for bringup; python3 for the driver, translator, and
# analysis; tmux because remote-campaign.sh runs campaigns under it.
render_user_data() {
    cat <<'CLOUDINIT'
#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
# clang/libclang-dev are not optional: rusqlite is built with the
# `session` feature, which forces libsqlite3-sys to run bindgen, and
# bindgen needs libclang plus clang's own builtin headers (without them
# it fails on `'stdarg.h' file not found`). CI does not surface this —
# GitHub's ubuntu-latest runners ship clang preinstalled, a bare Ubuntu
# AMI does not.
apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev protobuf-compiler \
    clang libclang-dev \
    git jq curl unzip tmux rsync ca-certificates gnupg \
    python3 python3-pip python3-venv

# HashiCorp apt repo — terraform, pinned to the same >= 1.6 the rig's
# terraform block requires.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://apt.releases.hashicorp.com/gpg |
    gpg --dearmor -o /etc/apt/keyrings/hashicorp.gpg
echo "deb [signed-by=/etc/apt/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
    >/etc/apt/sources.list.d/hashicorp.list
apt-get update && apt-get install -y terraform

# AWS CLI v2. Ubuntu's packaged awscli is v1 and misses commands the
# campaign scripts use.
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install && rm -rf /tmp/aws /tmp/awscliv2.zip

sudo -u ubuntu bash <<'ASUSER'
set -euxo pipefail
cd "$HOME"

curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
echo 'source "$HOME/.cargo/env"' >>"$HOME/.bashrc"

# The rig's shared key pair. Generated here, so the private half exists
# only inside the trust domain it unlocks — scripts/env.sh's
# resolve_local_ssh_key() looks for exactly this path, and
# install-loadgen.sh copies it onto the loadgen box for the fault-
# injection drills.
[ -f "$HOME/.ssh/id_ed25519" ] ||
    ssh-keygen -t ed25519 -N "" -f "$HOME/.ssh/id_ed25519" -C legion-bench-rig

# The campaign scripts run for hours over one SSH session; keep it alive.
printf 'Host *\n    ServerAliveInterval 30\n    ServerAliveCountMax 10\n' >>"$HOME/.ssh/config"
chmod 600 "$HOME/.ssh/config"
ASUSER

# /usr/local/bin is on the default PATH for a non-interactive `ssh host
# "cmd"`; ~/.cargo/bin is not, because rustup adds it via ~/.bashrc and
# Ubuntu's .bashrc returns early for non-interactive shells. Every remote
# command the campaign scripts issue arrives that way, so expose the
# toolchain somewhere those shells can actually see it.
for b in cargo rustc rustup; do
    ln -sf /home/ubuntu/.cargo/bin/$b /usr/local/bin/$b
done

touch /home/ubuntu/.setup-complete
CLOUDINIT
}

# --- push / tfvars / ssh / status / destroy -----------------------------

# rsync rather than `git clone`: it carries uncommitted work, and it does
# not need the box to hold a credential for a private repo. `.git` rides
# along on purpose — build-and-upload.sh stamps every artifact with
# `git rev-parse HEAD` for the fairness record.
cmd_push() {
    local ip
    ip=$(state_get .public_ip)
    log "syncing $REPO_ROOT -> $ip:~/legion"
    ssh_ "mkdir -p ~/legion"
    rsync -az --delete \
        --exclude 'target/' --exclude 'node_modules/' --exclude '.terraform/' \
        --exclude 'benchmarks/aws/results/' --exclude 'benchmarks/aws/.operator-box.json' \
        --exclude '__pycache__/' \
        -e "ssh -i $(state_get .key_path) -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes" \
        "$REPO_ROOT/" "ubuntu@$ip:legion/"
    log "synced"
}

cmd_tfvars() {
    local ip pubkey
    ip=$(state_get .public_ip)
    pubkey=$(ssh_ "cat ~/.ssh/id_ed25519.pub")
    cat <<EOF
Paste into benchmarks/aws/terraform/terraform.tfvars ON THE BOX:

admin_cidr     = "$ip/32"
ssh_public_key = "$pubkey"
EOF
}

# Write terraform.tfvars on the box, so nothing has to be pasted. The two
# values with no defaults are both facts about the box itself:
#   admin_cidr     — its Elastic IP. Every rig ingress rule pins to this,
#                    and an EIP is why it stays true for the campaign.
#   ssh_public_key — the rig key generated on the box at first boot.
cmd_configure() {
    local ip pubkey bucket=""
    ip=$(state_get .public_ip)
    pubkey=$(ssh_ "cat ~/.ssh/id_ed25519.pub")

    # The rig's own bucket is force_destroy — teardown deletes results with
    # it. A bucket terraform does not manage is the only copy that survives
    # both the VM and the teardown, so make one by default rather than
    # leaving it as an opt-in the first campaign discovers too late.
    if [ "$ARCHIVE_BUCKET" = none ]; then
        log "no archive bucket (--archive-bucket none) — results die with teardown.sh"
    elif [ -n "$ARCHIVE_BUCKET" ]; then
        bucket="$ARCHIVE_BUCKET"
        log "using existing archive bucket $bucket"
    else
        bucket="legion-bench-archive-$(aws_ sts get-caller-identity --query Account --output text)"
        if aws_ s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
            log "archive bucket $bucket already exists — reusing"
        else
            log "creating durable archive bucket $bucket (outlives teardown.sh)"
            # us-east-1 is the one region that rejects a LocationConstraint.
            if [ "$REGION" = us-east-1 ]; then
                aws_ s3api create-bucket --bucket "$bucket" >/dev/null
            else
                aws_ s3api create-bucket --bucket "$bucket" \
                    --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
            fi
        fi
    fi

    log "writing terraform.tfvars on the box"
    ssh_ "cat > ~/legion/benchmarks/aws/terraform/terraform.tfvars" <<EOF
# Written by scripts/bootstrap-operator-box.sh. Both values describe the
# operator box: its Elastic IP is the only address the rig accepts, and
# its generated key is the rig's shared key pair.
admin_cidr     = "$ip/32"
ssh_public_key = "$pubkey"
region         = "$REGION"
$([ -n "$bucket" ] && echo "archive_bucket = \"$bucket\"")
EOF
}

# Start phase 1 under tmux. Detached on purpose: `launch` returns and the
# run continues without this terminal, which is the same reason
# remote-campaign.sh exists for the loadgen box.
cmd_run() {
    if ssh_ "tmux has-session -t $REMOTE_SESSION" 2>/dev/null; then
        die "a phase-1 run is already going. '$0 logs -f' to watch, or '$0 ssh tmux kill-session -t $REMOTE_SESSION' to stop it."
    fi
    log "starting phase 1 on the box (tmux session '$REMOTE_SESSION')"
    ssh_ "cd ~/legion/benchmarks/aws && \
        tmux new-session -d -s $REMOTE_SESSION \
        \"scripts/phase1.sh$PHASE1_ARGS 2>&1 | tee $REMOTE_LOG\""
    sleep 3
    ssh_ "tmux has-session -t $REMOTE_SESSION" 2>/dev/null ||
        die "phase 1 exited immediately — check: $0 ssh cat $REMOTE_LOG"
}

cmd_logs() {
    if [ "${1:-}" = "-f" ]; then
        ssh_ "tail -f $REMOTE_LOG"
    else
        ssh_ "tail -100 $REMOTE_LOG"
    fi
}

ssh_() { # ssh_ [command...] — always to the operator box
    local ip key
    ip=$(state_get .public_ip)
    key=$(state_get .key_path)
    ssh -i "$key" -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes \
        -o ConnectTimeout=10 "ubuntu@$ip" "$@"
}

wait_for_ssh() { # wait_for_ssh <ip> [timeout-secs]
    local ip="$1" timeout="${2:-300}" start
    start=$(date +%s)
    log "waiting for SSH on $ip"
    until ssh_ true 2>/dev/null; do
        (($(date +%s) - start > timeout)) && die "timed out waiting for ssh on $ip"
        sleep 5
    done
}

# Between sessions, stop the box rather than destroying it: ~$0.37/hr
# running vs ~$0.016/hr stopped (the gp3 root volume and the Elastic IP,
# which is billed whether or not it is attached). Destroying instead
# would take the terraform state and the rig's SSH key with it.
cmd_stop() {
    local id
    id=$(state_get .instance_id)
    [ -n "$id" ] || die "no instance recorded — nothing to stop"

    # A stop kills the tmux session with the box. Say so rather than
    # letting a campaign quietly die mid-step.
    if ssh_ "tmux has-session -t $REMOTE_SESSION" 2>/dev/null; then
        cat >&2 <<EOF
WARNING: a phase-1 run is live in tmux. Stopping the box kills it, and
the rig it is driving keeps billing. Let it finish, or stop the run and
tear the rig down first. phase1.sh checkpoints, so 'run --from <step>'
resumes after a restart.
EOF
        read -rp "stop anyway? [y/N] " reply
        [[ "$reply" =~ ^[Yy]$ ]] || die "aborted"
    fi

    log "stopping $id"
    aws_ ec2 stop-instances --instance-ids "$id" >/dev/null
    aws_ ec2 wait instance-stopped --instance-ids "$id"
    log "stopped — now ~\$0.016/hr (EBS + Elastic IP). '$0 start' to resume."
}

cmd_start() {
    local id ip current
    id=$(state_get .instance_id)
    ip=$(state_get .public_ip)
    [ -n "$id" ] || die "no instance recorded — run: $0 create"

    log "starting $id"
    aws_ ec2 start-instances --instance-ids "$id" >/dev/null
    aws_ ec2 wait instance-running --instance-ids "$id"

    # An Elastic IP survives stop/start, unlike an auto-assigned public
    # IP — which is the reason the box has one, since admin_cidr in the
    # rig's terraform.tfvars is this address. Verify rather than assume:
    # if it ever came back different, every rig ingress rule is stale and
    # the next hop fails for a reason that looks like anything but this.
    current=$(aws_ ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    if [ "$current" != "$ip" ]; then
        log "public IP changed ($ip -> $current) — re-associating the Elastic IP"
        aws_ ec2 associate-address --instance-id "$id" \
            --allocation-id "$(state_get .eip_allocation_id)" >/dev/null
    fi

    wait_for_ssh "$ip"
    log "running at $ip"
    log "tmux does not survive a stop — resume with: $0 run --from <step>"
}

cmd_status() {
    local id ip
    id=$(state_get .instance_id)
    ip=$(state_get .public_ip)
    aws_ ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].{state:State.Name,type:InstanceType,launched:LaunchTime}' \
        --output table
    echo "public ip: $ip   ssh: $0 ssh"
    echo
    # The checkpoint markers phase1.sh drops, which say what has actually
    # finished — a tmux session can be alive on step 2 or step 6.
    if ssh_ "tmux has-session -t $REMOTE_SESSION" 2>/dev/null; then
        echo "phase 1: RUNNING"
    else
        echo "phase 1: not running (finished, failed, or never started)"
    fi
    ssh_ "ls -1 ~/.legion-phase1/step-*.done 2>/dev/null |
          sed 's|.*/step-|  completed step |; s|\.done||'" || true
    ssh_ "tail -3 $REMOTE_LOG 2>/dev/null" || true
}

# Order matters: the EIP cannot be released while associated, and the
# role cannot be deleted while it is in an instance profile.
cmd_destroy() {
    # Every field is optional: a `create` that failed partway recorded
    # only what it got to, and destroy's whole job in that case is to
    # clean up exactly that much.
    local id sg eip iam_role key_pair
    id=$(state_get .instance_id)
    sg=$(state_get .security_group_id)
    eip=$(state_get .eip_allocation_id)
    iam_role=$(state_get .iam_role_created)
    key_pair=$(state_get .key_pair_created)
    REGION=$(state_get .region)

    # Terraform state lives on this box and nowhere else (the rig's
    # backend is local — terraform/main.tf). Destroying the box while the
    # rig stands leaves it running, billing, and destroyable only by hand
    # in the console. Check before, not after.
    local rig_alive
    rig_alive=$(aws_ ec2 describe-instances \
        --filters "Name=tag:Project,Values=legion-bench" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query "length(Reservations[].Instances[?InstanceId!='${id:-none}'][])" \
        --output text 2>/dev/null || echo 0)
    if [ "${rig_alive:-0}" != 0 ]; then
        cat >&2 <<EOF
WARNING: $rig_alive rig instance(s) are still up in $REGION, and this box
holds the only copy of their terraform state. Tear the rig down first:

  $0 ssh 'cd ~/legion/benchmarks/aws && scripts/teardown.sh'

Destroying this box now orphans those instances — they keep billing and
only a console hunt will clear them.
EOF
    fi

    read -rp "destroy operator box ${id:-<none, partial create>} and its IAM role in $REGION? [y/N] " reply
    [[ "$reply" =~ ^[Yy]$ ]] || die "aborted"

    if [ -n "$id" ]; then
        log "terminating $id"
        aws_ ec2 terminate-instances --instance-ids "$id" >/dev/null
        aws_ ec2 wait instance-terminated --instance-ids "$id"
    fi

    if [ -n "$eip" ]; then
        log "releasing elastic IP"
        aws_ ec2 release-address --allocation-id "$eip" >/dev/null || true
    fi
    # The instance's ENI can outlive `instance-terminated` by a few
    # seconds, and the group cannot be deleted while it is in use.
    if [ -n "$sg" ]; then
        log "deleting security group"
        local attempt
        for attempt in $(seq 1 12); do
            aws_ ec2 delete-security-group --group-id "$sg" >/dev/null 2>&1 && break
            sleep 5
        done
    fi
    # Only if THIS script imported it. A pre-existing key pair of the same
    # name belongs to something else and is not ours to delete.
    if [ "$key_pair" = true ]; then
        log "deleting key pair"
        aws_ ec2 delete-key-pair --key-name "$NAME" >/dev/null || true
    fi

    if [ "$iam_role" = true ]; then
        log "deleting IAM role and instance profile"
        aws iam remove-role-from-instance-profile --instance-profile-name "$NAME" \
            --role-name "$NAME" >/dev/null 2>&1 || true
        aws iam delete-instance-profile --instance-profile-name "$NAME" >/dev/null 2>&1 || true
        aws iam delete-role-policy --role-name "$NAME" --policy-name rig-iam >/dev/null 2>&1 || true
        aws iam detach-role-policy --role-name "$NAME" \
            --policy-arn arn:aws:iam::aws:policy/PowerUserAccess >/dev/null 2>&1 || true
        aws iam delete-role --role-name "$NAME" >/dev/null 2>&1 || true
    fi

    rm -f "$STATE_FILE"
    log "destroyed. Terraform state lived on that box — if the rig was still"
    log "up, look for Project=legion-bench resources in $REGION and clear them."
}

# --- dispatch -----------------------------------------------------------

CMD="${1:-}"
[ $# -gt 0 ] && shift || true
while [ $# -gt 0 ]; do
    case "$1" in
    --region)
        REGION="$2"
        shift 2
        ;;
    --type)
        INSTANCE_TYPE="$2"
        shift 2
        ;;
    --disk-gb)
        DISK_GB="$2"
        shift 2
        ;;
    --admin-cidr)
        ADMIN_CIDR="$2"
        shift 2
        ;;
    --archive-bucket)
        ARCHIVE_BUCKET="$2"
        shift 2
        ;;
    # Credentials for THIS script's AWS calls only; see the header for
    # why they never reach the box.
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
    --skip-smoke)
        PHASE1_ARGS="$PHASE1_ARGS --skip-smoke" # passed through to phase1.sh
        shift
        ;;
    --from)
        PHASE1_ARGS="$PHASE1_ARGS --from $2"
        shift 2
        ;;
    --) # everything after this is a remote command for `ssh`
        shift
        break
        ;;
    *) break ;;
    esac
done
# Subcommands after `create` read the region from the state file so a
# stale AWS_REGION cannot point them at the wrong account's resources.
if [ -f "$STATE_FILE" ] && [ "$CMD" != create ]; then
    REGION=$(jq -r .region "$STATE_FILE")
    NAME=$(jq -r .name "$STATE_FILE")
fi

case "$CMD" in
launch) cmd_launch ;;
create) cmd_create ;;
push) cmd_push ;;
configure) cmd_configure ;;
run) cmd_run ;;
logs) cmd_logs "$@" ;;
stop) cmd_stop ;;
start) cmd_start ;;
tfvars) cmd_tfvars ;;
ssh) ssh_ "$@" ;;
status) cmd_status ;;
destroy) cmd_destroy ;;
*)
    sed -n '/^# Usage:/,/^# Requires/p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 1
    ;;
esac
