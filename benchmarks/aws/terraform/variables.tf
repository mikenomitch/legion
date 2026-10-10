variable "project" {
  description = "Name prefix for every resource (and the default tag)."
  type        = string
  default     = "legion-bench"
}

variable "region" {
  description = "AWS region. Pricing in the plan assumes us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "availability_zone" {
  description = <<-EOT
    Single AZ for every instance ("" = first available). One AZ on purpose:
    cross-AZ latency would smear the quorum-commit numbers the campaign
    exists to measure, and none of the systems under test are deployed
    cross-AZ in the comparison.
  EOT
  type        = string
  default     = ""
}

variable "admin_cidr" {
  description = <<-EOT
    CIDR allowed to reach SSH, Grafana (3000), and Prometheus (9090).
    No default: set it to your address as <ip>/32. Everything else is
    intra-VPC only.
  EOT
  type        = string
}

variable "ssh_public_key" {
  description = "SSH public key material for the shared benchmark key pair."
  type        = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.10.0.0/16"
}

variable "subnet_cidr" {
  type    = string
  default = "10.10.1.0/24"
}

# --- Control plane -----------------------------------------------------

variable "control_plane_count" {
  description = "Manager/server instances. The plan's topology is 3."
  type        = number
  default     = 3
}

variable "control_plane_type" {
  description = <<-EOT
    On-demand instance type for the control plane (4 vCPU / 16 GiB in the
    plan). Default `m6id.xlarge` — same 4 vCPU / 16 GiB as the `m7i.xlarge`
    it replaces, but with 237 GB of LOCAL NVMe, which cloud-init mounts at
    /var/lib/legion so every raft data dir lands on it.

    This is a measurement decision, not a tuning one. Measured 2026-08-03
    with `experiments/heartbeat-commit-probe.sh`, identical code and a
    3-voter lanes topology, varying only the disk:

      gp3 (EBS root)   ~86 heartbeats/sec   (17% of a 2s cadence)
      local NVMe     ~2000 heartbeats/sec   (100%)

    A raft commit fsyncs, and on network-attached storage that fsync — not
    the scheduler — sets control-plane throughput. Leaving the campaign on
    gp3 would publish a comparison of EBS latency wearing three different
    schedulers' clothes.

    It is NOT a thumb on the scale for Legion. The systems are affected
    unequally *by architecture*: Nomad resets an in-memory timer on the
    leader per heartbeat (`resetHeartbeatTimer`, no raft write), K8s writes
    a small Lease to etcd (KEP-589 shrank it from the 15 KB Node object),
    and Legion replicates every heartbeat through the status lane. Fast
    local disk is what stops that architectural difference from being
    swamped by storage latency — and identical hardware for all three is
    still the rule (judgement call 3).

    The trade, which belongs in every fairness record: instance-store NVMe
    is EPHEMERAL. It does not survive a stop/start or a replacement, which
    is a genuine deployment difference from the EBS-backed control planes
    many operators run on purpose.
  EOT
  type        = string
  default     = "m6id.xlarge"
}

variable "control_plane_disk_gb" {
  description = "Root gp3 volume per control-plane instance. Sized for the 24h soak's store growth."
  type        = number
  default     = 100
}

# --- Load / metrics ----------------------------------------------------

variable "loadgen_type" {
  # 2 vCPU was not enough, and it silently capped every cell rather than
  # failing. This box runs Prometheus and Grafana AND the driver, and the
  # driver spawns one CLI process per submitted job; on an m7i.large that
  # ceilinged submission at ~36 jobs/sec, which the phase-4 campaign then
  # reported as Legion throughput while the control plane idled at 8% CPU.
  # Sized so the driver, not the box, is the limit -- and the driver now
  # says so in `submission.driver_bound` when it is still the constraint.
  description = "On-demand instance type for the Prometheus + Grafana + driver box."
  type        = string
  default     = "m7i.4xlarge"
}

variable "loadgen_disk_gb" {
  description = "Root gp3 volume for the loadgen box (Prometheus TSDB + result archives)."
  type        = number
  default     = 200
}

# --- Simulated-worker fleet --------------------------------------------

variable "worker_instances" {
  description = <<-EOT
    INITIAL count of spot instances hosting simulated nodes. At ~1,250
    sim nodes per instance: 1k step -> 1, 2k -> 2, 5k -> 4, 10k -> 8, and
    the one-off 20k validation burst -> 16.

    Campaigns scale between steps through the ASG API rather than
    terraform (scripts/env.sh: scale_workers), so the ASG ignores drift
    on desired_capacity and changing this variable after bringup does
    nothing to a running fleet. The ASG replaces nothing on the way
    up/down either way, so the control plane is untouched.

    1,250 is the plan's estimate, not a measurement. Run
    `experiments/packing-density.sh` once against the standing rig to
    find this fleet's real ceiling: a higher density needs fewer
    instances per step, which cuts both cost and the spot vCPU quota a
    campaign has to clear.
  EOT
  type        = number
  default     = 1
}

variable "worker_on_demand_percentage" {
  description = <<-EOT
    Share of the sim-worker fleet launched on-demand instead of spot
    (0-100). Default 0 = all spot, the cheapest way to run a campaign.

    Raise it when a region's spot vCPU quota cannot cover a step. Spot
    and on-demand are separate AWS quotas, so a 50/50 split draws half
    the fleet from each and neither limit has to cover the whole fleet:
    at 8 vCPU per instance the 20k validation burst is 128 vCPU — 64
    from each bucket at 50, versus 128 from one at 0.

    It costs money: on-demand runs roughly 3x spot for these types. A
    2-hour validation burst at 100 is a few dollars more; a 24h soak is
    not. It also removes spot reclaims for that share of the fleet,
    which is a fidelity difference, not just a price one — so
    collect.sh records the setting in every cell's fairness record.
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.worker_on_demand_percentage >= 0 && var.worker_on_demand_percentage <= 100
    error_message = "worker_on_demand_percentage must be between 0 and 100."
  }
}

variable "worker_instance_types" {
  description = <<-EOT
    Spot capacity pool, in preference order. Diversified so a spot
    reclaim in one pool doesn't stall a campaign; all entries are
    8 vCPU / 16 GiB so sim-fleet capacity per instance stays uniform.

    The AMD pools are there for capacity, not preference: on 2026-10-09
    the three Intel pools alone scored 1/10 on spot placement in
    us-east-1a and the ASG failed with InsufficientInstanceCapacity at
    bringup; adding c7a/c6a lifted the same AZ to 9/10. Check with
    `aws ec2 get-spot-placement-scores` before narrowing this list.
  EOT
  type        = list(string)
  default     = ["c7i.2xlarge", "c6i.2xlarge", "c7a.2xlarge", "c6a.2xlarge", "c5.2xlarge"]
}

variable "worker_disk_gb" {
  type    = number
  default = 40
}

# --- Results / streaming storage ---------------------------------------

variable "archive_bucket" {
  description = <<-EOT
    Name of an EXISTING S3 bucket, managed outside this terraform, to
    mirror campaign results into. Optional, and strongly recommended for
    any run whose numbers you intend to keep.

    The rig's own bucket is `force_destroy` — `teardown.sh` deletes it
    with everything inside — and teardown's pre-destroy sync writes to
    whatever machine ran it, which for an unattended campaign is exactly
    the disposable thing you might lose. A bucket terraform does not
    manage cannot be destroyed by terraform, so results mirrored here
    outlive both the VM and the teardown.

    Create one once, outside any campaign:

      aws s3 mb s3://my-legion-benchmarks --region us-east-1

    then set archive_bucket = "my-legion-benchmarks". The rig is granted
    read and write on it, deliberately NOT delete: the durable copy
    should not be erasable by anything running inside a disposable rig.

    Empty (the default) means no mirror, and teardown says so loudly.
  EOT
  type        = string
  default     = ""
}

variable "results_bucket_prefix" {
  description = "S3 bucket name prefix (a random suffix is appended). Holds campaign archives, the Legion B streaming replica + lease, and the uploaded legion binary."
  type        = string
  default     = "legion-bench"
}
