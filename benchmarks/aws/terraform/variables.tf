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
  description = "On-demand instance type for the control plane (4 vCPU / 16 GiB in the plan)."
  type        = string
  default     = "m7i.xlarge"
}

variable "control_plane_disk_gb" {
  description = "Root gp3 volume per control-plane instance. Sized for the 24h soak's store growth."
  type        = number
  default     = 100
}

# --- Load / metrics ----------------------------------------------------

variable "loadgen_type" {
  description = "On-demand instance type for the Prometheus + Grafana + driver box."
  type        = string
  default     = "m7i.large"
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
  EOT
  type        = number
  default     = 1
}

variable "worker_instance_types" {
  description = <<-EOT
    Spot capacity pool, in preference order. Diversified so a spot
    reclaim in one pool doesn't stall a campaign; all entries are
    8 vCPU / 16 GiB so sim-fleet capacity per instance stays uniform.
  EOT
  type        = list(string)
  default     = ["c7i.2xlarge", "c6i.2xlarge", "c5.2xlarge"]
}

variable "worker_disk_gb" {
  type    = number
  default = 40
}

# --- Results / streaming storage ---------------------------------------

variable "results_bucket_prefix" {
  description = "S3 bucket name prefix (a random suffix is appended). Holds campaign archives, the Legion B streaming replica + lease, and the uploaded legion binary."
  type        = string
  default     = "legion-bench"
}
