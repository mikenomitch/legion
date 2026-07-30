# AWS comparative-benchmark rig — network, instances, and storage only.
#
# Deliberately system-agnostic: Terraform makes the same boxes for every
# campaign (Legion A/B, Nomad, Kubernetes); the per-system install is done
# by the scripts in ../scripts over SSH, so "identical hardware" stays
# literal. See internal_docs/plans/aws-comparative-benchmark-plan.md and
# ../README.md for the runbook.
#
# State is local by default. If more than one person runs campaigns, move
# it to S3 (uncomment the backend block and create the state bucket first).

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # backend "s3" {
  #   bucket = "<your-tf-state-bucket>"
  #   key    = "legion-aws-benchmark/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Purpose   = "benchmark" # everything here is disposable
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  az = var.availability_zone != "" ? var.availability_zone : data.aws_availability_zones.available.names[0]

  # Stable private IPs so config templates and Prometheus targets don't
  # depend on apply-time ordering.
  loadgen_private_ip = cidrhost(var.subnet_cidr, 5)
  control_plane_private_ips = [
    for i in range(var.control_plane_count) : cidrhost(var.subnet_cidr, 11 + i)
  ]
}
