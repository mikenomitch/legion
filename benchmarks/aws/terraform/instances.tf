# Canonical's official Ubuntu 24.04 AMI pointer — resolved at apply time,
# never a hardcoded AMI id. Pinned x86_64 to match the instance types.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_key_pair" "bench" {
  key_name   = "${var.project}-key"
  public_key = var.ssh_public_key
}

locals {
  node_exporter_version = "1.8.2"

  bootstrap_b64 = base64encode(templatefile("${path.module}/cloud-init/common.sh.tftpl", {
    node_exporter_version = local.node_exporter_version
  }))
}

# --- Control plane: 3 on-demand boxes, stable private IPs ---------------
# The same three boxes host every system serially (Legion A, Legion B,
# Nomad, K8s) — identical hardware is the credibility argument, so these
# are never recreated between campaigns. lifecycle.ignore_changes on the
# AMI keeps a mid-campaign `apply` (e.g. scaling workers) from replacing
# them under a new Ubuntu point release.
resource "aws_instance" "control_plane" {
  count = var.control_plane_count

  ami                    = data.aws_ssm_parameter.ubuntu.value
  instance_type          = var.control_plane_type
  subnet_id              = aws_subnet.bench.id
  private_ip             = local.control_plane_private_ips[count.index]
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  iam_instance_profile   = aws_iam_instance_profile.bench.name

  root_block_device {
    volume_type = "gp3"
    volume_size = var.control_plane_disk_gb
  }

  user_data = templatefile("${path.module}/cloud-init/instance.yaml.tftpl", {
    role           = "control-plane"
    hostname       = "${var.project}-cp-${count.index}"
    bootstrap_b64  = local.bootstrap_b64
    install_docker = false
  })

  # cloud-init only ever runs on first boot, so an in-place user_data
  # update (the provider's default) changes nothing about the running
  # box — `apply` reports success while leaving the old, broken boot
  # intact. If the first-boot script changed, the box has to be rebuilt
  # or the change is a lie. Deliberate: `ignore_changes = [ami]` above
  # already prevents the accidental churn this would otherwise cause.
  user_data_replace_on_change = true

  lifecycle {
    ignore_changes = [ami]
  }

  tags = {
    Name = "${var.project}-cp-${count.index}"
    Role = "control-plane"
  }
}

# --- Load / metrics box -------------------------------------------------
# Prometheus + Grafana + the workload driver. Never colocated with the
# control plane so measurement and load generation don't steal the CPU
# being measured.
#
# Takes the orchestrator instance profile rather than the shared one: it
# is also the box that drives unattended campaigns
# (scripts/remote-campaign.sh), so it alone may resize the sim fleet.
resource "aws_instance" "loadgen" {
  ami                    = data.aws_ssm_parameter.ubuntu.value
  instance_type          = var.loadgen_type
  subnet_id              = aws_subnet.bench.id
  private_ip             = local.loadgen_private_ip
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  iam_instance_profile   = aws_iam_instance_profile.orchestrator.name

  root_block_device {
    volume_type = "gp3"
    volume_size = var.loadgen_disk_gb
  }

  user_data = templatefile("${path.module}/cloud-init/instance.yaml.tftpl", {
    role           = "loadgen"
    hostname       = "${var.project}-loadgen"
    bootstrap_b64  = local.bootstrap_b64
    install_docker = true
  })

  # See the control plane above: an in-place user_data update leaves the
  # already-booted box untouched, so a first-boot change must rebuild it.
  user_data_replace_on_change = true

  lifecycle {
    ignore_changes = [ami]
  }

  tags = {
    Name = "${var.project}-loadgen"
    Role = "loadgen"
  }
}

# --- Simulated-worker fleet: spot ASG -----------------------------------
# Fake nodes are the one place spot is safe: an interruption is just a
# fleet blip the campaign already models as churn (the sim fleet
# re-registers from the replacement instance). Scale steps move desired
# capacity through the ASG API — see the lifecycle block below.
resource "aws_launch_template" "worker" {
  name_prefix   = "${var.project}-worker-"
  image_id      = data.aws_ssm_parameter.ubuntu.value
  key_name      = aws_key_pair.bench.key_name
  ebs_optimized = true

  iam_instance_profile {
    name = aws_iam_instance_profile.bench.name
  }

  network_interfaces {
    subnet_id                   = aws_subnet.bench.id
    security_groups             = [aws_security_group.bench.id]
    associate_public_ip_address = true
  }

  block_device_mappings {
    device_name = "/dev/sda1"

    ebs {
      volume_type = "gp3"
      volume_size = var.worker_disk_gb
    }
  }

  user_data = base64encode(templatefile("${path.module}/cloud-init/instance.yaml.tftpl", {
    role           = "sim-worker"
    hostname       = "${var.project}-worker"
    bootstrap_b64  = local.bootstrap_b64
    install_docker = false
  }))

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name = "${var.project}-worker"
      Role = "sim-worker"
    }
  }
}

resource "aws_autoscaling_group" "workers" {
  name                = "${var.project}-workers"
  desired_capacity    = var.worker_instances
  min_size            = 0
  max_size            = 20 # 20k-node validation burst ceiling
  vpc_zone_identifier = [aws_subnet.bench.id]

  # Scale steps move desired_capacity through the ASG API
  # (scripts/env.sh: scale_workers), not through terraform: the loadgen
  # box drives campaigns and has no state to apply from. var.worker_instances
  # is therefore the *initial* size only, and terraform must not drag the
  # fleet back to it on an unrelated apply mid-campaign.
  lifecycle {
    ignore_changes = [desired_capacity]
  }

  mixed_instances_policy {
    instances_distribution {
      # 0 = all spot. Raise it to split the fleet across the spot and
      # on-demand vCPU quotas, which AWS meters separately — see
      # var.worker_on_demand_percentage.
      on_demand_percentage_above_base_capacity = var.worker_on_demand_percentage
      spot_allocation_strategy                 = "price-capacity-optimized"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.worker.id
        version            = "$Latest"
      }

      dynamic "override" {
        for_each = var.worker_instance_types

        content {
          instance_type = override.value
        }
      }
    }
  }

  tag {
    key                 = "Role"
    value               = "sim-worker"
    propagate_at_launch = true
  }
}
