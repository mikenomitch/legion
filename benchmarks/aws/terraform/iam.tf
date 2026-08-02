# Two roles. The shared one below covers every box's bucket access;
# the orchestrator role at the bottom adds fleet control and goes to the
# loadgen box only.
#
# Bucket access is scoped to the one benchmark bucket. Control plane
# needs rw (Legion B streaming replica + lease CAS), loadgen needs rw
# (result archives), workers need read (binary fetch). A disposable
# single-tenant rig doesn't earn a role per box beyond that split.
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "bucket_rw" {
  statement {
    sid       = "BucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.bench.arn]
  }

  statement {
    sid = "ObjectRW"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.bench.arn}/*"]
  }
}

resource "aws_iam_role" "bench" {
  name               = "${var.project}-instance"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy" "bench_bucket" {
  name   = "${var.project}-bucket-rw"
  role   = aws_iam_role.bench.id
  policy = data.aws_iam_policy_document.bucket_rw.json
}

resource "aws_iam_instance_profile" "bench" {
  name = "${var.project}-instance"
  role = aws_iam_role.bench.name
}

# --- Orchestrator (loadgen box only) -----------------------------------
# scripts/remote-campaign.sh drives a whole campaign from the loadgen
# box so an overnight run does not depend on the operator's laptop
# staying awake or keeping one IP. That box therefore needs to resize the
# sim fleet between scale steps and read instance addresses.
#
# Deliberately NOT terraform-on-the-loadgen-box: driving scale steps with
# `terraform apply` would mean shipping state to the rig and handing an
# internet-facing instance the IAM to rebuild or destroy every resource
# here. Two ASG calls and a describe cost far less. This is also why the
# control plane and the spot workers keep the plain role — nothing but
# the orchestrator can move the fleet.
data "aws_iam_policy_document" "fleet_control" {
  statement {
    sid = "ReadFleet"
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "ec2:DescribeInstances",
    ]
    # Neither action supports resource-level permissions.
    resources = ["*"]
  }

  statement {
    sid       = "ResizeSimFleet"
    actions   = ["autoscaling:SetDesiredCapacity"]
    resources = [aws_autoscaling_group.workers.arn]
  }
}

resource "aws_iam_role" "orchestrator" {
  name               = "${var.project}-orchestrator"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

resource "aws_iam_role_policy" "orchestrator_bucket" {
  name   = "${var.project}-bucket-rw"
  role   = aws_iam_role.orchestrator.id
  policy = data.aws_iam_policy_document.bucket_rw.json
}

resource "aws_iam_role_policy" "orchestrator_fleet" {
  name   = "${var.project}-fleet-control"
  role   = aws_iam_role.orchestrator.id
  policy = data.aws_iam_policy_document.fleet_control.json
}

resource "aws_iam_instance_profile" "orchestrator" {
  name = "${var.project}-orchestrator"
  role = aws_iam_role.orchestrator.name
}
