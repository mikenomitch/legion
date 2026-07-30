# One instance role for every rig box, scoped to the one benchmark
# bucket. Control plane needs rw (Legion B streaming replica + lease
# CAS), loadgen needs rw (result archives), workers need read (binary
# fetch). A disposable single-tenant rig doesn't earn three roles; if
# this template ever grows real tenancy, split it.
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
