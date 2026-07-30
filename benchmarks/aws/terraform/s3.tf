# One bucket for everything the rig persists:
#   bin/               — the uploaded legion `dist` binary (+ nomad/k8s pins)
#   campaigns/<id>/    — result archives (driver output, Prometheus snapshots,
#                        fairness records)
#   legion-b/replica/  — Legion B streaming-persistence replica (rustyriver)
#   legion-b/leases/   — Legion B object-storage lease + lock objects
#
# force_destroy so `terraform destroy` tears the rig down even with
# results still inside — the campaign scripts sync archives out (or you
# copy them) BEFORE destroy; the runbook makes that explicit.
resource "aws_s3_bucket" "bench" {
  bucket_prefix = "${var.results_bucket_prefix}-"
  force_destroy = true

  tags = { Name = "${var.project}-bucket" }
}

resource "aws_s3_bucket_public_access_block" "bench" {
  bucket = aws_s3_bucket.bench.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The streaming lease protocol (ETag CAS) and replica sync both assume
# read-after-write on unversioned keys; versioning would only accumulate
# lease-object garbage.
resource "aws_s3_bucket_versioning" "bench" {
  bucket = aws_s3_bucket.bench.id

  versioning_configuration {
    status = "Disabled"
  }
}
