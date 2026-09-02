output "control_plane_public_ips" {
  value = aws_instance.control_plane[*].public_ip
}

output "control_plane_private_ips" {
  value = aws_instance.control_plane[*].private_ip
}

output "loadgen_public_ip" {
  value = aws_instance.loadgen.public_ip
}

output "loadgen_private_ip" {
  value = aws_instance.loadgen.private_ip
}

output "worker_asg_name" {
  description = "Query current spot fleet members with: aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names <this>"
  value       = aws_autoscaling_group.workers.name
}

output "bucket" {
  value = aws_s3_bucket.bench.bucket
}

output "region" {
  value = var.region
}

# Instance types are part of every cell's fairness record, and the
# loadgen box has no terraform state to read them from — they ride in the
# rig manifest instead (scripts/env.sh: write_rig_manifest).
output "control_plane_type" {
  value = var.control_plane_type
}

output "worker_on_demand_percentage" {
  description = "Sim-fleet purchase mix; rides the rig manifest into every fairness record."
  value       = var.worker_on_demand_percentage
}

output "archive_bucket" {
  description = "Durable results mirror outside this terraform ('' = none)."
  value       = var.archive_bucket
}

output "loadgen_type" {
  value = var.loadgen_type
}

output "ssh_config" {
  description = "Paste into ~/.ssh/config (or use scripts/env.sh helpers)."
  value       = <<-EOT
    %{for idx, ip in aws_instance.control_plane[*].public_ip~}
    Host ${var.project}-cp-${idx}
      HostName ${ip}
      User ubuntu
    %{endfor~}
    Host ${var.project}-loadgen
      HostName ${aws_instance.loadgen.public_ip}
      User ubuntu
  EOT
}
