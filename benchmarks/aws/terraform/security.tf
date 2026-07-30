# One security group for every benchmark instance: anything inside the
# rig may talk to anything else (manager RPC 4747, telemetry 4749, worker
# stubs, raft transport, Nomad 4646-4648, K8s 6443/etcd, node_exporter
# 9100 — enumerating them buys nothing on a disposable single-tenant
# VPC). Admin ingress is SSH + the two dashboards only.
resource "aws_security_group" "bench" {
  name        = "${var.project}-cluster"
  description = "Legion benchmark rig: open intra-rig, admin_cidr for SSH/UI"
  vpc_id      = aws_vpc.bench.id

  ingress {
    description = "intra-rig, all protocols"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "SSH from admin"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Grafana from admin"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Prometheus from admin"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    description = "all egress (packages, S3, spot metadata)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-cluster" }
}
