# A session task on the sandbox runtime: a cloud-hypervisor microVM booting
# Alpine with no NIC. The image comes from ECR Public, which serves AWS
# clients without Docker Hub's anonymous pull limit.
#
# 128 MiB guests in a 6 GiB envelope: 48 per c8i.xlarge worker by memory.
{
  name = "bench-vm-pinned",
  tasks = [{
    name = "vm",
    runtime = "sandbox",
    config = {
      image = "public.ecr.aws/docker/library/alpine@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc",
      # The guest's wall clock (kvm-clock, synced with the host at boot) and
      # uptime when the command starts, for boot-times.py.
      command = ["/bin/sh", "-c", "echo \"legion-bench-up $(date +%s.%N) $(cut -d' ' -f1 /proc/uptime)\"; exec sleep 86400"],
      vcpus = 1,
      hypervisor = "cloud-hypervisor",
    },
    resources = { cpu = 60, memory = 128 },
    stateful = {
      mode = "session",
      envelope = { resources = { cpu = 3200, memory = 6144 } },
    },
    constraints = { os = "linux" },
  }],
}
