# A session task on the sandbox runtime: a cloud-hypervisor microVM booting
# Alpine with no NIC. The image comes from ECR Public, which serves AWS
# clients without Docker Hub's anonymous pull limit.
#
# 128 MiB guests in a 150 GiB envelope: 1,200 on a c5.metal (96 vCPU, 192 GiB), and none fits a c8i box.
{
  name = "bench-vm-metal",
  tasks = [{
    name = "vm",
    runtime = "sandbox",
    config = {
      image = "public.ecr.aws/docker/library/alpine:3.20",
      # The guest's wall clock (kvm-clock, synced with the host at boot) and
      # uptime when the command starts, for boot-times.py.
      command = ["/bin/sh", "-c", "echo \"legion-bench-up $(date +%s.%N) $(cut -d' ' -f1 /proc/uptime)\"; exec sleep 86400"],
      vcpus = 1,
      hypervisor = "cloud-hypervisor",
    },
    resources = { cpu = 60, memory = 128 },
    stateful = {
      mode = "session",
      envelope = { resources = { cpu = 200000, memory = 153600 } },
    },
    constraints = { os = "linux" },
  }],
}
