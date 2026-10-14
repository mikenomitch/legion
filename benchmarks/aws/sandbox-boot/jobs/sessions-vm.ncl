# A session task on the sandbox runtime: a cloud-hypervisor microVM booting
# Alpine with no NIC. The image comes from ECR Public, which serves AWS
# clients without Docker Hub's anonymous pull limit.
#
# 128 MiB guests in a 6 GiB envelope: 48 per c8i.xlarge worker by memory.
{
  name = "bench-vm",
  tasks = [{
    name = "vm",
    runtime = "sandbox",
    config = {
      image = "public.ecr.aws/docker/library/alpine:3.20",
      command = ["/bin/sh", "-c", "echo up; exec sleep 86400"],
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
