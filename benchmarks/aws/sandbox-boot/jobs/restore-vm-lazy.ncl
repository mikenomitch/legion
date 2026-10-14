# A keyed sandbox that fills 900 MiB of its 2 GiB with random data (the
# guest's /dev/shm takes up to half its RAM), for timing a memory suspend and
# resume. memory_restore = "lazy". Lazy restore needs cloud-hypervisor v52
# or later and falls back to eager below that.
{
  name = "bench-restore-lazy",
  tasks = [{
    name = "vm",
    runtime = "sandbox",
    config = {
      image = "public.ecr.aws/docker/library/alpine:3.20",
      command = ["/bin/sh", "-c", "head -c 900m /dev/urandom > /dev/shm/fill; echo filled; exec sleep 86400"],
      vcpus = 2,
      hypervisor = "cloud-hypervisor",
      memory_restore = "lazy",
    },
    resources = { cpu = 2000, memory = 2048 },
    stateful = { mode = "keyed" },
    suspend = { checkpoint = "memory" },
    constraints = { os = "linux" },
  }],
}
