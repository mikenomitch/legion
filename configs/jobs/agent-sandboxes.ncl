# One microVM sandbox per user, suspended when idle and resumed in place.
#
# Guide = docs/guides/agent-sandboxes.md
#
#   legion job validate configs/jobs/agent-sandboxes.ncl
#   legion job run configs/jobs/agent-sandboxes.ncl      # places nothing
#   legion instance create agents --id user-42           # boots user-42's VM
#   legion instance suspend user-42 --wait               # checkpoint, free memory
#   legion instance start user-42 --wait                 # resume where it left off
#
# Keyed mode means the job itself runs nothing: the platform creates one
# instance per user and Legion keeps that instance's id, volume, and
# checkpoint for as long as it lives.
#
# Worker setup = the guide's "Set up the workers" section. Memory checkpoints
# need Linux with KVM and Firecracker or cloud-hypervisor on the node, and the
# idle policy needs resourceEnforcement.enabled on the worker type.
{
  name = "agents",
  # The band decides who gives way first under memory pressure
  # (docs/preemption-and-eviction.md). Sandboxes are interactive but
  # replaceable, so they sit in the default band.
  priority = "normal",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    group = "workspaces",

    stateful = {
      # One instance per user, created and destroyed by the platform.
      # Ids are never recycled, so a destroyed user's data can never leak
      # into a new instance with the same name.
      mode = "keyed",
      # The platform always names the instance (`--id user-42`), so its own
      # user ids map straight onto Legion's.
      idSource = "caller",
      storage = {
        # A guest kernel attaches a block device, so sandbox storage is
        # block mode. local-dir serves it as a disk image on the node and
        # pins the instance there (docs/volumes.md).
        path = "/workspace",
        mode = "block",
        params = { size = "10G" },
      },
    },

    config = {
      image = "ghcr.io/acme/agent-base:2026-10",
      command = ["/usr/local/bin/harness"],
      # No `network` block: the guest gets loopback only (`none`). A gated
      # network cannot take memory checkpoints yet, and a `tap` device is a
      # fixed name per task. See the guide's "Networking, today".
      # Guest vCPUs. This is the VM's shape, not a reservation: the host
      # schedules and weights the VMM's threads by resources.cpu below.
      vcpus = 2,
    },

    resources = {
      # MHz. The scheduler packs on it, and it becomes a CPU weight that only
      # matters when the node's CPUs are contended. Kept low so CPU doesn't
      # fill the node before memory does. (`cpus` is an alias for this same
      # MHz number, not a core count.)
      cpu = 250,
      # Reserved: what the scheduler packs on, and the cgroup's memory.low.
      memory = 512,
      # Ceiling: the guest boots with 4 GiB and may burst to it while the
      # node has room. Above 512 MiB it is what memory pressure reclaims
      # first (docs/resource-enforcement.md).
      memoryMax = 4096,
    },

    suspend = {
      # Capture the running VM (memory, devices, root disk), not just disk.
      checkpoint = "memory",
      # Free the reservation while suspended. That is the density win.
      reserve = false,
      # A checkpoint that cannot restore (lost node, VMM upgrade) boots the
      # user fresh on their retained /workspace instead of locking them out.
      onRestoreFailure = "cold",
      resumeTimeout = "30s",
      # Runs in the guest after every memory restore, once the new identity
      # is in place: reconnect to the platform, re-read credentials.
      onResume = ["/usr/local/bin/harness", "reconnect"],
      # The checkpoint lives on the node that took it.
      durability = "local",
      idle = {
        # Quiet this long, then suspend. Tuned for a human who stepped away.
        after = "3m",
        # The defaults. For a microVM on `none` or `tap` networking, cpu is
        # the signal that sees the guest: its traffic bypasses the node
        # datapath, and microVMs take no exec sessions. See the guide's
        # "An instance never goes idle, or goes idle too soon".
        signals = ["network", "cpu", "exec"],
        cpuBelow = 0.02,
      },
      # Under memory pressure, suspend this sandbox (state kept) before the
      # node evicts anything.
      onPressure = true,
    },

    # Opt into on-demand snapshots too: roll a workspace back, or capture a
    # warmed "golden" sandbox to start new users from.
    # A 14-day ttl leaves a weekly golden snapshot a week of overlap.
    snapshots = {
      ttl = "14d",
      keep = 3,
      memory = true,
    },

    constraints = {
      # Memory checkpoints need KVM and a real VMM.
      os = "linux",
    },
  }],
}
