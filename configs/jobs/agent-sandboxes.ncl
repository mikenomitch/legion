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
      requireId = true,
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
      # A NIC per sandbox: the node creates a TAP device for each instance
      # (`lgf` plus a hash of its id), attaches it to br0, recreates it on
      # resume, and deletes it with the allocation. Any number of these fit
      # on a node, and each can take memory checkpoints. br0 and DHCP on it
      # are the operator's. See the guide's "Networking".
      network = { mode = "tap", tap = "auto", bridge = "br0" },
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
      # Keep a quarter of the reservation charged while suspended, so most
      # resumes find room. false frees it all (the full density win).
      reserve = 0.25,
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
        # What a TAP-wired microVM can show: bytes the guest sends on its
        # TAP, and its CPU. `exec` would never fire (a microVM takes no exec
        # sessions), and `connections` is refused (no datapath fronts the
        # guest). See the guide's "An instance never goes idle, or goes idle
        # too soon".
        signals = ["network", "cpu"],
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

    # A platform token for the harness, kept fresh in the guest with no
    # reboot: the worker rewrites the guest's identity disk on every refresh,
    # and the guest's handler rewrites /legion/identity/platform. Inert until
    # the manager enables workload identity. See the guide's "Security".
    identities = [{
      name = "platform",
      audience = ["api.acme.dev"],
      ttl = "15m",
      file = { path = "/legion/identity/platform" },
    }],

    constraints = {
      # Memory checkpoints need KVM and a real VMM.
      os = "linux",
    },
  }],
}
