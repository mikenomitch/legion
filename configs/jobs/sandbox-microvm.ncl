# microVM sandboxes: keyed stateful instances, each running in its own VM
# (see docs/task-kind-sandbox.md, docs/stateful-workloads.md, docs/volumes.md).
#
# The `sandbox` kind boots an OCI image inside a microVM (cloud-hypervisor or
# firecracker on Linux+KVM; Apple `container` dev mode on macOS), so the
# workload gets its own kernel — the isolation level you want for untrusted
# code and agent workspaces.
#
# Keyed mode means submitting this job places ZERO sandboxes; each one is
# created on demand and lives until you destroy it:
#
#   legion job run configs/jobs/sandbox-microvm.ncl
#   legion instance create sandbox-vm --id user-42   # boots a microVM
#   legion job logs sandbox-vm                       # guest serial console
#   legion instance stop user-42                     # paused, /workspace kept
#   legion instance start user-42                    # same data, new VM
#   legion instance destroy user-42                  # gone; volume reclaimed
#
# On Linux you need an uncompressed guest kernel. Either export
# LEGION_SANDBOX_KERNEL=/path/to/vmlinux on the worker, install one at
# /var/lib/legion/kernels/vmlinux, or ship it per-job as an artifact and set
# `kernel = "${artifact.kernel}"` below.
{
    name = "sandbox-vm",
    tasks = [
        {
            name = "workspace",
            runtime = "sandbox",
            group = "sandboxes",
            stateful = {
                # On-demand instances with never-recycled ids: a destroyed
                # sandbox's identity (and its storage) can never leak into a
                # new one.
                mode = "keyed",
                # The persistent half of the sandbox. `mode = "block"` because
                # a guest kernel attaches a block device, not a host
                # directory = the local-dir driver serves a formatted disk
                # image bound to this instance, the worker hands it to the
                # kind, and the guest init mounts it at `path`. Destroying
                # the instance reclaims the volume (docs/volumes.md).
                storage = {
                    path = "/workspace",
                    mode = "block",
                    params = { size = "1G" },
                },
            },
            config = {
                image = "alpine:latest",
                # Print who we are and what persisted, then stay alive.
                command = [
                    "/bin/sh",
                    "-c",
                    "echo \"sandbox=$LEGION_INSTANCE_ID alloc=$LEGION_ALLOC_ID\"; " ++
                    "echo boot >> /workspace/boots.log; " ++
                    "echo \"boots so far: $(wc -l < /workspace/boots.log)\"; " ++
                    "sleep 3600",
                ],
                # Guest vCPUs. Guest RAM follows `resources` below
                # (memoryMax, else memory), so placement and the VM agree.
                vcpus = 1,
                # Writable scratch inside the ephemeral root disk. Everything
                # outside /workspace is discarded when the allocation ends.
                rootfs_headroom_mib = 256,
                # "auto" (default) picks cloud-hypervisor, then firecracker, on
                # Linux+KVM, and Apple `container` on macOS. Force one with
                # "cloud-hypervisor" / "firecracker", or use "dev" to run
                # without a VM on a machine that has no KVM (namespaces only —
                # not an isolation boundary for untrusted code). Dev-mode
                # backends bind-mount volumes instead of attaching disks, so
                # pair `hypervisor = "dev"` with a filesystem volume (drop the
                # `mode = "block"` above).
                # hypervisor = "cloud-hypervisor",
                # kernel = "/var/lib/legion/kernels/vmlinux",
                # kernel_args = "quiet",
                # Guest NIC backed by a *pre-created* host TAP device. Legion
                # does not create TAP devices (that needs CAP_NET_ADMIN and a
                # host addressing plan). Default is loopback-only.
                # network = { mode = "tap", tap = "tap0" },
            },
            resources = {
                cpu = 1000,
                memory = 512,
            },
            constraints = {
                # local-dir block volumes are node-local: after first
                # placement the instance is pinned to its node, and only
                # Linux nodes with KVM + mkfs.ext4 serve this shape at all.
                os = "linux",
            },
        },
    ],
}
