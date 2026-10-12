# The preemption drill (RUNBOOK 12.8, smoke.sh sandbox-preempt): microVM
# sandboxes in a low band fill the workers, then a high-band job that cannot
# fit displaces one of them (docs/preemption-and-eviction.md).
#
# One file, submitted twice with different vars:
#
#   # 1. fill: as many 3 GiB low-band microVMs as fit; the rest wait blocked
#   legion job run tests/spot-check/jobs/13-band.ncl \
#       --var name=band-fill --var band=low --var-code count=6 --var-code mib=3072
#   # 2. preview, then claim: one high-band microVM of the same size
#   legion job plan tests/spot-check/jobs/13-band.ncl \
#       --var name=band-claim --var band=high --var-code count=1 --var-code mib=3072
#   legion job run tests/spot-check/jobs/13-band.ncl \
#       --var name=band-claim --var band=high --var-code count=1 --var-code mib=3072
#   legion events list --kind allocation.preempted
#   legion job stop band-claim ; legion job stop band-fill
#
# No stateful storage, on purpose: an allocation with node-local storage is
# never a preemption victim, so a keyed workspace would only show the
# "holding node-local storage (never preemptible)" ending.
#
# Size `mib` from `legion node usage`: the fill should leave less than `mib`
# free on every worker, so the claim cannot place without displacing. Stop
# the sessions job first (its envelopes hold 2 GiB per worker).
let images = (import "lib/images.ncl") vars in
let params = {
  name | String | default = "band-fill",
  band | String | default = "low",
  count | Number | default = 6,
  mib | Number | default = 3072,
} & vars in
{
  name = params.name,
  priority = params.band,
  task = {
    name = "vm",
    runtime = "sandbox",
    lifecycle = "service",
    count = params.count,
    config = {
      image = images.agent,
      command = [
        "/bin/sh",
        "-c",
        "echo \"band vm $LEGION_ALLOC_ID up\"; exec sleep 86400",
      ],
      vcpus = 1,
      hypervisor = "cloud-hypervisor",
    },
    # The guest boots at `mib` and the scheduler reserves all of it. Guest
    # memory is only backed as the guest touches it, so a mostly idle 3 GiB
    # guest costs the node far less than 3 GiB of real memory.
    resources = { cpu = 250, memory = params.mib },
    constraints = { os = "linux" },
  },
}
