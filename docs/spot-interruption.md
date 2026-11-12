# Spot Interruption

Spot and preemptible instances are cheap because the cloud can take them
back. It says so a little ahead of time: EC2 publishes an interruption notice
two minutes before it stops or terminates a spot instance. Without help, that
notice reaches Legion only when the node is already gone, as a [lost
node](node-liveness.md), and a [gang](gang-scheduling.md) on it finds out by
losing a member.

Legion can act on the notice instead. A node with a **pending interruption**
keeps gangs off it and moves the gang members it runs before it goes, so a
training job restarts on healthy nodes while the doomed one is still up,
rather than after its peers have stalled in a collective for the time it
takes to declare the node lost.

## Where a notice comes from

**The worker's watcher.** A worker type that runs on spot capacity sets:

```nickel
# worker config
spotInterruption = {
  provider = "aws",                        # EC2's spot/instance-action; the only provider
  # endpoint = "http://169.254.169.254",   # the metadata service (default)
  # pollEvery = "5s",                      # default "5s", at least "1s"
},
```

The worker polls the instance metadata service's `spot/instance-action`
(IMDSv2, falling back to v1) every `pollEvery`. When a notice appears it
reports it on every heartbeat while it stands; when it disappears (a stop
the cloud withdrew), it stops reporting it and the managers withdraw it. A
metadata service that cannot be reached leaves the last answer in place, so a
hiccup neither invents nor retracts an interruption.

**An operator, or a handler of your own.** For other clouds, for scheduled
maintenance, or for an external handler (an ASG lifecycle hook, a
Kubernetes-style node termination handler), record the notice by hand:

```console
$ legion node interrupt gpu-07 --in 2m --detail "spot reclaim"
$ legion node interrupt gpu-07 --clear
```

`--in` is when the node is expected to go, from now (default: now). The
command writes the node's `legion.interruption` dynamic attribute through
`UpdateNodeDynamic`, so it needs `node:write` on the node's pool, and an agent
can do the same with the `legion_node_update_dynamic` MCP tool. A notice an
operator set is not withdrawn by a worker that reports none; `--clear`
withdraws it.

## What a notice does

The notice is recorded on the node as the dynamic attribute
`legion.interruption`:

```console
$ legion node info gpu-07 -j
{"ok":true,"data":{"id":"gpu-07", ...,
  "dynamicAttrs":{"legion.interruption":{"deadline":1794471720,"source":"aws-imds",
                                          "detail":"spot instance-action: terminate"}}}}
```

| Field | Meaning |
| --- | --- |
| `deadline` | When the node is expected to go, Unix seconds. |
| `source` | `aws-imds` (the worker's watcher) or `operator` (`legion node interrupt`). |
| `detail` | The notice's action, or the operator's text. |

While it stands:

- **No gang places on the node.** A fresh gang, a top-up, and a gang
  restarting all skip it; `job plan` shows the node as not taking the gang.
  Other work still places there (it is not moved either, see below).
- **The gang members on the node are lost now.** When the notice lands, each
  gang member on the node is stopped (stop cause `interruption`) and its
  gang's `onMemberLoss` applies, exactly as if the node had been lost: by
  default (`restart`) the whole gang stops and places again elsewhere;
  `replace` and `restartInPlace` move just the lost members. A
  `job.gang_member_lost` event records each, with cause `interruption`. Like
  a lost node, this spends no move budget.

A withdrawn notice (`--clear`, or the watcher no longer seeing it) sets the
attribute to `null`; the node takes gangs again. Members already moved stay
where they went.

## What it does not do

- **It does not drain the node.** Work that is not a gang keeps running until
  the node goes, and is then replaced like any lost node's. To move
  everything, drain the node (`legion node drain`), from the same handler that
  records the notice.
- **It does not checkpoint.** A member is stopped with its normal graceful
  stop (`stop.killAfter`), and a stop notice whose cause is `interruption`:
  a workload that checkpoints on SIGTERM can use the grace, but nothing waits
  for a checkpoint to finish.
- **It does not act on the deadline.** The notice moves gang members as soon
  as it lands, whatever its deadline: a gang re-placing takes longer than the
  two minutes AWS gives. The deadline is recorded for people and for the
  handler that drains.
- **One provider.** The watcher reads EC2's notice only. Other clouds need a
  handler that runs `legion node interrupt`.

## Failure modes and diagnostics

| You see | It means | What to do |
| --- | --- | --- |
| Gang members stopped with cause `interruption` | Their node has a pending interruption. | Nothing: their gang moved them. `legion node info` shows the notice. |
| A gang blocks though a node has room | The node has a pending interruption, so it takes no gang. | Wait for the node to go, or `legion node interrupt <id> --clear` if the notice is stale. |
| A worker with `spotInterruption` never reports a notice | Its metadata service is unreachable from the worker (a proxy, a hop limit of 1 for IMDSv2 inside a container). | Run the worker on the host network, or raise the instance's metadata hop limit. |
| Manager startup fails with `invalid spotInterruption config` | An unknown `provider`, or a `pollEvery` under 1s or not a duration. | Fix the worker config. |
