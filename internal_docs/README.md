# Internal Docs

Working material: plans, to-dos, and development notes. Nothing here is written
for a public audience — it is allowed to be unfinished, speculative, blunt about
gaps, and out of date.

Anything fit for users, operators, or contributors reading the code belongs in
[`docs/`](../docs/README.md) instead. That includes deeply technical material:
architecture and behavior docs are public.

## What goes where

| Put it here | Put it in `docs/` |
| --- | --- |
| Design plans, proposals, critiques | The shipped feature's reference page |
| To-do lists, milestones, roadmaps | Documented current behavior |
| Judgement-call records | Limitations a *user* needs to know (also state them publicly) |
| Competitive gap analyses | Honest "not implemented yet" notes on a real page |
| Library evaluations, migration write-ups | Architecture and internals |
| Assorted development notes | Operational runbooks |

## Current work

- [TODO.md](TODO.md) — the higher-level To Do list. Open work at the top grouped
  by area, shipped work in the Done section at the bottom. Check things off as
  you finish them.
- [differentiators.md](differentiators.md) — the running list of what makes
  Legion different from K8s and Nomad (part idea list, part roadmap).
- [stateful-vs-nomad-k8s.md](stateful-vs-nomad-k8s.md) — honest gap analysis of
  the stateful primitives against StatefulSets and Nomad.

## Notes

- [development.md](development.md) — running locally, Linux worker containers,
  gRPC poking, CI lint baseline.
- [rustyriver-migration.md](rustyriver-migration.md) — porting streaming
  persistence from the Litestream binary to the embeddable rustyriver crate.
- [ui-api-explorer.md](ui-api-explorer.md) — library evaluation behind the in-app
  API Explorer.
- [old-notes.md](old-notes.md) — original Bear notes, kept for archaeology.

## Plans

Design plans and proposals live in [`plans/`](plans/). Each plan carries a
`> **Judgement calls:**` pointer under its title.

**Every non-trivial feature gets a plan before the code** — a new user-facing
surface, subsystem, default, security boundary, format change, or migration.
When one is required and what goes in it:
[`plans/README.md`](plans/README.md).

| Plan | Status |
| --- | --- |
| [artifact-system-plan.md](plans/artifact-system-plan.md) | shipped |
| [artifact-cache-and-prewarm-plan.md](plans/artifact-cache-and-prewarm-plan.md) | planned |
| [autoscaler-plan.md](plans/autoscaler-plan.md) | planned |
| [auth-acl-plan.md](plans/auth-acl-plan.md) | phases 1–4 executed |
| [exec-and-actions-plan.md](plans/exec-and-actions-plan.md) | shipped (all five phases) |
| [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) | executed (workstreams A/B/C/D + adversarial verification, 2026-07-28); lanes topology opt-in |
| [gang-scheduling-plan.md](plans/gang-scheduling-plan.md) | planned |
| [job-templates-plan.md](plans/job-templates-plan.md) | in execution |
| [kv-reactive-task-updates-plan.md](plans/kv-reactive-task-updates-plan.md) | shipped |
| [late-binding-plan-apply-plan.md](plans/late-binding-plan-apply-plan.md) | shipped (phases 0–3; Phase 4 backlog deferred) |
| [persistence-plan.md](plans/persistence-plan.md) | phases 0–1, 3 done |
| [preemption-and-eviction-plan.md](plans/preemption-and-eviction-plan.md) | planned |
| [multi-cluster-federation-plan.md](plans/multi-cluster-federation-plan.md) | planned |
| [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md) | planned |
| [scheduler-plan-snapshot-plan.md](plans/scheduler-plan-snapshot-plan.md) | shipped |
| [scheduling-index-plan.md](plans/scheduling-index-plan.md) | shipped (phases 0–3; group-path + backlog deferred) |
| [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) | authoritative; v1 in progress |
| [service-discovery-plan.md](plans/service-discovery-plan.md) | superseded (rationale + full mesh design) |
| [service-discovery-mesh-and-config.md](plans/service-discovery-mesh-and-config.md) | superseded (cookbook + mesh extensions) |
| [service-discovery-critique.md](plans/service-discovery-critique.md) | superseded (self-critique) |
| [stateful-workloads-and-instance-identity-plan.md](plans/stateful-workloads-and-instance-identity-plan.md) | shipped |
| [task-runtime-rename-plan.md](plans/task-runtime-rename-plan.md) | shipped |
| [ui-jump-hints-plan.md](plans/ui-jump-hints-plan.md) | shipped |
| [workload-identity-oidc-plan.md](plans/workload-identity-oidc-plan.md) | shipped (task identities) |
| [volume-plugin-system-plan.md](plans/volume-plugin-system-plan.md) | shipped (v1; CSI bridge + snapshot/clone deferred) |
| [sandbox-snapshots-and-fast-start-plan.md](plans/sandbox-snapshots-and-fast-start-plan.md) | planned (§0 convergence shipped with the sandbox kind) |
| [shared-alloc-dir-proposal.md](plans/shared-alloc-dir-proposal.md) | proposal |

## Judgement calls

[`calls/`](calls/) holds one record per plan: the decisions taken where the plan
was ambiguous, contradictory, or silent — what was decided, what the alternatives
were, and why.

**Every non-trivial feature must leave one.** Name the file after its plan with
`_calls` appended (`plans/<name>.md` → `calls/<name>_calls.md`), append to the
existing file rather than starting a second one, and link it from the plan
header. Entries give the call, the alternatives, the trade-off, and why — ranked
by project impact. The full template and a worked example:
[`calls/README.md`](calls/README.md).

- [artifact-system-plan_calls.md](calls/artifact-system-plan_calls.md)
- [artifact-cache-and-prewarm-plan_calls.md](calls/artifact-cache-and-prewarm-plan_calls.md)
- [autoscaler-plan_calls.md](calls/autoscaler-plan_calls.md)
- [auth-acl-plan_calls.md](calls/auth-acl-plan_calls.md)
- [container-task-kind-macos_calls.md](calls/container-task-kind-macos_calls.md)
- [exec-and-actions-plan_calls.md](calls/exec-and-actions-plan_calls.md)
- [gang-scheduling-plan_calls.md](calls/gang-scheduling-plan_calls.md)
- [job-templates-plan_calls.md](calls/job-templates-plan_calls.md)
- [kv-reactive-task-updates-plan_calls.md](calls/kv-reactive-task-updates-plan_calls.md)
- [multi-cluster-federation-plan_calls.md](calls/multi-cluster-federation-plan_calls.md)
- [multi-raft-persistence-plan_calls.md](calls/multi-raft-persistence-plan_calls.md)
- [persistence-plan_calls.md](calls/persistence-plan_calls.md)
- [preemption-and-eviction-plan_calls.md](calls/preemption-and-eviction-plan_calls.md)
- [service-discovery-final-plan_calls.md](calls/service-discovery-final-plan_calls.md)
- [shared-alloc-dir-proposal_calls.md](calls/shared-alloc-dir-proposal_calls.md)
- [stateful-workloads-and-instance-identity-plan_calls.md](calls/stateful-workloads-and-instance-identity-plan_calls.md)
- [volume-plugin-system-plan_calls.md](calls/volume-plugin-system-plan_calls.md)
- [workload-identity-oidc-plan_calls.md](calls/workload-identity-oidc-plan_calls.md)
