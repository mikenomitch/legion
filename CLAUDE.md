# CLAUDE.md

Legion's repository guidelines live in [AGENTS.md](AGENTS.md) — one file, so the
instructions never drift between tools. It is imported below; read it before
making changes.

Three rules are easy to miss and are not optional.

## Non-trivial features get a plan, before the code

Write it as `internal_docs/plans/<feature>-plan.md`: the problem, the prior art
(how Nomad/K8s/Consul solve it), goals and non-goals, the surface as the user
will type it, phases that each ship on their own, and the testing and rollout
story. Required for any new user-facing surface, subsystem, default, security
boundary, format change, or migration — not for bug fixes and refactors.

**→ How to write one: [`internal_docs/plans/README.md`](internal_docs/plans/README.md)**

## Non-trivial features record their judgement calls

Anywhere the work could have gone another way — an ambiguous request you
interpreted, a real alternative you rejected, a deviation from the plan, a
limitation you knowingly accepted — write it down in
`internal_docs/calls/<plan-name>_calls.md`. Every entry gives the call, the
alternative(s), the trade-off it costs, and why it won — tied to a core product
goal (simplicity/ease, extensibility, performance), showing the user-facing
surface, ranked by project impact.

**→ How to write them: [`internal_docs/calls/README.md`](internal_docs/calls/README.md)**

## All new features must be well documented

Public page in [`docs/`](docs/README.md) covering the surface, defaults, failure
modes, and known limitations; proto comments for every RPC and field; every new
job-config field in `configs/jobs/maximal.jsonnet`.

**→ The full rule: the Documentation section of [AGENTS.md](AGENTS.md)**

@AGENTS.md
