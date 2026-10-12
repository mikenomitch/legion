# Judgement calls — workerplane track (P0-1)

The calls for this work belong to an existing plan, so they are recorded in its
calls doc (one calls file per plan, per internal_docs/calls/README.md), inserted
at rank with the doc renumbered:

**internal_docs/calls/worker-plane-auth-plan_calls.md**, entries 10, 11, 15, and
20–25 (added 2026-10-12):

| # | Call | Impact |
| --- | --- | --- |
| 10 | Phase 1 ships without the CA pin (§6.5); it moves to Phase 4, because a pin checked over the plaintext worker channel checks a public value | security boundary, token format |
| 11 | CA rotation stays one step (§6.9 deferred); a node re-issues on the next heartbeat after a handshake fails for an unknown CA (rate-limited 1/min) | rotation behavior, acceptance criterion not met |
| 15 | `ExecTask` admits only `manager` until the sandbox-exec plan builds exec tickets; two-class refusal logic is in place | classification table |
| 20 | Worker-plane channels keyed by (node name, address), not address (stops cross-node payload delivery; keeps sim at one connection) | correctness/perf |
| 21 | Simulated fleets serve one certificate named by `workerPlaneName`, honored only on `simulated: true` nodes (plan open question 4) | benchmark rig |
| 22 | No `Worker/Probe` yet (Phase 8); every unreachable call is explained by node, address, and port, with a 5s connect timeout | diagnostics |
| 23 | The listener binds only once it holds a certificate; a fresh one is requested at every registration even when a persisted one loads | startup behavior |
| 24 | Manager certificates minted lazily on first dial (re-minted at 8h or on CA change), not at startup | deviation from §6.2 |
| 25 | `--advertise-addr` for both real and simulated workers; the simulator's `--advertise-host` is removed (no alias) | CLI surface |

Renumbering: old entries 10–16 became 12, 13, 14, 16, 17, 18, 19. Citations
fixed in internal_docs/plans/worker-plane-auth-plan.md (§6.1 "calls entry 16",
§6.3 "calls entry 19", §6.11 "calls entry 17") and
internal_docs/plans/sandbox-exec-plan.md (line ~579, "calls entry 16"), and in
the calls doc's own header and entry 13.

The user-visible parts of these calls are stated in the public docs:
docs/auth-and-acls.md ("The worker plane" → "Known limits"),
docs/architecture-managers-workers.md ("What Is Not Implemented Yet"),
docs/edge-boot.md ("Known limitations"), docs/worker-simulation.md.
