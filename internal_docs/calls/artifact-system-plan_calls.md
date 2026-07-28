# Judgement Calls — Artifact System

Companion to [`plans/artifact-system-plan.md`](../plans/artifact-system-plan.md).
Decisions taken while designing the plan where it was ambiguous or where a real
alternative existed. Each entry: the call, the alternative, and why. Decided
here, reversible later.

Entries are ranked by project impact, highest first.

1. **First-class task field, not per-kind config.** The alternative (each
   kind grows its own `download` options) was rejected: it forfeits reuse,
   splinters auth, and makes admission validation per-kind. Kinds keep
   agency through the three-hook recipe (§8).
2. **Pin by default (`resolve: 'pin'`), roll explicitly
   (`resolve: 'always'`)** (decided, was an open question): deterministic
   rollouts are the safe default; tag-tracking is available but opt-in,
   with its mid-rollout drift cost documented.
3. **KV ACL gap accepted for now** — with documentation, a source-code
   comment at the credential-read site, and a TODO callout so the
   artifact credential path becomes an early ACL enforcement point later.
4. **Cluster auth defaults support `kv` and `nodeIdentity`, never task
   `identity`.** Task identities are a per-task security boundary (the WI
   plan's `sub` rules); silently attaching one cluster-wide would blur it.
   Node identity exists precisely to be the cluster-grain credential.
5. **Cache excluded from this plan, by design.** Fetch-at-staging is
   correct on its own; the `ArtifactSink` seam and digest-everywhere
   contract let the companion plan add the cache without touching fetchers,
   specs, or plugins. The cost — repeat downloads on restart — is accepted
   and documented until that plan lands.
6. **Container `image` remains the user-facing field** and desugars at
   admission — but gated off by default until the cache plan lands (see
   Rollout) to avoid regressing runtime-side image caching.
7. **Blocking `ArtifactFetcher` trait run on blocking threads**, matching
   the existing getters and — decisively — the dylib plugin boundary
   (Phase 6), where async traits across `cdylib` are a hazard. Parallelism
   comes from the worker's fetch pool, not async fan-out.
8. **All-or-nothing staging per task** (any artifact failure fails the
   allocation through the existing failure path). Partial staging plus
   per-artifact retry policies was judged not worth the state-machine
   complexity in v1.
9. **Fetch failures reuse `on_unhealthy` budgets** (decided, was an open
   question): a staging failure is an allocation failure — restart, then
   move, then job-fail, like any other. A dedicated `onFetchFailure`
   policy (e.g., registry outage ≠ unhealthy task) is a documented
   potential follow-up once real-world flakiness data exists.
10. **Interpolation is dumb substitution** (`${artifact.<name>}` only, no
    expressions), mirroring the WI plan's sub-template philosophy: easy to
    validate at admission, nothing to sandbox.
11. **OCI via the `oci-client` crate** (decided, was an open question):
    registry auth flows and manifest/media-type handling are
    well-trodden, security-sensitive code; the dependency cost is worth not
    re-implementing the token dance.
