# Judgement Calls — Service Discovery

Companion to
[`plans/service-discovery-final-plan.md`](../plans/service-discovery-final-plan.md),
which is the authoritative plan. It consolidates the original
[plan](../plans/service-discovery-plan.md), the
[config cookbook / mesh extensions](../plans/service-discovery-mesh-and-config.md),
and the [self-critique](../plans/service-discovery-critique.md); where those
disagree, the final plan wins. The calls below are the binding decisions from all
four documents in one place.

## Correctness posture

1. **AP read / CP write.** The original plan never stated a CAP posture and its
   implied default was wrong. Reads (resolution) stay available through a
   partition, serving node-local last-known-good; writes (definitions, intentions)
   go through the consistent path. Naming the posture is the point — an unstated
   default is what produces surprise outages.
2. **Liveness-as-lease.** Health is a lease on the worker heartbeat; expiry
   auto-evicts endpoints. Cleanest model in the prior art (ZK ephemeral nodes,
   etcd leases) and it removes a whole class of stale-endpoint bugs.
3. **Self-preservation on correlated failure.** Mass eviction is suspended when
   failures correlate (a degraded observer is distrusted rather than believed).
   Eureka's lesson: the failure mode of a naive health model is evicting a healthy
   fleet.
4. **Fail-open locality.** Locality is a *preference*, never isolation — an
   invariant, not a config default. A locality mechanism that can strand traffic
   is worse than no locality.
5. **Deregister-before-stop.** Drain ordering on rollout/drain, so an endpoint
   leaves the catalog before the process it points at goes away.
6. **Fail-static templates.** On template render failure, keep last-good output;
   never fail the task. Paired with coalesce + splay + cap on endpoint-change
   `on_change` actions so an endpoint flap cannot become a restart storm.

## Shape of the feature

7. **Catalog as a projection, not a registration RPC.** Services derive from
   `task_config × allocation × allocation_health × node`. No registration call
   means no drift between "what is registered" and "what is running", and no new
   write path to keep consistent.
8. **One block, one document.** `network` and `service` live on the task, so
   there is nothing to apply out of order. This is a direct response to Istio's
   documented config-ordering footguns (VirtualService before DestinationRule →
   503s; a DestinationRule mTLS default that instantly breaks working traffic).
9. **Validation at submit, loud status after.** Dangling port refs are rejected at
   submit; the catalog shows `0/N healthy` loudly. The alternative — accepting
   config that references nothing and 503ing silently — is the single most
   complained-about mesh behavior.
10. **`legion service doctor` is part of the feature, not a follow-up.** One
    command that walks the layers and names the failing one, modeled on
    `linkerd check`'s verdict + cause + remediation contract. The canonical
    alternative is a ten-step manual debugging ladder.
11. **No per-task sidecar, ever.** A per-node forwarder instead. Sidecars bring
    jobs that never complete, app-before-proxy startup races, and pod restarts on
    mesh upgrade — all avoidable by construction.
12. **The datapath is a plugin, with a CNI shim.** Keeps Linux and macOS on the
    same design instead of making macOS a degraded special case, and lets the
    ecosystem's CNI plugins be reachable without adopting CNI as the native ABI.
13. **VIP-first with DNS-direct fallback** for load balancing, and a
    **no-UDP-over-NAT resolver path** with correct truncation + TCP fallback and
    bounded, randomized large answers.
14. **Per-resource validation, partial apply, delta pushes** — one bad resource
    must not block every other resource's update.

## Scope

15. **Hard v1 line at Phases 1–3.** v1 is ports + env-var discovery + the catalog
    projection + `list | resolve | doctor`, working identically on Linux and
    macOS. The mesh (DNS, forwarder, mTLS, intentions, L7 waypoint, WASM
    middleware) is strictly post-v1. The critique's finding was that the plan was
    too big to ship and the simplicity tenet was losing to seven-phase sprawl.
16. **The L4 forwarder gets its own SLO'd sub-plan.** It is a large new data-plane
    component and was being waved at; it does not ride along inside the SD plan.
17. **mTLS depends on workload identity**, plus jittered rotation and an optional
    non-mTLS L4 mode — otherwise the mesh inherits the cert-rotation-storm problem
    and a hard dependency it cannot degrade past.
18. **Cross-service env injection: never.** Not deferred — rejected. This is the
    Kubernetes service-env-var E2BIG lesson; discovery goes through DNS,
    templates, or the resolve API.
19. **Honest macOS scope.** `/etc/resolver/legion` rather than implying parity we
    cannot deliver.
