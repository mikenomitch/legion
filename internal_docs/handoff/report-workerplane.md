# Workerplane track report (saved by coordinator from the agent's hand-back)

P0-1 fixed: Phase 1 of worker-plane-auth-plan implemented; CA pin, two-step CA rotation, ExecTask ticket class deferred (calls entries in internal_docs/calls/worker-plane-auth-plan_calls.md: 10, 11, 15, 20-25, renumbered).

Root cause: worker served 127.0.0.1:4848 and registered http://127.0.0.1:4848; Worker service checked no caller.

Changes:
1. src/worker/plane.rs: bind default 0.0.0.0; LEGION_WORKER_HOST, LEGION_WORKER_PORT, LEGION_WORKER_ADVERTISE_ADDR (host or host:port, IPv6 bracketed); --advertise-addr on join and start worker; auto-detect via connected UDP socket local addr toward manager; log "Worker plane listening on 0.0.0.0:4848 (mTLS); advertised as https://..."; workerAddress https://.
2. mTLS listener: swappable rustls config (refreshed certs on new conns); handshakes on own tasks -> serve_with_incoming, peer_certs() for handlers; client cert requested not required; WORKER_RPC_CALLERS table, authorize() first line in real worker + simulator; PostWork, GetTaskLogs, RestartWorker, ExecTask manager-only; StartClaim ticket; Unauthenticated w/ plan wording; foreign-CA handshake failure -> node re-issues cert (<=1/min).
3. src/manager/worker_plane.rs: manager cert ClientAuth-only 24h, re-mint after 8h, CA change within 10s; worker_plane::target(store,node) refuses http:// naming protocol version, trusts only cluster CA, verifies <node>.node.legion, caches per (node,address), 5s connect timeout; used by dispatcher (push, speculative, ratification), exec/actions, logs, restart. legion sandbox start dials https:// and verifies node against CA (--ca-cert or fetched).
4. CSR issuance: worker generates key, csr_pem required, key_pem removed (reserved 2); manager picks names; eager after registration (10s bound, retried each heartbeat); persisted <state-dir>/identity/ 0600 atomic; reloaded at start; mesh uses same identity.
5. Simulator: fleet's first node requests one cert; every node registers workerPlaneName pointing at it; managers honor only for simulated:true; stub listener enforces caller classes; --advertise-host renamed --advertise-addr (benchmarks/aws script updated).
6. Errors: Unavailable naming node, address, port, fix; worker's own answer passes through with its code; PROTOCOL_VERSION=3.

Tests: tests/worker_plane.rs 4/4 (every_worker_rpc_is_classified; per-class admission; foreign CA refused + refresh; factory refuses wrong node / explains closed port / refuses plaintext); tests/worker_simulation.rs 3/3; tests/acl_endpoints.rs 27/27; lib unit tests in mesh_ca, worker::plane, manager::worker_plane, simulate, dispatcher, netd::tls/ingress, manager_forwards_worker_restart over mTLS incl impostor refused.

Files: new src/worker/plane.rs, src/manager/worker_plane.rs, tests/worker_plane.rs; src/manager/mesh_ca.rs, src/worker/server.rs, src/manager/exec.rs, src/cmd/start.rs, src/worker/simulate.rs, tests/worker_simulation.rs; surgical: src/worker/daemon.rs, src/manager/server.rs, src/manager/dispatcher.rs, src/cmd/join.rs, src/cmd/sandbox.rs, src/worker/forwarder.rs, src/worker/netd/tls.rs, src/worker/mod.rs, src/manager/mod.rs, src/constants.rs, proto, tests/acl_endpoints.rs. Docs: architecture-managers-workers.md (Ports and who dials whom), auth-and-acls.md (worker plane section, removed false claim), edge-boot.md, upgrades.md (protocol 3), network-handlers.md, worker-simulation.md, data-directory.md, agent-readiness.md, configs/jobs/sandbox-edge-boot.ncl, README Add more machines, internal_docs/development.md. Plan status IN EXECUTION w/ "Phase 1 as shipped" checklist; CA pin moved to Phase 4; sandbox-exec-plan citation fixed (calls entry 16).

Deferred:
- CA pin in join tokens (§6.5) -> Phase 4 (calls 10).
- Two-step CA rotation (§6.9) (calls 11): after `legion net ca rotate` first exec/logs/restart per node can fail; node nothing dials keeps old cert up to 8h. Cheap follow-up: CA fingerprint on heartbeat reply.
- Worker/Probe + WORKER_UNREACHABLE (Phase 8) (calls 22).
- ExecTask ticket class, exec.direct, JWKS verifier wait for sandbox-exec plan (calls 15).
- Encrypted worker->manager channel (Phase 4) unchanged.
- Plan §9 tests not written: two workers on 127.0.0.2/.3 in one process, CA-rotation integration, protocol-2 worker integration, AWS 1,000-node dispatch throughput before/after (dispatch now over TLS, unmeasured).

Found:
- Speculative dispatch never fired for real workers (read protocolVersion from config_attrs, registration stores in dynamic_attrs). Fixed -> edge-boot speculative dispatch now on for real nodes for the first time: smoke test.
- --run-worker on a manager in a multi-manager cluster advertises 127.0.0.1 (manager addr loopback). Workaround LEGION_WORKER_ADVERTISE_ADDR; real fix in manager/daemon.rs: detect toward server.advertiseAddr.
