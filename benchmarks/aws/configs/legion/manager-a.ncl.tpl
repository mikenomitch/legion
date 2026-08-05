# Legion A: 3-voter raft, lanes topology, the HA deployment shape from
# docs/multi-raft.md. Rendered per manager by scripts/install-legion-a.sh
# with envsubst: NODE_ID (cp-0|cp-1|cp-2), BOOTSTRAP (true|false),
# PRIVATE_IP, CP0_IP/CP1_IP/CP2_IP, EVAL_THREADS, and JOIN_TOKEN_LINE
# (empty on the bootstrap node, `joinToken = "lgnraft_..."` on joiners).
{
  server = {
    port = 4747,
    host = "0.0.0.0",
    # The address embedded in every join token this manager mints. Not
    # optional here: with host '0.0.0.0' and this unset, Legion derives
    # it as http://127.0.0.1:4747, so a remote worker decodes the token
    # and dials its OWN loopback. The fleet then retries connection-
    # refused forever while the campaign only sees "registered: 0/N".
    # Distinct from persistence.raft.advertiseAddr below, which is the
    # raft peer address.
    advertiseAddr = "http://${PRIVATE_IP}:4747",
  },
  storePath = "/var/lib/legion/store",
  workerConfigPath = "/opt/legion/configs/workers/",
  # Defaults to 4, and 4 is still the number to quote. Measured
  # 2026-08-04: raising this to 16 made throughput WORSE at both scales
  # (1000 nodes 354.6 -> 310.4 alloc/sec, 5000 nodes 86.6 -> 77.3) while
  # box CPU went 29% -> 85%, because the extra planners collided and
  # their plans were discarded — the plan-apply rejection rate went
  # 31.9% -> 73.6%. The CPU was doing work that was thrown away.
  #
  # plan-apply-conflict-reduction-plan.md has since landed, and this
  # number is exactly what that fix should let you raise. Whether it
  # does is a measurement, not an assumption, which is why the value is
  # rendered rather than hardcoded: sweep it with run-campaign.sh's
  # --eval-threads and compare against the table above.
  evalThreads = ${EVAL_THREADS},
  schedulingPolicy = "bin_pack",
  defaultPriority = 50,
  artifacts = {},

  # Scraped by the loadgen Prometheus: the per-phase scheduler histogram
  # (legion_scheduler_plan_phase_seconds) is the campaign's core metric.
  telemetry = {
    enabled = true,
    port = 4749,
  },

  persistence = {
    type = "raft",
    raft = {
      nodeId = "${NODE_ID}",
      bootstrap = ${BOOTSTRAP},
      # Raft transport shares the manager gRPC listener; wildcard bind
      # means peers need the routable address spelled out.
      advertiseAddr = "${PRIVATE_IP}:4747",
      peers = [
        "cp-0=${CP0_IP}:4747",
        "cp-1=${CP1_IP}:4747",
        "cp-2=${CP2_IP}:4747",
      ],
      expectedVoters = 3,
      dataDir = "/var/lib/legion/raft",
      # Lanes: control/scheduler/status raft groups isolated from each
      # other — heartbeat storms can't stall control-plane commits. The
      # campaign measures this topology because it is what the docs
      # recommend at this fleet size.
      topology = "lanes",
      joinTokenTtlSeconds = 900,
      ${JOIN_TOKEN_LINE}
    },
  },
}
