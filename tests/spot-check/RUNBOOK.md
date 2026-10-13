# Spot-check runbook

A hands-on pass through Legion's headline features on the AWS rig and on
your Mac, before sharing Legion externally. Plan on about two hours. Work top
to bottom, tick the checklist at the end, and note anything that surprises
you. "Surprising" is the signal this pass exists to catch.

An agent already ran this once. What it found, with root causes, is in
[launch-spot-check-findings.md](../../internal_docs/launch-spot-check-findings.md).
Each step below says what good looks like and which known issue you will hit
along the way, so you can tell a new problem from a known one.

Commands are fish. Run them from the repo root on the `spot-check/launch`
branch.

## 0. Setup

You need the `legion-bench` AWS profile, OrbStack (or Docker Desktop), Rust,
and Apple `container` (`brew install container`).

**If the rig is down,** bring it up. The Linux build takes about an hour under
Rosetta, and the README has the command. Apply `shims.patch` before that build
while the fixes it stands in for are missing, and build the macOS CLI from the
same tree for section 10.

```fish
tests/spot-check/infra/up.sh
tests/spot-check/images/build-push.sh
tests/spot-check/infra/install.sh
```

**Point your CLI at the cluster.** The repo's `mise.toml` sets `LEGION_ADDR`
to localhost, so a bare `legion` talks to your Mac. Either load the rig's env
into this shell, or prefix commands with the `lg` wrapper, which always
targets the rig's current leader.

```fish
cargo build --bin legion && set -gx PATH (pwd)/target/debug $PATH   # this shell only
tests/spot-check/infra/cli-env.sh --fish | source
alias lg tests/spot-check/infra/lg          # optional
tests/spot-check/infra/ssh.sh sc-wkr-1       # a shell on a box, when you need one
```

Worker public IPs are in `tests/spot-check/.state/hosts`. App ports only
accept your IP as of `up.sh`. If you change networks, re-run `up.sh` (it is
idempotent and adds the new IP).

```fish
set w1 (awk '$1=="sc-wkr-1"{print $2}' tests/spot-check/.state/hosts)
set w2 (awk '$1=="sc-wkr-2"{print $2}' tests/spot-check/.state/hosts)
```

## 1. The cluster

```fish
legion raft status          # 3 voters, quorum 2
legion node list            # sc-wkr-1 and sc-wkr-2 active
legion volume drivers       # host and local-dir usable on 2/2
legion acl whoami           # the management token
```

**Good:** 3 voters, 2 active workers, volume drivers `2/2`.
**Known:** if `node list` says active but jobs fail with `No nodes available`,
you are in findings P0-6. Run `legion node uncordon sc-wkr-1` and the same
for `sc-wkr-2`.

## 2. Basic container, exec, and sandbox over HTTP

```fish
legion job run tests/spot-check/jobs/01-container.ncl
legion job run tests/spot-check/jobs/02-exec.ncl
legion job run tests/spot-check/jobs/03-sandbox.ncl
curl http://$w1:8081/ ; curl http://$w2:8081/   # container, from private ECR
curl http://$w1:8082/ ; curl http://$w2:8082/   # host process
curl http://$w1:8090/ ; curl http://$w2:8090/   # microVM: one of the two answers
```

**Good:** each container response names its allocation. The exec page shows
the host kernel. The microVM answers "from a Legion microVM" on whichever
worker it landed on, and its log shows a different, guest kernel.
**Look at:** `legion service resolve web` returns private IPs, not
`127.0.0.1` (the advertised worker address at work).
**Known:** Legion creates the microVM's TAP on `br0` and leases it
`172.30.0.2` (the guest's `LEGION_GUEST_ADDR` shows it), but the bridge and the host port forward are the rig's
(`infra/cloud-init/worker.sh`). Legion does not map ports into a guest.

## 3. Logs and exec on all three

```fish
legion job logs hello-container -f          # curl again in another tab; lines appear
legion exec --job hello-container -it -- /bin/sh
legion exec --job hello-exec -it -- /bin/bash   # this is a shell ON THE HOST
legion action run hello-container env
legion job logs hello-vm                    # the guest's serial console
```

**Good:** live log lines, interactive shells with a working prompt and
resize, the action prints `LEGION_*` vars.
**Known:**
- `legion exec` into the microVM works through the guest agent, once
  `legion-guest` is on the workers (section 12.0). Without it, exec fails
  closed with an error naming the missing guest agent.
- The exec-task shell lacks the task's env (P2-8).
- `cat /proc/self/cgroup` in the exec task shows limits are not enforced
  (P1-1).

## 4. A somewhat complex DAG

```fish
legion job run tests/spot-check/jobs/05-dag.ncl --var run=human1
legion job logs airports-dag -f
```

Then open `http://<its worker>:8085/report.html` (the `serve` task's node).

**Good, by design:** `fetch` → `validate` → four parallel steps (`flaky` fails
once and retries) → `report` → `serve`, each step starting after the one
before it succeeds.
**Known, expect this to look wrong:** every step starts at once and
crash-loops until its inputs exist (P0-4), and finished batch steps restart
every 30 seconds forever (P0-2). Count `[fetch] fetched` lines in the logs.
Stop the job when you are done looking: `legion job stop airports-dag`.

## 5. A full-stack app on the service mesh

```fish
legion kv put fullstack/db-password (openssl rand -hex 16)
legion job run tests/spot-check/jobs/06-fullstack.ncl
open http://$w1:8080/ ; open http://$w2:8080/
legion service resolve api
legion job scale fullstack --task api --count 4
```

**Good:**
- Both frontends load.
- A note added on one shows up on the other.
- The `served by` line changes as you reload (HTTP-mode balancing), and
  `cache hit` or `cache miss` reflects the Redis cache.
- After scaling, the new replicas start answering without a redeploy.

**Known:** the stock nginx and Postgres images do not run in Legion's Linux
containers, so this uses rebuilt ones (P0-7). If nothing answers after a
couple of minutes, check `legion service doctor api`. "health checks
starting" forever means the P0-3 shim is missing from the binary.

## 6. Sandboxed agents, with the launcher

```fish
legion job run tests/spot-check/jobs/07-agents.ncl
tests/spot-check/launcher/run-rig.sh         # then open http://localhost:8089
```

In the launcher:

1. Launch a **box** sandbox. It appears with an allocation within about a
   minute.
2. Click **Terminal** and run `curl -sI https://pypi.org/simple/ | head -1`
   (allowed) and `curl -sI https://example.com/` (denied by the egress proxy
   with a 403). Then run `ls /workspace; tail journal.md`.
3. Click **Stop**, then **Start**. The terminal comes back with
   `/workspace/journal.md` intact and `.boots` incremented.
4. Launch a **vm** sandbox. **Logs** shows the agent booting inside a
   microVM. **Terminal** opens a shell inside the guest through the guest
   agent (needs `legion-guest` on the workers, section 12.0).
5. **Destroy** both. It needs a second click within 5 seconds.

**Known:** a box restart occasionally fails on a network-namespace race and
then gets stuck "rescheduling" (P1-4 then P1-3).

## 7. KV updates reaching a workload

```fish
legion kv put spot-check/greeting hello
legion kv put spot-check/message "first message"
legion job run tests/spot-check/jobs/04-kv.ncl
curl http://$w1:8083/ ; curl http://$w2:8083/   # one answers
legion kv put spot-check/message "changed live"  # file: same `started`
legion kv put spot-check/greeting bonjour        # env: new `started`
```

**Good:** the message changes within a few seconds and `started` stays put.
The greeting change gives a new `started` time.
**Known:** if you edit the spec and re-run it, the update rollout can wedge on
the static port and take the service down (P1-2). `legion job stop kv-demo`,
then `job run` again.

## 8. Stateful Redis on AWS storage

```fish
legion job run tests/spot-check/jobs/08-redis.ncl
legion action run redis cli-cmd --param cmd="SET greeting persisted"
legion job run tests/spot-check/jobs/08-redis.ncl --force-reschedule
legion action run redis cli-cmd --param cmd="GET greeting"     # persisted
legion volume list --job redis
legion action run redis cli --tty                              # interactive redis-cli
```

Then the harder one: reboot the worker that holds it (`legion job logs
redis` names the node), and wait for it to come back.

```fish
tests/spot-check/infra/ssh.sh sc-wkr-1 sudo reboot
legion node list                                               # lost, then active
legion action run redis cli-cmd --param cmd="GET greeting"
```

**Good:** the value survives the reschedule and the reboot. The data lives in
`/data/redis/appendonlydir` on the EBS disk.
**Watch for:** after the reboot, whether the worker rejoins on its own,
whether Redis comes back without help (P1-3 or P1-5 may bite), and how long
it takes.

## 9. Failure drills (optional, and they break things)

**Leader failover.** Find the leader, stop it, and confirm that reads, exec,
and writes keep working against the new one.

```fish
tests/spot-check/infra/leader.sh
tests/spot-check/infra/ssh.sh sc-mgr-2 sudo systemctl stop legion-manager   # if mgr-2 leads
tests/spot-check/infra/cli-env.sh --fish | source                           # follow the new leader
legion job logs hello-container ; legion exec --job hello-container -- true
legion job scale hello-exec --task web --count 2
tests/spot-check/infra/ssh.sh sc-mgr-2 sudo systemctl start legion-manager
```

**Known, and the most important thing to see for yourself (P0-5):** stopping
or demoting **sc-mgr-1** strands both workers, because their join tokens
point at it and followers cannot forward heartbeats. To recover, make
sc-mgr-1 the leader again:

```fish
tests/spot-check/infra/on-manager.sh sc-mgr-1 raft command trigger-election
legion node uncordon sc-wkr-1 ; legion node uncordon sc-wkr-2               # P0-6
```

**Worker restart.** `tests/spot-check/infra/ssh.sh sc-wkr-2 sudo systemctl
restart legion-worker`, then check what happened to its tasks
(`legion node list`, the HTTP endpoints). Watch for orphaned processes
(P1-5).

## 10. Local development on macOS

Use a separate data dir so nothing collides with your usual local manager,
and check nothing already holds 4747 (`lsof -nP -iTCP:4747`).

```fish
container system start
tests/spot-check/mac/build-local.sh                 # spot-check-web:dev and spot-check-agent:dev
env LEGION_DATA_DIR=/tmp/legion-sc legion start manager --run-worker -c tests/spot-check/mac/manager.ncl
```

In another tab (with `LEGION_ADDR=http://127.0.0.1:4747`, which mise sets):

```fish
legion job run tests/spot-check/jobs/02-exec.ncl --var-code count=1
legion job run tests/spot-check/jobs/mac/mac-container.ncl
curl http://localhost:18081/
legion exec --job mac-container -it -- /bin/sh
legion kv put spot-check/greeting hi ; legion kv put spot-check/message local
legion job run tests/spot-check/jobs/04-kv.ncl --var image=spot-check-agent:dev
curl http://localhost:8083/ ; legion kv put spot-check/message changed ; curl http://localhost:8083/
legion job run tests/spot-check/jobs/mac/mac-agents.ncl
env LAUNCHER_JOB=mac-agents python3 tests/spot-check/launcher/launcher.py   # http://localhost:8088
```

The dev loop to try: edit `images/web/app.py`, rerun `mac/build-local.sh
web`, then `legion job run tests/spot-check/jobs/mac/mac-container.ncl
--force-reschedule`, and curl again.

**Good:** everything above works, including a launcher terminal into a
macOS sandbox VM.

**Known:**
- Run `02-exec.ncl` without `count=1` once to see P1-2: two replicas on one
  static port wedge the scheduler, and the job says `done` with nothing
  running. `legion job scale hello-exec --task web --count 1` recovers it.
- Apple `container` needs at least 200 MiB per task (P2-6).
- Also try the README quickstart in an empty directory outside the repo.
  It fails today (P2-1).

## 11. Optional

- **UI:** `cd ui && npm install && env VITE_MANAGER_ENDPOINT=http://<leader-ip>:4747 npm run dev`,
  then paste the management token (`tests/spot-check/.state/token`) on the
  Access page.
- **MCP:** `claude mcp add legion-sc --env LEGION_ADDR=http://<leader-ip>:4747 --env LEGION_TOKEN=<token> -- (pwd)/target/debug/legion mcp serve --allow-write`,
  then ask an agent to deploy and debug one of these jobs.

## 12. Sandboxes and sessions

The current microVM sandbox surface on two workers: exec through the guest
agent, gated egress and the host guard, credential injection, suspend and
resume, the idle policy, store durability, memory pressure, preemption, and
session mode. Small on purpose: a handful of sandboxes and a few dozen
sessions, no load balancer. Plan on about an hour.
`tests/spot-check/smoke.sh sandboxes` runs all of it unattended.

Steps marked **(PR #36)** test the session-durability work
([session-durability-and-routing-plan.md](../../internal_docs/plans/session-durability-and-routing-plan.md)).
On a branch without it they fail or are skipped, and that is expected.

Not covered: `suspend.keepFor` on sessions (session mode Phase 5, not built),
the gateway at scale, and anything behind a load balancer.

### 12.0 Prerequisites

**The guest agent.** The agent image is Alpine (musl), so the worker cannot
fall back to copying itself into the guest. Without `legion-guest` beside the
worker binary, exec into a microVM and every gated network path fail closed
([task-runtime-sandbox.md, "The guest agent"](../../docs/task-runtime-sandbox.md#the-guest-agent)).
Build it in the same container as the worker binary, then re-run
`install.sh`, which uploads it to `/opt/legion/bin/legion-guest` on the
workers when it finds it (or set `SC_GUEST_BINARY`):

```fish
docker run --rm --platform linux/amd64 -v "$PWD":/src:ro -v "$PWD/target-linux-amd64":/target \
  -e CARGO_TARGET_DIR=/target -w /src rust:1.90-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq protobuf-compiler clang libclang-dev musl-tools >/dev/null && rustup target add x86_64-unknown-linux-musl && cargo build -p legion-guest --profile dist --locked --target x86_64-unknown-linux-musl'
SC_ONLY_BINARY=1 tests/spot-check/infra/install.sh
tests/spot-check/infra/ssh.sh sc-wkr-1 ls -l /opt/legion/bin/legion-guest
```

**Edge boot, workload identity, and direct exec.** The rig's configs turn
these on: `workloadIdentity` and `edgeBoot` in `infra/configs/manager.ncl.tpl`,
and `edgeBoot.acceptClaims` and `exec.direct` in
`infra/configs/workers/default.ncl`. A rig installed before 2026-10-12 has
the old configs, because `install.sh` only writes them on first install.
Push them by hand: managers read worker configs at start, so restart the
managers (followers first), then the workers.

```fish
for m in sc-mgr-1 sc-mgr-2 sc-mgr-3
    tests/spot-check/infra/ssh.sh $m "sudo tee /opt/legion/configs/workers/default.ncl >/dev/null" < tests/spot-check/infra/configs/workers/default.ncl
    tests/spot-check/infra/ssh.sh $m "grep -q edgeBoot /opt/legion/configs/manager.ncl || sudo sed -i 's|^  acl = { enabled = true },|&\n  workloadIdentity = { enabled = true },\n  edgeBoot = { enabled = true },|' /opt/legion/configs/manager.ncl"
end
# restart: the two followers, then the leader (legion raft status names it)
tests/spot-check/infra/ssh.sh sc-mgr-2 sudo systemctl restart legion-manager   # and so on
tests/spot-check/infra/ssh.sh sc-wkr-1 sudo systemctl restart legion-worker
tests/spot-check/infra/ssh.sh sc-wkr-2 sudo systemctl restart legion-worker
legion boot-ticket create sessions --task shell --pool default --scope session --ttl 5m   # mints: both switches are on
```

**A content store (optional, for 12.6).** The workers have no instance
profile, so give them a KV credential. Your SSO session credentials expire,
so re-run the `kv put` when they do. Then uncomment the `contentStore` block
in `infra/configs/workers/default.ncl` with the rig's bucket name
(`legion-spot-check-<account>`), push it as above, and restart the managers
and workers.

```fish
aws configure export-credentials --profile legion-bench --format process \
  | jq -c '{type: "aws", accessKeyId: .AccessKeyId, secretAccessKey: .SecretAccessKey, sessionToken: .SessionToken, region: "us-east-1"}' \
  | read -l cred; and legion kv put secrets/content-store $cred
legion node info sc-wkr-1 | grep content-publish      # advertised once configured
```

**The jobs.**

```fish
legion kv put spot-check/echo-key sc-key-1             # injected by 12.3
legion job run tests/spot-check/jobs/09-sandboxes.ncl  # ws (keyed, gated) and idler
legion job run tests/spot-check/jobs/11-sessions.ncl   # session envelopes
```

### 12.1 Start, exec, stop

```fish
legion instance create sandboxes --task ws --id ws-1
legion instance get ws-1                       # running, its node, its http endpoint
legion job logs sandboxes | grep ws-1          # "sandbox ws-1 booted on ..."
legion exec --instance ws-1 -- uname -r        # the guest kernel, not the host's
legion exec --instance ws-1 -it -- /bin/sh     # a shell; try `ip link` (only lo)
legion exec --instance ws-1 -- sh -c 'exit 3'; echo $status   # 3
legion exec --instance ws-1 -- sh -c 'echo $LEGION_NETWORK_MODE; grep " /workspace " /proc/mounts'
legion volume list --job sandboxes             # one volume bound to ws-1
```

**Good:** the instance is running within a couple of minutes (the first boot
pulls the image and builds the root disk). Exec reaches the guest, exit
status passes through, the guest has no NIC, `LEGION_NETWORK_MODE=gated`,
and `/workspace` is a mounted block volume.
**Known:** `legion exec` failing with "the guest agent ... is not listening"
means `legion-guest` is missing on that worker (12.0).

### 12.2 Egress allow, deny, and the host guard

```fish
legion exec --instance ws-1 -- curl -sI https://pypi.org/simple/       # 200
legion exec --instance ws-1 -- curl -sI https://example.com/           # refused at CONNECT
legion exec --instance ws-1 -- curl -sI http://example.com/            # 403, Legion-Denied-By: policy
legion exec --instance ws-1 -- curl -sI http://169.254.169.254/latest/meta-data/   # 403, Legion-Denied-By: host-guard
legion exec --instance ws-1 -- curl -sI http://<its node's private ip>:4848/       # 403, host-guard
legion net policy test sandboxes --task ws --to https://example.com/             # deny, and which rule
tests/spot-check/infra/ssh.sh <its node> "sudo find /var/lib/legion -name net-access.log -exec tail -3 {} +"   # verdicts and deciding rules
```

**Good:** the allow list works, a plain-HTTP denial carries a
`Legion-Denied-By` header naming who refused it, and the metadata address is
refused by `host-guard` even though `09-sandboxes.ncl` lists
`169.254.169.254` in its allow list: the host guard sits below job policy
([network-handlers.md, "The host guard"](../../docs/network-handlers.md)).
The node's own worker port is refused the same way.

### 12.3 Credential injection

```fish
legion exec --instance ws-1 -- sh -c 'curl -s http://$LEGION_UPSTREAM_ECHO/headers'   # x-spot-check-key: sc-key-1
legion exec --instance ws-1 -- sh -c 'env; cat /legion/env' | grep sc-key-1             # nothing
legion kv put spot-check/echo-key sc-key-2
legion exec --instance ws-1 -- sh -c 'curl -s http://$LEGION_UPSTREAM_ECHO/headers'   # sc-key-2 within a minute
```

**Good:** the guest calls its `echo` upstream in plain HTTP on loopback, the
node opens TLS to httpbin.org and adds the header from KV, and the key is
never in the guest. A rotation reaches the next request with no restart.

### 12.4 Ingress

```fish
legion instance get ws-1 -j | jq '.data.instance.endpoints'
curl http://<public ip of that node>:<port>/       # {"instance": "ws-1", "process": ..., "boots": 1}
```

**Good:** the guest answers on the node's address through the datapath (vsock
into a guest that has no NIC). The port is in 20000-32000, which the rig
opens to your IP.

### 12.5 Suspend and resume (local durability)

```fish
legion exec --instance ws-1 -- sh -c 'echo before > /tmp/marker'
curl -s http://<node>:<port>/ | jq .process                 # note it
legion instance suspend ws-1 --wait -j                      # state suspended, kind memory, bytes, durationMs
legion instance get ws-1                                    # suspended, reason manual
legion instance start ws-1 --wait -j                        # restoreMs, coldBoot false
legion exec --instance ws-1 -- cat /tmp/marker /tmp/resumed-at /legion/resume.env
curl -s http://<node>:<port>/ | jq .process                 # the same as before
legion instance stop ws-1 ; legion instance start ws-1      # a cold boot on the same volume
legion exec --instance ws-1 -- sh -c 'ls /tmp; wc -l < /workspace/boots'   # no marker; 2 boots
legion instance suspend ws-1 --wait ; legion instance start ws-1 --cold --wait
```

**Good:** `/tmp` lives in guest RAM, so the marker surviving proves a memory
restore rather than a reboot. `onResume` wrote `/tmp/resumed-at`,
`/legion/resume.env` names the new allocation, and the same server process
answers. Stop then start is a cold boot: `/tmp` is fresh and `/workspace`
counts a second boot. `start --cold` boots fresh from a suspended state.
**Look at:** the restore time against the 300 ms p50 target for a 512 MiB
memory checkpoint (docs/suspend-and-resume.md). Nested virtualization on
m7i.large is slower than bare metal, so treat a miss here as a data point,
not a failure.

### 12.6 Store durability: resume on the other worker

Needs the content store from 12.0. Without one, `instance suspend` is refused
with FailedPrecondition naming the node, which is itself worth checking once.

```fish
legion job run tests/spot-check/jobs/10-sandbox-store.ncl
legion instance create sandbox-store --id sbs-1
legion exec --instance sbs-1 -- sh -c 'echo moved > /tmp/marker'
legion instance suspend sbs-1 --wait -j                         # durability store
legion instance get sbs-1 -j | jq -r .data.instance.checkpointState   # local, then published
legion node cordon <its node>
legion instance start sbs-1 --wait --timeout 300 -j            # node is the other worker
legion exec --instance sbs-1 -- cat /tmp/marker                 # moved
legion node uncordon <its node> ; legion instance destroy sbs-1
aws s3 ls s3://legion-spot-check-<account>/content/sealed/ --profile legion-bench | head   # sealed parts only
```

**Good:** the checkpoint publishes, the resume fetches it on the other node,
and guest memory moves with it. The bucket holds only sealed parts.

### 12.7 Idle suspend and wake

```fish
legion instance create sandboxes --task idler --id idle-1
legion instance get idle-1                 # running; then leave it alone
# about a minute later (idle.after = 30s plus the sweep)
legion instance get idle-1                 # suspended, reason idle
# wake through the gateway, on a worker (it dials the node's dynamic port)
tests/spot-check/infra/ssh.sh sc-wkr-2 "env LEGION_ADDR=http://<leader private ip>:4747 LEGION_TOKEN=(cat tests/spot-check/.state/token) legion start gateway --listen 127.0.0.1:4780 &"
tests/spot-check/infra/ssh.sh sc-wkr-2 "curl -s -H 'Legion-Instance: idle-1' http://127.0.0.1:4780/"
legion instance get idle-1                 # running again
```

**Good:** the idle policy suspends a sandbox with no network, CPU, or exec
activity, and a request through the gateway resumes it and is answered by the
same process (a memory restore). Polling `instance get` is not activity.
**Known:** the idle policy needs resource enforcement, which the rig's worker
config turns on.

### 12.8 Eviction: memory pressure and preemption

Memory pressure, with `ws-1` running:

```fish
legion job run tests/spot-check/jobs/12-mem-hog.ncl
legion node usage sc-wkr-1                       # watch the PRESSURE column
legion events list --kind node.pressure          # reclaim, suspend (ws-1), evict-bursting (mem-hog)
legion job stop mem-hog
legion instance get ws-1                         # suspended, reason pressure
legion instance start ws-1 --wait
```

**Good:** the steps run in order: reclaim, then suspend for `ws-1` (it sets
`suspend.onPressure`), then evict-bursting for the hog, which is far above its
128 MiB reservation. Nothing inside its reservation is evicted.
`legion_node_pressure_actions_total` counts each step on the `:4749/metrics`
of the manager the worker heartbeats to (sum across managers). The hog's
replacement waits its `on_unhealthy.move_delay` before it is placed again, so
it is not re-evicted in a tight loop. If `ws-1` is on the other node, only the hog's node acts.

Preemption (stop `sessions` and `mem-hog` first; their room matters):

```fish
legion job stop sessions
legion job run tests/spot-check/jobs/13-band.ncl --var name=band-fill --var band=low --var-code count=6 --var-code mib=3072
legion node usage                                # less than 3 GiB free on each worker
legion job plan tests/spot-check/jobs/13-band.ncl --var name=band-claim --var band=high --var-code count=1 --var-code mib=3072
legion job run tests/spot-check/jobs/13-band.ncl --var name=band-claim --var band=high --var-code count=1 --var-code mib=3072
legion events list --kind allocation.preempted
legion job stop band-claim ; legion job stop band-fill
```

**Good:** `job plan` prints "Preemption: would preempt" and names a
band-fill allocation, the run preempts it, and the high-band microVM boots in
the room. Adjust `mib` if `node usage` shows the fill left more room than
that (`SC_FILL_MIB` for `smoke.sh`).
**Known:** an allocation with node-local storage is never a victim, which is
why `13-band.ncl` has none. `suspend.keepFor` (keep a suspended session's
checkpoint for a time, then report it `evicted`) is not built (session mode
Phase 5), so there is nothing to test for it yet.

### 12.9 Session mode

```fish
legion job run tests/spot-check/jobs/11-sessions.ncl
legion job status sessions                                 # an envelope per worker per task
legion instance create sessions --task shell -j            # instanceIds, mode session, durable false, node
legion instance list sessions -j | jq '.data | {complete, asOf, n: (.instances | length)}'
seq 80 | xargs -P 40 -I{} legion instance create sessions --task shell -j | jq -c '{ok, code: .error.code}' | sort | uniq -c
legion instance create sessions --task vm -j               # a microVM session
legion instance promote <a shell session id> --id promoted-1 -j   # durable true, allocationId
legion exec --instance promoted-1 -- true
legion instance stop <the vm session id> ; legion instance destroy promoted-1
```

**Good:** starts answer at once with `durable: false`; the listing is
`complete`. The burst of 80 starts up to 32 per worker (the envelope's
memory over 32 MiB per session) and refuses the rest as `RESOURCE_EXHAUSTED`
(retryable), with no other errors. The raft
applied index on the leader (`legion_raft_applied_index` on `:4749/metrics`)
moves far less than once per start. Promotion gives a named, durable instance
you can exec into.
**(PR #36)** Session ids read `shell-<8 base32 tag>.<26>`, one tag per node.

### 12.10 Session nodes, tags, and edge starts (PR #36)

First, the picker's view: which nodes hold an envelope, their tags, and
whether they take edge starts.

```fish
legion session nodes sessions --task shell                 # node, address, tag, accepting, room, weight
legion session nodes sessions --task shell -j | jq -c '.data.nodes[] | {node, tag, accepting, reason}'
legion session nodes sessions --task shell --format envoy-eds | jq .   # what an Envoy picker would load
legion instance create sessions --task shell -j | jq -r '.data | "\(.instanceIds[0]) \(.node)"'
legion instance get <that id> ; legion instance stop <that id>      # routed to its node by the tag
```

**Good:** both workers are listed with an 8-character tag, `accepting:
true` (they advertise `edge-sessions` because `edgeBoot.acceptClaims` is on;
otherwise `reason` is `no-edge-sessions`). A session id from `instance
create` reads `shell-<tag>.<26>`, and its tag is the tag `session nodes`
shows for the node it landed on. `instance get` and `stop` on a tagged id go
to that node first (Worker.GetSession).

Then the edge path: start, read, and stop a session at a worker with a
session ticket and no manager on the path. Port 4848 accepts only the
managers, so run `legion session` on `sc-mgr-1`; it fetches the cluster CA
with your token. `smoke.sh sessions-nodes` and `sessions-edge` do the same.

```fish
set ticket (legion boot-ticket create sessions --task shell --pool default --scope session --ttl 30m -j | jq -r .data.ticket)
legion session nodes sessions --task shell -j | jq -r '.data.nodes[] | "\(.node) \(.address)"'   # https://<private ip>:4848
tests/spot-check/infra/ssh.sh sc-mgr-1      # then, there, with the token exported as LEGION_TOKEN:
```
```bash
export LEGION_ADDR=http://127.0.0.1:4747 LEGION_TOKEN=<token> T=<ticket> W1=https://<sc-wkr-1 ip>:4848 W2=https://<sc-wkr-2 ip>:4848
legion session start sessions --task shell --node $W1 --node-name sc-wkr-1 --ticket $T -j   # sessionId, node, state running
legion session get <id> --node $W1 --ticket $T -j                     # origin edge, generation 1
legion session stop <id> --node $W2 --ticket $T -j                    # NotFound, wrong_node
legion session get <id> --node $W1 --ticket ${T}x -j                  # PermissionDenied
legion session start sessions --task shell --node $W1 --ticket $T --idempotency-key k1 -j   # twice: same id, deduplicated true
legion session stop <id> --node $W1 --ticket $T -j                    # stopped true, then false
```

**Good:** the start answers `running` with an id carrying sc-wkr-1's tag;
`get` shows `origin: edge`; the managers list the session after the next
heartbeat; sc-wkr-2 answers `wrong_node`; a tampered ticket is refused; the
same idempotency key returns the same id with `deduplicated: true`; a second
stop answers `stopped: false`.
**Look at:** `legion_worker_edge_session_starts_total` and
`legion_worker_launch_refusals_total`. Workers export metrics only with
`LEGION_WORKER_TELEMETRY_PORT` set, which the rig does not set by default.

### 12.11 Worker restart re-adopts sessions (PR #36)

```fish
for i in (seq 6); legion instance create sessions --task shell >/dev/null; end
legion instance list sessions -j | jq -r '.data.instances[] | "\(.nodeId) \(.instanceId)"' | sort
tests/spot-check/infra/ssh.sh sc-wkr-1 "pgrep -x -f 'sleep 86399' | sort; sudo find /var/lib/legion -name sessions.db"
tests/spot-check/infra/ssh.sh sc-wkr-1 sudo systemctl restart legion-worker
legion instance list sessions -j | jq -r '.data.instances[] | "\(.nodeId) \(.instanceId)"' | sort   # the same
tests/spot-check/infra/ssh.sh sc-wkr-1 "pgrep -x -f 'sleep 86399' | sort"                           # the same pids
```

**Good:** the worker reads `sessions.db` at start, re-adopts the running
session processes (the unit has `KillMode=process`), and reports the same
ids. Nothing restarts.
**Optional, (PR #36 Phase 3):** stop a worker for about 150 seconds and run
`legion instance get <one of its sessions>`: the answer is UNAVAILABLE naming
that node, not NotFound (`smoke.sh sessions-route`, not in the default run).

### 12.12 Destroy

```fish
legion instance destroy ws-1 ; legion instance destroy idle-1
legion volume list --job sandboxes             # ws-1's volume is gone (--all shows it as destroyed)
legion instance create sandboxes --task ws --id ws-1   # refused: ids are never reused
legion job stop sandboxes ; legion job stop sessions
```

## Unattended rerun

`tests/spot-check/smoke.sh` runs sections 1-8, section 12, and the failover
drill, and prints PASS, FAIL, or SKIP per check. `smoke.sh sandboxes` runs
section 12 alone. Section 12 has not been run on the rig yet (written
2026-10-12). On 2026-10-11 the failures it reports were all
real (P0-2, P0-4, P1-1, P1-2) once the harness was fixed.

## Teardown

```fish
tests/spot-check/infra/down.sh       # instances, volumes, groups, bucket, ECR repos
container system stop                # if you started it for section 10
```

## Checklist

| # | Area | Works? | Notes |
| --- | --- | --- | --- |
| 1 | Cluster formation, raft, ACLs | | |
| 2 | Container / exec / microVM over HTTP | | |
| 3 | Logs and exec on all three | | |
| 4 | DAG | | |
| 5 | Full stack on the mesh | | |
| 6 | Agent sandboxes and launcher | | |
| 7 | KV updates | | |
| 8 | Stateful Redis, reschedule and reboot | | |
| 9 | Failover drills | | |
| 10 | macOS local development | | |
| 12.1-12.4 | Sandbox exec, egress, host guard, injection, ingress | | |
| 12.5-12.7 | Suspend/resume local and store, idle and wake | | |
| 12.8 | Memory pressure and preemption | | |
| 12.9 | Session mode: start, burst, list, promote, stop | | |
| 12.10-12.11 | Edge sessions, worker restart re-adopt (PR #36) | | |
