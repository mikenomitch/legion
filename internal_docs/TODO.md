# TODO

The running to-do list for Legion. **Open work is at the top**, grouped by area;
everything already shipped is in [Done](#done) at the bottom.

How to use it:

- Each group links its plan in [`plans/`](plans/) and its public page in
  [`docs/`](../docs/README.md) where one exists. If an item needs a plan and
  doesn't have one, that's noted.
- **When you finish something here, check it off and move it to Done in the same
  change.** A todo that outlives its feature is worse than no todo.
- Items that came out of a plan's "deferred" or "follow-up" section say so, so
  the plan stays the source of truth for the design.

---

- [ ] **Launch blocker: workers on another host lose exec, logs, and the
      mesh, and any task can take over the cluster.** Phases 0 to 8 are
      built ([#32](https://github.com/mikenomitch/legion/pull/32), in
      flight; it replaces the first cut of Phase 1 that shipped in
      [#17](https://github.com/mikenomitch/legion/pull/17)). What is left is
      verification on real hardware, listed below. Before Phase 1 the worker
      plane authenticated nobody (`PostWork`, `ExecTask`, `GetTaskLogs`,
      `RestartWorker`), and Linux containers and `exec` tasks shared the host
      network, so they reached the manager (ACLs off by default), the worker
      port, the cloud metadata service, and other allocations' datapath
      listeners. Found while preparing the pre-launch AWS spot check, which
      ran on a temporary shim (`tests/spot-check/shims.patch`) until Phase 1
      landed. Plan:
      [worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md) (IN EXECUTION)
      · calls: [worker-plane-auth-plan_calls.md](calls/worker-plane-auth-plan_calls.md).
      Phases in ship order, most dangerous gap first:
  - [x] Phase 0: ACLs on by default. The first management token is minted at
        first start into `<dataDir>/bootstrap-token` and `~/.legion/token`
        (`0600`), and the open `BootstrapAcl` RPC and `legion acl bootstrap`
        are removed (plan §6.10). Anonymous denials are now `Unauthenticated`
        with a hint naming the token file (calls entry 17). Docs:
        [auth-and-acls.md](../docs/auth-and-acls.md#quick-start).
  - [x] Phase 1: the mTLS worker plane with per-RPC caller classes, CSR
        issuance, the CA pin in join tokens (v2), reachable bind and
        advertise (`--advertise-addr`, route-detected), two-step CA rotation
        by cross-signing, and the simulator's fleet certificate (plan §6.1
        to §6.6, §6.9; calls 20 to 26). Docs:
        [architecture-managers-workers.md](../docs/architecture-managers-workers.md#the-worker-plane),
        [auth-and-acls.md](../docs/auth-and-acls.md#the-worker-plane).
        The `ticket` class on `ExecTask` shipped with sandbox-exec Phase 2
        (direct exec, below).
  - [x] Phase 2: the host guard in the datapath. Resolve once, judge every
        address, dial what was judged, on the gated egress proxy's three
        paths, external upstreams, and `proxy` handler targets, with the
        worker-type `hostGuard` overrides and `host-guard` in denials,
        `net policy test`, and `net doctor` (plan §6.11; calls 22, 23, 27,
        28). Docs: [network-handlers.md](../docs/network-handlers.md#the-host-guard).
  - [x] Phase 3: Linux containers gated by default (`auto` reads the
        node's `os`), ingress twins (`to` or the advertised number) and a
        `127.0.0.53` resolver for every gated allocation, and the
        `gated-default` warning on `job validate`, `job run`, and `job plan`
        (plan §6.12; calls 29 to 31). `gated-default` and the lower-case proxy
        variables came from the gated-sandbox plan. Docs:
        [task-runtime-container.md](../docs/task-runtime-container.md#networking-gated-by-default-on-linux),
        [network-handlers.md](../docs/network-handlers.md#networkmode).
  - [ ] Run a real gated container end to end on a Linux node (runc or
        crun) with the default mode: a stock `nginx` reachable on its
        advertised port, `curl` through the proxy refused with
        `gated-default`, then allowed after `network.egress.allow`. This
        repository's CI starts no OCI runtime; the placement, port, resolver,
        and namespace pieces are tested separately
        (`tests/gated_container_default.rs`,
        `netns::tests::gated_start_overhead_benchmark`).
  - [x] Phase 4: the encrypted worker-to-manager channel, with the ticket
        JWKS fetched over it (plan §6.8; calls 32 to 34). The manager's API
        port answers TLS for `manager.legion` with its cluster certificate and
        plaintext for the CLI and UI, told apart by the first byte; workers
        always dial TLS pinned to the CA. Docs:
        [auth-and-acls.md](../docs/auth-and-acls.md#workers-reach-managers-over-tls).
  - [ ] Raft peers still dial each other in plaintext, and with `server.tls`
        on the API port drops plaintext, which a multi-manager raft cluster
        needs (`src/storage/raft/network.rs:197`, `driver.rs:934`). Deferred by
        the plan (non-goals); listed in
        [architecture-managers-workers.md](../docs/architecture-managers-workers.md#what-is-not-implemented-yet).
  - [x] Phase 5: the host guard in the kernel: an `inet legion_host_guard`
        table with one chain per allocation leaf, counters reported as
        `host-guard` denials, the `hostGuard` node attribute in
        `legion node info`, and a label-only cgroup leaf for every task when
        enforcement is off (plan §6.13; calls 35, 36). Docs:
        [network-handlers.md](../docs/network-handlers.md#the-host-guard-in-the-kernel).
  - [ ] Run the kernel host guard for real on a cgroup v2 node as root:
        `sudo -E cargo test --lib kernel_guard_loads -- --ignored --nocapture`
        (a dial to `169.254.169.254` from an allocation leaf fails with
        `EPERM` and the counter reports it). Built on a hybrid-cgroup host,
        where only the rendered table was checked against the real `nft`
        (`the_rendered_table_passes_nft_check_test`).
  - [x] Phase 6: the container privilege floor: per-allocation user
        namespaces from a worker-owned id pool, shifted layer extraction,
        idmapped task directory and volumes, a default seccomp profile over
        Docker's blocked set, a cgroup namespace, `/sys` bound read-only
        under a user namespace, the `security` block, and opt-out warnings
        (plan §6.14; calls 37 to 39). Docs:
        [task-runtime-container.md](../docs/task-runtime-container.md#security).
  - [x] AWS spot-check fixes to the floor (Ubuntu 24.04, runc 1.5.2,
        youki 0.7.0): the worker state dir and `allocs/` are traverse-only
        and a user-namespaced allocation's directory is `0710` to its own
        range, so runc can reach the bundle as container root (it failed
        every start with `remount-propagation … permission denied`); the
        default OCI runtime is runc, and youki or runc before 1.2 is refused
        at a user-namespaced start (youki rejects idmapped mounts); `exec`
        and actions no longer pass `--` to runc/crun, which ran it as the
        command (calls 40, 41 here, and
        [container-runtime-exec calls](calls/container-runtime-exec-and-signals-plan_calls.md)
        21). Docs:
        [task-runtime-container.md](../docs/task-runtime-container.md#security),
        [installation.md](../docs/installation.md#runc-the-default).
  - [x] Dropped youki (2026-10-12): Legion runs containers on runc 1.2+
        (the default) or crun only, identified by the `--version` banner,
        and refuses any other runtime at preflight on every start (container
        and Linux dev-mode sandbox), which also withholds `container: pass`.
        The youki-only `--` exec argv is gone, and the spot-check rig no
        longer installs youki or sets `LEGION_OCI_RUNTIME` (calls 40). Docs:
        [installation.md](../docs/installation.md#crun-instead),
        [task-runtime-container.md](../docs/task-runtime-container.md).
  - [ ] Re-run the spot check's container and exec paths on these fixes,
        with no shim: the `0710` group grant was checked by unit tests
        only. By hand, `0711` on both directories was what let runc run the
        bundle as uid 1048576.
  - [ ] Run the privilege floor on crun
        (`LEGION_OCI_RUNTIME=crun cargo test --lib oci_runtime_security -- --ignored`).
        Only runc 1.5 was available when it was built, so crun is
        supported on trust (calls 40).
  - [ ] `container: pass` readiness means the runtime is an installed runc
        or crun, but not that it is new enough. A runc older than 1.2
        advertises it and fails every default (`userNamespace = true`)
        start. Consider a separate readiness key for idmapped-mount support
        so the scheduler can avoid such nodes (calls 40).
  - [ ] An `exec` task with a `user` still can't reach a `local-dir`
        volume's host path (`<state dir>/volumes/…` is owner-only, as the
        whole state dir was before). Volume data dirs are created
        at the umask, so `volumes/` stays the guard until each volume
        guards itself.
  - [ ] Vendor Docker's default seccomp allowlist once CI can run it on every
        runtime (calls entry 37): the shipped profile is a denylist over the
        same blocked set.
  - [ ] Five pre-existing ignored runc tests fail on a hybrid-cgroup host
        before and after the floor (`oci_runtime_exec_runs_inside_the_container`,
        `..._sees_task_dir_files_and_their_rewrites`,
        `..._enforces_a_declared_memory_limit`, `..._stop_force_kills_at_the_deadline`,
        `..._stop_lets_a_trapping_container_drain`). Re-run them on a cgroup v2
        host to tell environment from bug.
  - [x] Phase 7: `exec` trust. Gated `exec` refuses root at submit, the user
        switch drops supplementary groups (`setgroups(0, NULL)`), and
        host-level jobs need `host` on `job:write`, with a refusal naming the
        task (plan §6.15, §6.16; calls 40 to 42). Docs:
        [task-runtime-exec.md](../docs/task-runtime-exec.md#trust),
        [auth-and-acls.md](../docs/auth-and-acls.md#host-level-jobs).
  - [x] Phase 8: reachability diagnostics: `Worker/Probe` from the
        manager that accepts a registration, the outcome on the node
        (`workerPlane`, shown by `legion node info` and the UI node page),
        and `WORKER_UNREACHABLE` on exec, logs, and restart in the CLI and MCP
        envelopes (plan §6.7; calls 43, 44). Docs:
        [architecture-managers-workers.md](../docs/architecture-managers-workers.md#reachability),
        [agent-readiness.md](../docs/agent-readiness.md).
  - [x] Fix the public docs' "cannot be bypassed" claims for gated
        `container` and `exec`: they now describe the host guard, and that a
        root `exec` task can `setns` back out.
  - [ ] `authz` webhooks, artifact fetches, and the worker's own calls are
        not judged by the host guard (calls entry 23). Revisit if `authz`
        ever returns data to the task.
  - [ ] `legion net policy test` judges only the host guard's built-in
        classes, not a node's own addresses or its `hostGuard` entries
        (calls entry 27).

- [ ] **Launch blockers from the AWS spot check (2026-10-11).** Each has
      evidence and a root cause in
      [launch-spot-check-findings.md](launch-spot-check-findings.md) (the IDs
      below are that doc's); rerun with `tests/spot-check/smoke.sh`.
  - [x] **Batch tasks never finish (P0-2).** The worker rebuilds `TaskConfig`
        without `lifecycle` or `restart`, so batch tasks restart forever after
        exit 0 and jobspec restart policies are ignored. Fixed in #17 (Done,
        "Task lifecycle"). The 2026-10-12 rerun saw `fetch` exit once and
        stop.
  - [ ] **Health checks never start for artifact-staged tasks (P0-3),** which
        includes every container image under `containerImageDesugar`. The
        staging thread calls `tokio::spawn` outside a runtime. Shimmed in
        `tests/spot-check/shims.patch`, and the shim is likely the real fix.
  - [x] **DAG ordering is not enforced (P0-4).** The scheduler treats planned
        batch deps as satisfied, and the worker has no gating. Fixed in #17
        (Done, "Scheduler"). The 2026-10-12 rerun held every dependent.
  - [ ] **A DAG never advances past its first step (2026-10-12 rerun).**
        `fetch` exited 0 on the worker, but its allocation stayed `placed`
        and every `success` dependent stayed held. Fixes are in the working
        tree, not yet in a PR:
    - [x] The root cause: `complete_allocation` wrote `task_status` (status
          lane) from its scheduler-lane op, so the ownership guard refused
          every batch completion on a raft cluster. It is now two writes in
          order: the scheduler half marks the row and decides the status,
          and `set_task_status` (status lane) records it. Both forward from a
          follower (`SetTaskStatus` is a new forwardable op). Covered by
          `tests/raft_ownership_guard.rs` and `tests/raft_follower_writes.rs`.
    - [ ] In the `lanes` topology the dependency eval can run before the
          scheduler leader applies the status-lane write, and then nothing
          re-evaluates the job (`docs/multi-raft.md`, "Non-leader write
          redirects and forwarding"). `single` is not affected.
    - [x] A terminal report the manager can't record (row not applied yet,
          forward to the lane leader failed) is handed back in
          `HeartbeatReply.retry_terminated_allocations` and re-sent, instead
          of being logged and dropped (`handle_terminated_allocations`,
          `src/manager/server.rs`, `docs/task-supervision.md`).
    - [x] A failed changeset projection no longer fails the raft state
          machine's apply. openraft treats that as fatal and stopped the raft
          group on any manager a worker streamed from
          (`capture::apply_changeset_with`, `docs/state-store.md`).
    - [ ] The projection error itself: `note_work_changes` queries
          `allocation.job_id`, which does not exist, whenever a job-level
          volume row is inserted or deleted (first submit of a job with
          `volumes`) on a manager with a streaming worker. Being fixed
          separately.
    - [ ] `handle_failed_allocations` has the same log-and-drop shape: a
          failure report whose `remove_allocation_by_id` forward fails is
          lost (seen on the rerun: `forwarded remove_allocation_by_id but the
          target no longer leads the lane`).
    - [ ] Explain the missing sidecar: the plan placed `fetch` and
          `progress`, but only `fetch` has an allocation row.
    - [ ] Rerun `05-dag.ncl` on the 3-manager rig to confirm.
  - [ ] **A resubmitted DAG job's rollout never progresses (2026-10-12
        rerun).** `05-dag.ncl --var run=<new>` started a rollout that logged
        `waiting for batch retire window` every 2 s for 10+ minutes.
    - [x] The root cause: finished old allocations (`complete`/`failed`)
          still counted as old allocations to retire, and with
          `batchKillAfter` unset the batch retire window refused to retire
          any batch allocation, finished or not. Finished ones are now
          retired at once and first; only running batch steps wait
          (`retire_old_allocations_for_group`, `src/manager/rollout.rs`;
          [rollouts.md](../docs/rollouts.md#lifecycle-specific-behavior)).
          Uncommitted on `fix-rollout-retire`, not yet in a PR.
    - [x] The old version's `serve` never started. Not a dependency bug:
          the scheduler places a service that depends on a finished batch
          step in the same group (`dag_batch_success_allows_service_within_group`),
          and so does the rollout path now
          (`rollout_over_finished_batch_steps_retires_them_and_runs_the_new_dag`).
          While a rollout targets a config, `report`'s exit releases `serve`
          through a `rollout_tick`, so a rollout stuck on the same retire
          window froze it. Once the resubmit superseded that version, the
          scheduler and `queue_dependency_progress` skip it by design, and
          the new version's `serve` replaces it.
    - [ ] A config under rollout releases its held dependents through a
          `rollout_tick`, so a rollout that is paused, waiting for a manual
          advance, or stuck on anything else freezes the DAG along with it.
          Decide whether a dependency release should place through the
          rollout regardless of its step gating
          (`Store::queue_dependency_progress`).
    - [ ] Rerun `05-dag.ncl` twice with different `run` values to confirm.
  - [ ] **A `durability = "store"` checkpoint never resumed on another node
        (2026-10-12 rerun).** The part fetch went through the artifact
        fetchers with no credential (an unsigned GET), so the private
        bucket answered 403, though the publisher had used the
        `contentStore.auth.kv` credential.
    - [x] A part URL under the restoring node's own `contentStore.url` is
          now fetched with that node's `contentStore.auth` (KV credential
          read at fetch time, or the ambient AWS chain via the new
          `Credential::AwsAmbient`). Any other URL stays anonymous and its
          failure says the node has no credential for that store
          (`ArtifactStager::fetch_sealed_into_store`,
          [artifacts.md](../docs/artifacts.md#publishing-content),
          [calls #63](calls/launch-spot-check-fixes-plan_calls.md)).
          Unit-tested, not yet re-run on the rig.
    - [x] With the fetch fixed, the restore failed in cloud-hypervisor with
          `Cannot open disk path`: the saved state names each disk by its
          device link, and links lived under the worker-id-keyed
          `<data dir>/workers/<id>/allocs/.vmdev/`, which no other node
          has. Links now live at `<data dir>/vmdev/<key>/`, the launch
          records the link dir in the checkpoint, and a cloud-hypervisor
          restore rewrites its `config.json` disks to this node's links
          ([calls #1, #2](calls/sandbox-snapshots-and-fast-start-plan_calls.md),
          [docs](../docs/task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored)).
          Unit-tested with fake VMMs, not yet re-run on the rig.
    - [ ] A published Firecracker checkpoint still cannot move between
          nodes with different data dirs: its `vm-state` cannot be
          rewritten and `PUT /snapshot/load` has no drive override. The
          restore is refused by name. Revisit if Firecracker adds drive
          overrides, or if placement should prefer matching data dirs.
    - [ ] The same restore logged `Resuming a restored gated sandbox before
          its egress proxy socket is bound` (5s wait for
          `<alloc>/.sandbox/vsock.sock_1080`). The paths the daemon binds
          and the restore waits for agree, so the cause is unknown. If the
          allocation was not actually gated, the next item explains it.
          Check `legion net doctor` and the socket during the rerun.
    - [ ] Every microVM sandbox has a vsock (for exec), so every checkpoint
          records `network = gated` and every restore sets
          `RestoreTarget.vsock`. A non-gated (TAP or loopback-only) sandbox
          therefore waits the full 5s `DATAPATH_WAIT` and warns on every
          restore, and the gated-vs-TAP refusal in `check_restorable` never
          fires. Derive both from the allocation's network mode.
    - [ ] Rerun the store suspend and cross-node resume on the rig.
    - [ ] Placement ignores which `contentStore` a node has, so a resume can
          land on a node pointed at another bucket and fail. Prefer or
          require nodes whose `contentStore.url` covers the part URLs.
    - [ ] A node has no way to read a store other than its own (no per-store
          read credentials, and cluster `artifacts.auth` never reaches part
          URLs). Decide whether cross-store restores are worth a surface.
  - [ ] **One manager down strands every worker (P0-5).** Followers fail to
        forward heartbeats in the `single` topology (lane mismatch), and
        workers know one manager.
  - [ ] **Recovered nodes stay unschedulable (P0-6)** until `node uncordon`.
  - [ ] **Stock nginx and Postgres images fail in Linux containers (P0-7):**
        minimal capabilities, ownership dropped on extraction, root-owned
        stdout/stderr. The repo's `container-service.ncl`, `fullstack.ncl`,
        and `postgres.ncl` cannot run on Linux.
  - [ ] **Workers cannot restart after their join token expires (P0-8)**, which
        conflicts with the docs' advice to keep join tokens short-lived.
  - [ ] **P1s:** resource enforcement never turns on; static ports deadlock
        placement, reschedules, and rollouts; reschedules exclude the only
        viable node (and advise deleting healthy data); gated netns race;
        orphaned tasks after a worker re-join; allocation id reuse;
        misleading job status; failure causes buried.
        Details in the findings doc.
  - [ ] **P2s:** eleven papercuts (error mapping, `kv get` on a missing key,
        README commands, follower write hints, startup log noise, and more).
        Details in the findings doc.

- [ ] **Pre-launch spot check findings (2026-10-11), from reading the code
      while preparing the AWS run.** Each is verified in source unless marked
      otherwise. The fixed ones (README quickstart, `redis.ncl`,
      `action run -p`, the guest kernel docs, `WorkerRegister` on a
      follower, `tls-edge.ncl`, and the dev guide's `:4748`) are in Done,
      "Pre-launch spot check (2026-10-11)".
  - [ ] **The installation guide's release versions are placeholders.**
        `docs/installation.md` names cloud-hypervisor and guest
        kernel release tags that were not checked against the upstream
        release pages (GitHub was unreachable when it was written). Confirm
        the tags and asset names before launch.
  - [ ] **No exec into microVM sandboxes.** Documented
        (`docs/task-runtime-sandbox.md`, "Current Scope and Limitations"), but
        it is the headline agent-sandbox use case. macOS dev mode can exec, so
        a demo that works locally fails on Linux.
  - [ ] **No way to reach a microVM sandbox over HTTP** without a hand-built
        TAP device, guest IP config, and host DNAT. Gated mode has no
        host-to-guest ingress relay (`src/worker/daemon.rs:1934-1936`, gated
        sandboxes never get a netns).
  - [x] **The worker never uses TLS to reach the manager.** Fixed by
        worker-plane-auth Phase 4; the doc claim is gone. Raft peers dialing
        `http://` stays open above.

- [ ] **Follow-ups from the 2026-10-11 spot-check fixes**
      ([#17](https://github.com/mikenomitch/legion/pull/17);
      [plan](plans/launch-spot-check-fixes-plan.md) §8.7 ·
      [calls](calls/launch-spot-check-fixes-plan_calls.md)). Each was
      deliberately deferred or found along the way.
  - [ ] **Run the sandbox spot check** (RUNBOOK §12,
        `tests/spot-check/smoke.sh sandboxes`; added 2026-10-12, never run).
        Covers microVM exec, egress and the host guard, credential
        injection, suspend/resume (local and store), idle and gateway wake,
        memory pressure, preemption, and session mode. Needs `legion-guest`
        on the workers and the config changes in RUNBOOK §12.0. The steps
        marked PR #36 need the session-durability work.
  - [ ] **Verify the sandbox clean stop on the KVM rig.** The P0 volume
        data-loss fix (guest stop request, flush before kill, unmount in
        init) is unit-tested only. Re-run `tests/spot-check/jobs/09-sandboxes.ncl`
        task `ws`: write `/workspace/probe`, `instance stop` + `start`, and
        the smoke `sandbox-suspend` boots count, on cloud-hypervisor and
        Firecracker ([calls](calls/sandbox-clean-stop_calls.md),
        [docs](../docs/task-runtime-sandbox.md#stopping-a-sandbox)).
  - [ ] **A guest without an agent still loses unflushed writes on stop.**
        The stop request and the flush both go through the guest agent
        (calls 1). Firecracker's `SendCtrlAltDel` could reach init without
        it on a guest booted with an API socket. Worth it only if agent-less
        guests turn out to be common.
  - [ ] **`SIGINT`/`SIGQUIT` as `stop.signal` reach a sandbox command
        ignored** (background job, calls 3). A shell-script workload cannot
        act on them. Documented. A launcher that resets dispositions would fix
        it.
  - [ ] **Re-run the AWS spot check without the shims**
        (`tests/spot-check/smoke.sh` and the RUNBOOK human pass on the
        `spot-check/launch` branch). Nothing in #17 ran on the real rig.
        Smoke-test edge-boot speculative dispatch in particular: the
        dispatcher read `protocolVersion` from the wrong attribute map, so
        it had never fired for a real worker before #17.
  - [ ] **`move_count` is counted per task, even for keyed tasks.** The
        reschedule backoff runs per instance (2026-10-12 re-run fixes, in
        Done), but the job-failing budget still counts every move of the
        task inside `move_period`, so with `move_count` set, a few
        unrelated sandboxes failing fails the whole keyed job. A per-instance
        budget that fails (or stops) just the instance is the likely shape
        (`Store::remove_failed_allocation` in
        `src/storage/store/reschedule_holds.rs`).
  - [ ] **A worker-reported failure leaves no `last_stop` on the
        instance.** The failure path removes the allocation with
        `StopCause::Unknown`, which records no `last_stop`, so once the
        reschedule hold lapses the instance row no longer says why its last
        allocation ended. A `failed` stop cause carrying the reason would
        close it (`remove_allocation_row_tx`).
  - [ ] **Scheduler benchmark numbers for the reschedule holds.** The
        holds add one indexed `reschedule_hold` read per `Reschedule` plan
        and a subquery to the keyed binder's ordering. AGENTS.md asks for
        before/after `scheduler_bench` numbers on scheduler changes; they
        were not taken in the change that added them. (The pinned-storage
        disk exemption that followed was measured: `scheduler_speed` and
        `restore` unchanged within noise.)
  - [ ] **The disk-fit check may read the wrong disk.** The kitchen-sink
        fingerprint writes `disk_available_space` once per disk it lists, so
        the node reports whichever disk came last (`/boot/efi` on some
        hosts), not the one holding the worker's volumes
        (`src/worker/node_check/node_checkers/kitchen_sink.rs`). It may be
        why the rig's sc-wkr-1 failed the check for a 2G volume while 12%
        reserved (unconfirmed: a full root disk would do the same). Report
        the free space of the disk holding the state dir instead.
  - [ ] **A rollout's replacement skips the previous version's backoff.**
        A hold is keyed by job config, so when an instance fails on the old
        version and a rollout then places the new one, the new version's
        plan does not see the hold and places at once
        (`Store::held_replacements_for_job_config`). Harmless for a fixed
        config, but a crash-looping task gets one undelayed retry per
        rollout.
  - [ ] **A failed changeset projection drops that changeset's capacity
        events.** The recovery rebuilds the read model and resyncs work
        streams, but blocked evals waiting on the freed capacity wait for the
        next capacity event (`Store::project_committed_changeset`).
  - [ ] **Followers never publish a config snapshot.** P2-12 fixed the
        Managers page roster, but a manager still writes its config row only
        when it leads the control lane (`upsert_manager_config` in
        `src/manager/daemon.rs`), so a manager that has never led has no
        config page and shows `—` in the Config column. Fix by forwarding
        the write to the leader (`ForwardableLaneWrite`) from every manager
        at startup. Rows written before P2-12 under the old `host:port` key
        linger until the store is reset
        ([calls](calls/managers-page-raft-members_calls.md) 3).
  - [ ] **Capability-aware placement.** A task that adds capabilities places
        like any other and fails at start on a node whose
        `container.allowCaps` refuses them; authors pin such tasks by pool
        (calls 1). The per-author `host` grant shipped with worker-plane-auth
        Phase 7, but a capability `add` inside the user namespace does not
        need it, so the allowlist still bounds the node, not the author.
  - [x] **User namespaces and seccomp matter more now** that containers get
        Docker's 13 default capabilities instead of 3: both shipped as the
        worker-plane-auth Phase 6 privilege floor
        ([task-runtime-container.md](../docs/task-runtime-container.md#security)).
  - [ ] **On Linux `command` replaces the image `ENTRYPOINT`; on macOS it
        does not**, so one jobspec runs different commands per OS (calls 51).
        Needs a plan.
  - [ ] **Pre-vote for partitioned raft members.** A restarted member now
        starts quiet (calls 17), but a member cut off by a partition still
        deposes the leader when it heals; openraft 0.9 has no pre-vote.
  - [ ] **Workers fail over to raft advertise addresses**
        (`HeartbeatReply.manager_addresses`), which must be reachable from
        workers. Consider serving each manager's `server.advertiseAddr`
        instead (calls 7).
  - [ ] **Check the ticket `iss` a worker expects across managers
        (unverified).** `build_jwks_verifier` in `src/worker/daemon.rs`
        requires `iss` to equal the join token's manager address, while each
        manager's issuer defaults to its own listener address
        (`WorkloadIdentityState::initialize`) unless `workloadIdentity.issuer`
        is set. If that holds, boot and exec tickets minted by any other
        manager are rejected, failover or not. Found while fixing calls 53.
  - [ ] **Fold the partial-placement remainder into blocked evals.** A plan
        that places nothing blocks on main's blocked evals, but the unplaced
        remainder of a partial plan still waits for the free-triggered
        capacity pass (`capacity_waiters`), so there are two retry paths and
        `job info` shows `blocked` only when nothing placed (calls 21).
  - [ ] **A `legion_leader_forwards_total` metric** for follower-to-leader
        write forwarding (calls 3).
  - [ ] **Terminal batch reports are not persisted.** A worker restarted
        between a batch exit and the heartbeat that reports it cannot know
        the exit status, so the completed step is rescheduled and runs again.
  - [ ] **`TerminatedAllocation.success = false` is never sent**: every
        unsuccessful terminal exit takes the failure (reschedule) path,
        including `restart.policy = "never"`. Route `never` through it or
        drop the field.
  - [ ] **`legion exec` into a gated `exec` task runs in the host network
        namespace** (`ExecOptions` has no namespace field), and a pushed
        gated task's datapath listeners bind only at the next heartbeat.
  - [ ] **Removed-allocation history and worker log retention disagree**:
        the manager forgets a removed allocation after 24h, the worker keeps
        its logs 7 days. Add a `--node` override or a configurable retention.
        The UI Logs tab never sets `include_removed`.
  - [ ] **Destructive static-port rollouts race the old process's graceful
        stop**; the new process may fail to bind once and fall back on its
        restart policy. A worker can also stop an unwanted allocation and
        start its static-port replacement in the same tick.
  - [ ] **No-op `rollout_tick` evals still emit eval events** (backoff caps
        them at about two every 30s).
  - [ ] **`batch_task_outcome_tx` compares against the spec `count`**, not a
        `job scale` override.
  - [ ] **Image pulls**: the `container` runtime's extracted rootfs is not
        cached (every new allocation re-extracts. Sandbox root disks are, in
        the rootfs base cache below), ORAS layers are not cached, a hung pull
        blocks other starts of the same image (oci-client has no read
        timeout), and oci-client's bundled webpki roots cannot pull through
        a TLS-intercepting proxy (`rustls-tls-native-roots` would). On
        macOS the pull happens inside the task process, so a pull failure is
        reported as an exit with stderr rather than a staging failure.
  - [ ] **Supplementary groups from the image's `/etc/group` are not
        applied** in Linux containers.
  - [ ] **CLI errors print twice**: `#[instrument(err)]` on command handlers
        logs each human-mode error as ERROR trace lines before `Error:`.
  - [ ] **Event metadata keys are still mixed case**; the camelCase rule now
        covers API messages and CLI envelopes, not event metadata.
  - [ ] **The https CA default (`configs/tls/rootca.pem`) is relative to the
        working directory**, like the manager config path was.
  - [ ] **`legion job info` prints only the job row**, not its allocations.
  - [ ] **Not verified on real hardware**: cgroup v2 resource enforcement
        turning on (P1-1), and the worker-plane throughput cost of TLS
        dispatch at 1,000 nodes.

- [ ] **Architecture audit (2026-10-09).** Design-level findings across the
      store, scheduler, manager API, worker, and lifecycle, each with an ID and
      a status line: [architecture-audit-2026-10-09.md](architecture-audit-2026-10-09.md),
      calls in [architecture-audit-2026-10-09_calls.md](calls/architecture-audit-2026-10-09_calls.md). The fix PR
      ([#6](https://github.com/mikenomitch/legion/pull/6)) fixes 25 of 31 in full, 5 in part, and leaves SC7 as
      deliberate. What it left open is listed under each area below, tagged
      "(audit <ID>)". Check this off once the PR merges.

- [ ] **The `fit` index does not see `maxAllocations`.** A node at its
      allocation cap still projects into a fitting (cpu, memory, disk)
      cell, so under `bin_pack` (tight order: fullest fitting cell first)
      capped nodes rank at the FRONT of a truncated candidate list and
      spend budget on nodes the ledger then rejects; the plan pays a
      `refills` scan when the list runs dry. Fix shape: project the
      `allocations` pseudo-resource as a fourth dim (or a cap flag that
      moves the node to the non-fitting tail) so the ordering-not-filtering
      invariant holds. Needs the `dense_fleet` fill-ladder re-measure
      (Scheduling & placement) to size it.

- [x] **Batch the control-lane job submit** (the "fuse the five control-lane
      rounds" item, right-sized). The two `clear_*` rounds were already
      empty changesets; what remained was one `handle_job_submit` commit
      (and `synchronous=FULL` fsync) per job. Now group-committed with
      versions derived through the batch transaction, which also fixes the
      code's own commented version race for concurrent submits. Measured
      locally: per-job quorum rounds ~3.0 -> ~1.5, unit-eval throughput at
      the knee 419.6 -> 476/s. Remaining singles at ~0.5/job are
      arrival-gaps, not a defect.
- [x] **JSONB columns carry NUMERIC affinity, so every JSON write parses as
      a number first — fixed** (2026-08-06,
      [jsonb-text-affinity-plan.md](plans/jsonb-text-affinity-plan.md),
      doc in [state-store.md](../docs/state-store.md#column-affinity)).
      All 46 JSON columns across 17 tables rebuilt to TEXT affinity in one
      migration that preserves column order/count/rowids exactly — the
      load-bearing property, since raft changesets bind columns by
      position and a reordering rebuild would corrupt a mixed-version
      cluster mid-rolling-upgrade. `job_config` (the one CASCADE parent)
      gets a rename dance, with `job_config_submission`,
      `job_kv_reference`, and `job_update` rebuilt byte-identically purely
      to re-point their FKs. Local A/B on the raw JSON insert path: the
      NUMERIC twin is ~5-10% slower; before/after Criterion cells in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
- [ ] **`STRING` and `TIMESTAMP` columns still carry NUMERIC affinity** —
      deliberately excluded from the fix above. Same wasted parse on
      every id/timestamp bind, but for these the conversion can
      *succeed*: numeric-looking ids and unix-seconds timestamps are
      stored as INTEGERs today, so a re-type changes what future writes
      store while existing rows stay INTEGER, and INTEGER never
      compares equal to TEXT — a value-rewriting migration with its own
      risk budget, not a type rename. See
      [jsonb-text-affinity-plan_calls.md](calls/jsonb-text-affinity-plan_calls.md)
      entry 2.

## In flight (open PRs)

| PR | What it lands | Notes |
| --- | --- | --- |
| [#44](https://github.com/mikenomitch/legion/pull/44) | Swappable seams: one `Attachment` type for how an allocation reaches the datapath, a typed worker ↔ netd boundary (`Netd`, `NetdInput`/`NetdWants`/`NetdReport`), one listener and dialer, one object-storage layer, pluggable cluster CA providers (`builtin`/`file`/`exec`), DNS on hickory-proto and the egress proxy on hyper, plus the mesh-upstream, VIP-by-node-OS and pipelined-request fixes | [plan](plans/swappable-seams-plan.md) · [calls](calls/swappable-seams-plan_calls.md) · [CA plan](plans/cluster-ca-provider-plan.md); the `legion netd` process split and the rest deferred below |
| [#162](https://github.com/mikenomitch/legion/pull/162) | Lanes hardening: worker-write forwarding, split-tolerant submits, adversarial fixes | Plan §8 + calls doc in [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) |
| [#143](https://github.com/mikenomitch/legion/pull/143) | Note the node-level builtin resource override gap | Docs only; the fix itself is a todo below |
| [#84](https://github.com/mikenomitch/legion/pull/84) | Instrument DB hot paths, batch task-status updates | Old; rebase or close |
| [#108](https://github.com/mikenomitch/legion/pull/108) | Versioned job templates | Superseded by [#151](https://github.com/mikenomitch/legion/pull/151) (merged) — close |

---

## Scheduling & placement

- [x] **Per-eval planning cost no longer grows with node count
      (local; AWS retest pending).** Phases 1-2 of
      [plan-path-fleet-scaling-plan.md](plans/plan-path-fleet-scaling-plan.md)
      (2026-10-09, uncommitted on `bench/aws-comparison-refresh`): the
      read model's snapshot maps are persistent (`imbl`), so the capacity
      snapshot is O(1) and carries the fence epochs, which retires the
      lazy/eager sample and its re-plans (86-96% of concurrent plans in
      the bench); index-served candidates are verified during a budgeted
      walk instead of across the whole shard. Concurrent per-plan cost in
      `plan_fleet_scaling` is now flat from 5k to 10k nodes (5.9 → 3.8 ms
      and 12.7 → 3.7 ms). Public docs: scheduler-and-eval-loop.md,
      scheduling-indexes.md, metrics.md, testing-and-benchmarks.md.
- [x] **Confirm the fleet-scaling fix on AWS** (plan Phase 3, 2026-10-09):
      10k `web_fleet` 163 → 322 alloc/s, 5k 504 → 736, dense 5k
      1,983-719 → 2,401-1,049 alloc/s across the sweep. Numbers in the plan.
- [x] **Per-eval O(fleet) and O(jobs) residuals removed, and the whole
      eval is instrumented** (plan Phase 4, 2026-10-10, uncommitted): the
      available-node set and its by-id lookup are cached as one shared
      `Arc` (no per-plan `node_lookup` map or list clone; rebuilt off the
      write lock only after a node change); the eval path's O(jobs) SQL
      scans got indexes (`job_update.to_job_config_id`,
      `job_config(name, version)`; the plan apply's own-usage read rides
      the `job_config(job_id, created_at)` index added alongside); dispatch
      and batch follow-ups read nodes and task configs from the read model
      instead of SQLite; hot eval-path statements are `prepare_cached`.
      New metrics `legion_eval_phase_seconds` (incl. blocking-pool
      `queue_wait` and `conflict_backoff`), `legion_eval_queue_round_seconds`,
      `legion_store_read_model_lock_wait_seconds`,
      `legion_scheduler_node_list_rebuilds_total`, plus plan phases
      `job_lookup` / `speculative_dispatch` / `post_apply`; the AWS collector
      scrapes them. `plan_fleet_scaling` covers 20k/50k nodes and an
      `eval_cycle` cell. Numbers in the plan and
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
- [x] **Leader-side O(jobs²) scans and lost-node under-placement fixed**
      (plan § 13.6, 2026-10-10, uncommitted): `job_config(job_id,
      created_at)`, `job_config(job_id, version)`, and
      `rollout(to_job_config_id)` indexes (the status checker's
      `jobs_with_unrolled_latest_config` went 60 s → 31 ms at 16.7k jobs, and
      10k `web_fleet` 322 → 537 alloc/s on AWS); cached heartbeat statements;
      lost-node replans now retry plan conflicts and hand off to a
      `job_reschedule` eval instead of leaving the job short
      ([calls](calls/plan-path-fleet-scaling-plan_calls.md) #15).
- [ ] **Job stop/destroy at scale drains at ~4 evals/s** (AWS 10k cleanup,
      2026-10-10). Profile with `legion_eval_phase_seconds{kind="job_stop"}`.
- [x] **Re-ran `web_fleet` on AWS with Phase 4 and adaptive cadence**
      (2026-10-10, build `ac024750`): 829 / 789 / 905 / 738 / 553 / 327
      alloc/s at 1k / 2k / 5k / 10k / 20k / 50k nodes, against 733 / 669 /
      736 / 322 / ~85 before. Eval `queue_wait` is now ~0.3 ms, so the
      blocking-pool contention is gone. Public numbers in
      [docs/benchmarks.md](../docs/benchmarks.md).
- [x] **`RunJob` no longer parks tokio workers.** The submit is a chain of quorum writes, and `RunJob` ran
      it inline on a runtime worker, the same mistake the heartbeat offload
      fixed. With 128 submits in flight on a 4-vCPU leader, every worker sat
      in a commit, so each eval worker's `spawn_blocking` await waited for a
      thread to resume on. The archived 2026-10-10 cells show it as eval
      handle time outside every phase: 14 ms at 1k nodes, 16 ms at 5k, and
      60-78 ms at 10k while submits were in flight, against a 12-22 ms plan.
      Heartbeat p99 rose from ~2 ms to 145-900 ms in the same windows. Now
      `RunJob` and `RunJobTemplate` run the submit under `block_in_place`
      (`run_blocking_admission`). On a local 3-voter `lanes` cluster with 4
      runtime workers per manager, alternating builds: 1k nodes 514 / 478 /
      661 → 1,159 / 964 / 1,160 alloc/s, 5k nodes 714 / 503 → 860 / 1,049.
      Eval handle time now matches the plan phase, the `job_submit`
      coalescer batches ~16 instead of ~1.1, and `RunJob` latency rises
      (40-125 → 140-225 ms) because submits now queue in the coalescer
      instead of in front of the runtime. On the AWS rig (2026-10-14, PR
      #40) `web_fleet` went 680 → 1,261 alloc/s at 1k nodes, 640 → 1,067 at
      2k, 635 → 945 at 10k, and 1,175 → 968 at 5k (see the open item
      below).
- [ ] **`web_fleet` at 5k nodes got slower with the `RunJob` offload**
      (1,175 and 1,151 → 968 alloc/s on AWS, one run), even as its p99
      improved (25s → 17.5s) and every other fleet size got 49-85% faster.
      Submits now arrive 3-4x faster, and heartbeat freshness dipped during
      the bursts (0.20 average at 1k). Find out whether faster admission
      changes the plan coalescer's batching at 5k or crowds the heartbeat
      path, before tuning anything.
- [ ] **Other mutating handlers still commit on a runtime worker.**
      `StopJob`, `DestroyJob`, the KV writes, and most other mutating RPCs
      call the store inline like `RunJob` did. None of them is hot in a
      benchmark today, but a burst of any of them starves the eval loop and
      heartbeats the same way. Move them behind `run_blocking_admission`
      or `spawn_blocking` as they show up.
- [ ] **`legion_runtime_spawn_delay_seconds` can't see the starvation it
      exists for.** The probe spawns its task from a task already running on
      a worker, so the probe lands in that worker's LIFO slot and is polled
      as soon as the spawner yields. It read under 50 µs throughout local
      runs where heartbeats averaged 90 ms. The delay that matters is the
      ticker task's own wake. Measure from the tick's deadline instead
      (`src/manager/telemetry.rs`, `start_runtime_probe`).
- [ ] **The rig's plan phase is ~25x the micro-bench.** On AWS
      `legion_eval_phase_seconds{phase="plan"}` averages 26 ms per
      `job_submit` eval at 20k nodes and 45 ms at 50k, against ~1 ms for
      `plan_and_apply` in `plan_fleet_scaling`. The leader sits at ~2 of 4
      cores, so this is now the throughput ceiling past 10k. Break it down
      with the plan sub-phases (`job_lookup`, `speculative_dispatch`,
      `post_apply`) and a perf profile at 50k before guessing (plan § 13).
      First read of the 50k archive: `apply` is ~82% of plan time and
      leader CPU is ~2 of 4 cores, so this is serial commit work, not a core
      shortage. The two items below are the follow-ups.
- [ ] **Make plan-commit cost flat in fleet size.** The slowest coalesced
      commit lane grows 9.2 → 13.5 → 23.3 ms at 5k / 20k / 50k nodes for
      the same size of plan, so something inside the commit transaction
      scales with node count. Suspects, none verified: the free-pool
      rebuild inside the apply commit, the read-model reprojection, or
      heartbeat writes sharing the lane. Profile the commit path at 50k to
      find out. This is the biggest throughput lever past 10k nodes, and it
      likely explains most of the plan-phase item above.
- [ ] **Batch more plans per commit.** Group commit averages ~1.75 plans
      per commit at every fleet size, so there is room. The rig runs 4 eval
      threads. Phase 2 found 16 threads cost ~11% because extra planners
      collided, but plan conflicts are now zero, so re-measure with one
      `run-campaign.sh --system legion-a --eval-threads 8` campaign at 20k
      (50k cleanup takes ~2h) and compare plans per commit and throughput.
- [ ] **`sync_missing_task_volume_claims` still runs one task-config query
      per active job per checker tick** on the scheduler leader (86 ms at
      16.7k jobs locally with the new index, 9.5 s without). Prefilter in
      SQL to jobs whose latest config declares volumes. Left alone in
      Phase 4 because the status-checker loop was being changed in parallel
      (plan § 13).
- [ ] **In-place rollout ticks build an O(fleet) exclusion set**
      (`src/manager/rollout.rs` `available_node_ids` → "every available node
      except the old ones"), which also forces the plan's filtered node-set
      copy. Express it as an allow-list instead (plan § 13).
- [ ] **Truncation-ineligible tasks still scan the fleet** (`count.per`,
      `distinct`, volumes, multi-task groups): a deliberate non-goal of the
      fleet-scaling plan, open as its own follow-up.
- [ ] **Retire `legion_scheduler_lazy_sample_fallback_total`** and the
      `lazy_version_sample` key one release after the fleet-scaling
      change ships. Both are inert now (docs/metrics.md).
- [ ] **Find what makes `apply` 27.9 ms/eval.** Phase 3 measured
      plan-apply conflicts at **0%** (from 31.9%) with throughput
      unchanged at 80.35/s, so the ceiling is the replicated write path,
      not planning. `apply` is 67% of plan time, and 41.42 ms/eval over 4
      eval threads permits ~580 alloc/sec against 80 measured — so evals
      are far more serialized than 4-way. Prime suspect: the scheduler
      lane's single `pipeline_loop` thread. Collect
      `legion_store_write_lock_wait_seconds` and the lane pipeline
      metrics next campaign. See
      [aws-phase3-findings.md](aws-phase3-findings.md).
- [ ] **Warmup is longer than a small cell.** `run-campaign.sh` defaults
      `WARMUP=30`; phase 3's 1,000-node cell placed 5,004/5,004 and
      reported `throughput_per_sec: 0.0` with `steady_window_placed: 0`
      because every placement fell inside the warmup. Scale warmup to
      cell size, or floor the steady window. This is why phase 3 has no
      1,000-node throughput number.
- [x] **Read-connection pool: fixed, measured, and ruled out as the
      ceiling.** Steal-on-contention, cached heartbeat task configs, and a new
      `legion_store_read_lock_wait_seconds` (read-connection wait
      previously had **no metric at all**). The hypothesis that this was
      the ceiling was **refuted three times**: phase 3 measured
      `task_fetch` at 0.65 ms at 5,000 nodes on the *unmodified* binary, so
      the 107x growth seen in the phase-2 archives was coupled to the
      conflict rate, not to fleet size; the plan path was traced in code and
      holds **no** read connection across a quorum round, an `.await`, or
      `build_plan` (the apply runs on the write connection inside
      `replicated()`), so the pool cannot serialize two planners; and the
      2026-08-05 eval-thread sweep ran every cell with **four** connections
      and still got 251.68 alloc/s at 32 eval threads, up 14.8% on 16. The
      "~4 effective planners" figure that started the hunt was a metrics
      error — `plan_phase_seconds{total}` times planning only, so Little's
      law over it counts evals in the planning *stage*, not busy workers.
      Use `eval_handle_seconds`. See
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      entry 5.
- [x] **Take the read-connection pool off `available_parallelism()`.**
      Now `evalThreads + 4`, capped at 16. **No throughput claim** — the
      sweep above shows four connections were adequate through 32 eval
      threads, so this is a correction to the sizing *argument* plus a
      bounded hedge for the higher `evalThreads` values the sweep says
      operators will want. Core count was the wrong basis: the readers are
      latency-bound (8% CPU at saturation, and a page-cache miss holds the
      mutex through a `pread`), holds are uneven
      (`cache_refresh_allocation_scopes` keeps one connection across ~44
      per-node queries after every apply, on every replica), and a pool
      sized at the core count saturates in lockstep with the tokio runtime
      by construction, since the heartbeat body reads inline on runtime
      threads.
      **A config knob was deliberately not added**: `evalThreads` is the
      concurrent-reader count we already have, and a `readConnections`
      field would be a number no operator can size from first principles.
      The 16 cap is a memory budget — `SQLITE_CACHE_SIZE_KIB` is 64 MiB per
      connection and the caches do not share, so the cap holds the pool's
      worst case at 1 GiB (unit-tested).
- [x] **Cut the raft round-trips per eval** —
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md).
      `Store::complete_and_claim_next` fuses the eval loop's terminal
      write with the following claim (4-5 rounds/eval → 3-4), and
      `Store::enqueue_evals` makes every fan-out one round instead of one
      per row (leadership regeneration, drained and lost nodes). **The
      AWS re-measurement is NOT run** — see the open item below.
- [x] **Measure the eval-path raft cuts at 5,000 nodes.** Done 2026-08-05,
      but only after the rig itself was fixed — see
      [aws-phase4-findings.md](aws-phase4-findings.md). The benchmark driver
      spawned one CLI process per job from a pool of 4 on a 2-vCPU box, which
      capped submission at ~36 jobs/sec and meant every earlier cell reported
      the DRIVER's rate. Results on a rig that reports
      `submission.driver_bound: false`: **177.27 alloc/sec** at 4 eval
      threads, **219.25** at 16, and **736.1** on `dense_fleet` (44
      allocations per eval), all at 5,000 nodes on three 4-vCPU managers.
- [x] **Reprojection no longer reads a node's whole allocation list**
      (2026-08-06, phase 5 of
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md)).
      The read model keeps a per-node `NodeUsage` aggregate beside each
      cached allocation list — the in-memory sibling of `node_alloc_usage`,
      same type, same equivalence tests — so `reproject_model_nodes` is a
      subtraction over resource dims instead of one JSON normalization per
      allocation on the node. The delta path adjusts the memo with exactly
      the rows it spliced; every wholesale list reload (scope refresh,
      scrub, drift fallback, node removal) drops the entry and the next
      reprojection rebuilds it from the list, so an absent entry costs
      precisely the old walk. The 44 per-scope epoch reads became one read
      connection + one cached statement. Local A/B
      (`bench_local_reprojection_by_fill`, 44 touched nodes): 414µs → 50µs
      at 10 allocations/node, 1.92ms → 74µs at 40 — the memo arm grows
      1.5x over a 4x fill change where the walk grew 4.6x. Numbers in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md);
      calls in
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §3. **The `dense_fleet` ladder re-run is the regression test** — see
      the open re-measure item below.
- [x] **Job intake group-commits on the scheduler and status lanes**
      (2026-08-06, phase "intake" of
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md),
      documented in
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md)).
      A submitted job cost THREE quorum rounds, one per lane, and all
      three sat on top of the AWS 5,000-node profile in lockstep with the
      submission rate: `enqueue_eval_after_delay` 19.1 applied/s,
      `handle_job_submit` 16.6/s, `seed_task_statuses` 16.6/s, against
      21.9 evals/s overall — while the eval loop's own
      `complete_evals_and_claim_next`, already batched, sat at 4.2/s.
      Concurrent submitters now coalesce their scheduler-lane enqueue and
      their status-lane `task_status` seed through the existing
      `coalesce.rs` group commit, each request fenced by its own
      savepoint so one bad submission fails alone. No timer, so a lone
      `job run` on an idle cluster is byte-for-byte the path it was.
      `schedulerSettings.job_intake_coalescing = false` takes both back
      out. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §2. **Unverified at scale** — see the re-measure item below.
    - [ ] **The control-lane `handle_job_submit` is NOT batched, on
          purpose.** It derives a config's next version from committed
          state read through a read connection, so two submits of the
          same job inside one uncommitted batch transaction would both
          compute version N+1 — the "there is a race condition here with
          the version" note in `handle_job_submit_apply`, promoted from
          rare to routine. The composite is also not one store call:
          `submit_rendered_job` interleaves `clear_job_stopped_status`,
          `replace_job_kv_references`, `seed_keyed_instances`,
          `previous_job_config_id` and rollout creation between the
          control write and the eval enqueue. Batching it means making
          the submit a pure function of its request first — a rewrite of
          the RPC, not a storage change. Intake therefore still costs one
          quorum round per submitted job.
    - [ ] **Re-measure intake with batching on.** Same cell as the
          measurement above (5,000 nodes, `dense_fleet` at 200,000
          allocations, 16 eval threads, 32 concurrent submitters).
          `legion_raft_ops_total{op="enqueue_evals_isolated"}` and
          `{op="seed_task_statuses_batch"}` should be well below the
          submission rate, and
          `legion_store_coalesce_batch_size{path="eval_enqueue"}`'s
          `le="1"` bucket well below its `_count`. **A local run cannot
          measure this** — `Store::replicated` short-circuits when raft
          is off, so the batched and unbatched forms execute identical
          code.
- [ ] **Batch the eval loop (claim N, plan N, apply N, complete N).** Now the
      whole remaining cost. With the wake herd fixed and health checks shown
      to be `outcome=empty`, the only meaningful quorum traffic left is
      `complete_and_claim_next` (44/s) plus ~20/s of plan applies. The
      amortization curve says the same thing from the other side: 6
      allocations per eval gives 219 alloc/sec, 44 gives 736, so per-eval
      overhead is what sets the rate.
    - [x] **claim N and complete N are done** — phase 4 of
          [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md),
          documented in
          [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#batching).
          `Store::claim_evals` takes a batch in one round and
          `Store::complete_evals_and_claim_next` settles the batch while
          claiming the next, so the 44/s `complete_and_claim_next` traffic
          becomes 44/N. The batch is a fair share of the live queue
          (`pending / evalThreads`, capped at 16), never lingers to fill,
          and has no config knob. **Unverified at scale**, see below.
    - [ ] **plan N and apply N are NOT done, and were declined on purpose.**
          Handlers write as they go across lanes rather than returning a
          plan, so committing N plans together means restructuring every
          handler to be a pure function of a snapshot — a scheduler rewrite,
          not a loop change ([call 2](calls/eval-path-raft-cost-plan_calls.md)).
          It also overlaps the D5 group commit in `coalesce.rs`, which
          already batches *concurrent* applies and gets *more* to batch as
          the claim/complete rounds get cheaper. This is the same target as
          "the plan-apply commit is serialized" in
          [aws-phase4-findings.md](aws-phase4-findings.md) (`apply` at 354 ms
          of a 381 ms total at 100,000 allocations) and should be costed out
          against that, not against the eval loop.
    - [ ] **Re-measure with batching on.** Rerun cells G/H from
          [aws-phase4-findings.md](aws-phase4-findings.md). Expect
          `legion_raft_ops_total{op="complete_evals_and_claim_next"}` well
          below the eval rate (it was 44/s at 44 evals/s), and the 3.4x
          `web_fleet` / `dense_fleet` gap to narrow. Divide that op's rate by
          `legion_evals_processed_total` for evals-per-round; if it stays at
          1, the queue never gets deeper than `evalThreads` and the
          bottleneck is upstream of the eval loop. **A local run cannot
          measure any of this** — `Store::replicated` short-circuits when
          raft is off, so a batched claim of N and N single claims execute
          the same SQL in the same process.
- [x] **Fix the eval-notifier thundering herd.** `notify_waiters()` woke
      every parked eval worker on ANY `eval` row change, and each answered
      with a `claim_eval` — a scheduler-lane quorum round whether or not it
      found work. Every eval crosses that path three times. Measured at
      16,678 scheduler-lane raft ops at 4 threads against 47,096 at 16, for
      identical work, with throughput FALLING as threads rose. Now wakes one
      worker per newly-claimable eval; empty claims are down to 0.3/sec and
      16 threads beat 4 by 23.7%. Adds
      `legion_eval_claim_total{outcome}`, whose absence is what hid it, and
      an `op` label on `legion_raft_ops_total`.
- [x] **Stop re-reading every allocation on a node inside the plan-apply
      commit.** `free_pool_for_node_tx` rebuilt a node's free pool by
      selecting every allocation row on it and parsing every `resources`
      blob, inside the serialized commit — so the cost of committing a plan
      scaled with cluster fill while the work per plan did not (~440 rows per
      eval at 50k allocations, ~1,760 at 200k, for the same ~44 placements;
      throughput 736.1 → 456.6 → 272.9 alloc/s). Now cached per node in
      `node_alloc_usage` and validated on every read against a fingerprint of
      the live rows (node allocation epoch + row count + id sum), so the
      cache can never inflate free capacity — the worst a stale row does is
      cost one rebuild. A Fresh plan's own allocations are read once per
      plan instead of re-filtered per node. Local A/B on the rebuild: 15.6
      → 8.6 µs per pool at 10 allocations/node, 37.8 → 9.4 µs at 40 (the
      old path grows 2.4x over that fill change, the new one 1.08x). See
      [aws-phase4-findings.md](aws-phase4-findings.md),
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md) and
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#the-per-node-usage-cache).
      **Unmeasured end to end** — see the open item below.
- [ ] **Re-measure the `dense_fleet` fill ladder.** The 2026-08-05 re-run
      measured a uniform ~2.4x throughput lift with the slope UNCHANGED
      (~1.65x per doubling) — which ruled out the per-node usage cache and
      the delta refresh as the dominant term and pointed at
      `reproject_model_nodes`. That term is now gone too (the usage memo,
      2026-08-06 above), and so is the planner's own fill term: the
      2026-09-02 pass reproduced a fill slope locally (`plan_under_fill`:
      `plan_only` 2.55 → 7.07 ms from an empty 5,000-node fleet to 40
      allocations per node) and removed it — no step of a plan walks the
      cluster's allocations any more, and a local plan commit publishes
      its rows as a delta rather than reloading every touched node's list
      (Done, below). Re-run the 50k/100k/200k ladder once more: if the
      slope still does not flatten, the mechanism is off the planner and
      store/refresh paths entirely and the next measurement is
      control-plane CPU at saturation
      ([aws-phase4-findings.md](aws-phase4-findings.md) open item 4).
      Two conditions for the re-run to mean anything: every deep cell in
      phases 4–5 was `driver_bound`, AND every cell carried the
      index-truncation bug (planner universe = the ~64 lowest node ids,
      fixed 2026-09-21, Done below), so submit with `--submitter native`
      and read `submission.driver_bound`, `submission.per_minute`, and
      `cross_check.agree` before trusting a cell. Re-run the eval-thread
      sweep above 32 in the same session — its knee is unmeasured for the
      same two reasons.
- [ ] **`node_alloc_usage` never sheds rows.** Node removal is a
      control-lane op and may not delete a scheduler-lane row, so a
      churning fleet accretes one inert ~100-byte row per departed node,
      in the table, the raft snapshots, and every plan changeset that
      republishes it. Harmless at rig scale; needs a scheduler-lane sweep
      (the `prune_node_health_orphans` shape) if node churn becomes a
      real workload. Same wart class: each applied plan's changeset now
      carries one `node_alloc_usage` upsert per touched node (~44 ×
      ~100 B on `dense_fleet`) — the write-side cost of the read-side
      cache, unmeasured on the rig.
- [ ] **The read-pool cap re-couples with the runtime on >12-core
      hosts.** The pool is `evalThreads + 4` capped at 16 for the 1 GiB
      page-cache budget, but `evalThreads` still defaults to
      `available_parallelism()` — so a 32-core host runs 32 eval workers
      against 16 read connections plus inline heartbeat reads, which is
      the saturating-in-lockstep shape the sizing change argued against.
      Resolves itself when the `evalThreads` default is decided
      ([eval-thread-default-plan.md](plans/eval-thread-default-plan.md));
      until then it is a documented corner, not a defect.
- [ ] **Decide the `evalThreads` default.** It is `available_parallelism()`,
      which is the 4 that placed slowest in the 2026-08-05 sweep at 5,000
      nodes: 177.27 alloc/s at 4 threads, 208.83 at 8, 219.25 at 16,
      **251.68 at 32**, and >= 263.08 at 64 where the LOAD GENERATOR became
      the constraint. An eval worker parks on quorum round trips rather than
      computing (8% CPU at saturation), so core count is the wrong basis —
      but the curve never turned over, so the right value is unknown.
      **Planned, not built** —
      [eval-thread-default-plan.md](plans/eval-thread-default-plan.md), which
      deliberately picks no number.
  - [x] **Fix the load generator before sweeping again (Phase 0a, the
        gate)** (2026-08-05). `legion-loadgen`
        (`benchmarks/loadgen/main.rs`, `cargo build --features loadgen`)
        submits a whole workload from one process over a pool of long-lived
        gRPC channels, with no per-job process spawn or dial. Measured on
        one macOS box, 5,001 web_fleet jobs, the two paths alternated back
        to back: **255-260 jobs/sec** on the CLI loop against **1,107-1,492
        jobs/sec** native on an idle box, and 105/108 against 741/564 on a
        loaded one — **5-7x either way**. End to end at 600 nodes,
        submission of 2,001 jobs went from 8.54s to 2.64s and the placement
        tail after the last submit went from 2.1s to 8.1s, so the scheduler
        is now the constraint by a fourfold wider margin.
        `driver.py --submitter {auto,native,cli}` selects it and every cell
        archives which path it took. The eval-thread sweep above can now be
        re-run past 32. Calls:
        [aws-comparative-benchmark-plan_calls.md](calls/aws-comparative-benchmark-plan_calls.md)
        §23-24.
  - [ ] **Native submitters for Nomad and K8s, before the next
        cross-system table.** `legion-loadgen` is Legion-only, so a cell run
        with `--submitter native` is comparable to another Legion cell and
        to nothing else; Nomad and K8s still spawn a CLI process per job.
        Until they have equivalents (Nomad's HTTP API and the apiserver
        REST API, each over one keep-alive session), every cross-system
        comparison must force `LEGION_SUBMITTER=cli` on all three. The rule
        is in `benchmarks/aws/README.md` § Methodology and the path taken is
        recorded per cell as `submitter` in `metrics.json` and
        `fairness-record.json`.
  - [ ] **Burst submission costs measured placement throughput — find out
        why.** The same 600-node/12,006-allocation cell placed 1,329
        alloc/sec when the CLI loop paced submission over 8.5s and 1,126
        alloc/sec when the native submitter delivered the same jobs in
        2.6s: about 15% lost to receiving the work faster. That is the
        first look the rig has ever had at Legion under a submission burst,
        and the obvious suspect is plan-apply conflict
        ([plan-apply-conflict-reduction-plan.md](plans/plan-apply-conflict-reduction-plan.md)).
        Measure `legion_plan_apply_total{outcome}` across a paced/burst
        pair before drawing a conclusion.
  - [ ] `configs/manager.ncl` pins `evalThreads = 4` explicitly, so it has
        to move with the default.
- [x] **The eval loop no longer blocks tokio worker threads on quorum
      rounds** (2026-08-06). The batch claim and the fused settle round
      both run under `spawn_blocking`, like the handler always did, so a
      parked quorum round trip never parks a runtime worker — the
      prerequisite for sweeping past 64 eval threads. The settle round
      also retries (bounded, `EVAL_SETTLE_MAX_ATTEMPTS`, fail-fast on
      `NotRaftLeader`) before abandoning a batch: on a STABLE leader
      nothing else ever recovers an orphaned batch, because
      `requeue_in_flight_evals` runs only at startup and on leadership
      acquisition. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §13. The status checker's `mark_nodes_as_lost`/`recheck_lost_nodes`
      moved onto the blocking pool in the same change.
- [ ] **A stable leader still never re-sweeps in-flight evals.** The
      settle retry above shrinks the window; the complete fix is a
      periodic `requeue_in_flight_evals` with an age threshold safely
      above the worst-case handler time (conflict backoffs can hold an
      eval for hundreds of ms × retries), so it cannot race a live
      handler. Needs that bound stated before it can ship. Related: the
      terminal write has no `status = 'in_flight'` guard, so a worker
      that survives a double leadership flap can settle an eval a new
      leader already requeued and re-claimed — narrow, pre-existing, and
      widened slightly by batching (audit finding, 2026-08-06).
- [ ] **`run_dispatch_future` has no `Send` bound** (`dispatcher.rs:408`),
      so holding a `MutexGuard` across an `.await` inside
      `dispatch_allocations` would **compile**. It does not happen today —
      the per-allocation loop is entirely synchronous and the awaits come
      after it — but this is the one spot on the plan path where the type
      system is not enforcing the invariant. Latent, not a live bug.

      which is the 4 that just lost to 16 by 23.7%. An eval worker parks on
      quorum round trips rather than computing (8% CPU at saturation), so
      core count is the wrong basis. A changed default needs a plan under
      [plans/README.md](plans/README.md)'s rule.
- [x] **Stop the coalescer polling for its handoff.** A submitter that lost
      the commit lock slept 1 ms on its reply channel and re-contended, so
      the lock sat free until some waiter's next tick and every waiter
      burned a wake-up per millisecond of every commit (phase 4: 347 ms
      follower wait against a 138 ms commit). The wait is now one
      `Mutex` + `Condvar`, woken exactly when the commit it queued behind
      ends. It also fixes a wedge: the old `commit_lock` was held across
      the batch runner, so a panic there **poisoned** it and every later
      submitter spun forever. `legion_store_coalesce_wait_rounds_total` now
      counts extra commits sat through, not poll ticks, and should read 0.
      Judgement calls, including why the leader deliberately does *not* run
      a second batch:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md) §3.
- [ ] **The plan-apply commit is still serialized, and batch size is the
      arrival rate.** The handoff fix above is worth milliseconds per
      commit; the 138 ms commit itself is untouched, and `batch_size` 1.32
      at 16 eval threads means only ~1.3 plan applies arrive per commit
      period — there is nothing queued to batch. No timer-free coalescer
      change can raise that (and a timer would tax the lone submitter), so
      the levers are a cheaper commit or more concurrent demand reaching
      the coalescer. Find out which by measuring how many threads are
      actually inside `submit_and_apply_plan` at once —
      [aws-phase4-findings.md](aws-phase4-findings.md) item 2.
- [x] **Skip the health-check write when nothing changed.** Shipped in
      #191 (`f8dbd68`) but never checked off here: `update_node_health_checks`
      compares against the current row on a READ connection first and
      returns before the write path when nothing changed — no savepoint,
      no write connection, no replicated-op machinery. The 2026-08-06
      audit re-verified it end to end. Residual: a *forwarded* health
      check (leadership-rejection path only) skips the compare on the
      origin and runs it on the target.
- [ ] **Re-measure the plan-apply fix on AWS.** Phase 4 of
      [plan-apply-conflict-reduction-plan.md](plans/plan-apply-conflict-reduction-plan.md).
      The two-pass bind is shipped and unit-proven but **unmeasured at
      scale** — every number in
      [aws-phase2-findings.md](aws-phase2-findings.md) is still the
      pre-change baseline. Re-run its five-row table; that table is the
      regression test. Success is `plan_apply_total{rejected}` under 5%
      (from 73.6%) and throughput that RISES with eval threads instead of
      falling. Watch `legion_scheduler_plan_bind_pass{pass="revalidated"}`
      — it counts the conflicts that used to be rejections, and should be
      high under bin_pack.
    - [ ] Run `spread` as well as `bin_pack`. Every phase-2 number is
          bin_pack (the manager template sets it and `translate.py` drops
          the scenario's `spread`), which is the maximally correlated
          policy and therefore the worst case for this conflict. It is
          also where the fix's regression risk lives: pass 2 improves
          packing under bin_pack but slightly loosens spread. Make the
          rig config and the scenario agree first.
    - [ ] Answer the two open questions the campaign could not:
          whether `build_plan` stays flat above 5k nodes (the whole scale
          claim rests on it, and it has never been tested there), and what
          made `task_fetch` grow 33x.
- [ ] **Late-binding plan apply, Phase 4 backlog** (each optional, gated on
      the fallback-rank / `plan_apply_total{rejected}` metrics the shipped
      phases added): capacity-aware candidate skipping, per-component partial
      commit + eval refill, striped disjoint candidate lists for identical
      replicas, and a serialized/batched apply lane for physically-full
      pools — [plan §8](plans/late-binding-plan-apply-plan.md). The shipped
      phases are in Done below.
      **The gating metric now has a number.** AWS rig 2026-08-04, 5,000
      allocations over 1,000 nodes with concurrent submission:
      `plan_apply_total{outcome="rejected"}` 472 against
      `{outcome="applied"}` 2503 — a **15.9% rejection rate**, each one a
      full eval retry. The rejections read "no candidate for task N could
      bind: all 1 candidate(s) were stale or invalid", while
      `legion_scheduler_index_candidates{index="fit"}` shows the index
      handing placement 33-64 candidates per task. So the index is not
      candidate-starved; the plan carries one candidate per task and loses
      it to a concurrent apply. That points straight at striped disjoint
      candidate lists as the highest-value item of the four.
- [ ] **Adaptive scheduler concurrency, phases 0/4/5.** The controller
      shipped in `observe` (see Done below); what it left open is everything
      that needs measurement — and the measurement it was planned against is
      retracted ([aws-phase4-findings.md](aws-phase4-findings.md)), so the
      first job is to find out whether a conflict-bound cell exists on the
      current build at all —
      [plan](plans/adaptive-scheduler-concurrency-plan.md).
  - [ ] **Phase 0's matrix was never run**, so every constant in the
        controller (`HIGH_WATERMARK = 0.20`, `LOW = 0.05`, the 4-tick probe
        dwell) is a fitted starting value. Run the fixed-limit sweep over
        `evalThreads ∈ {1,2,4,8,16,32}` at 1,000 and 5,000 nodes on a
        workload candidate lists cannot absorb (coupled replicas on a tight
        pool), recording conflict rate, fallback rate, candidate-rank
        histogram, goodput and per-op lane wait, into
        `benchmarks/scheduler/README.md` (plan §11, §16). Needs the native
        submitter first — the current rig cannot outrun the scheduler.
  - [ ] **Phase 4: flip the default to `enforce`.** Gated on §11's matrix
        showing `enforce` beating the best static `evalThreads` on that
        workload without regressing the uncontended one. Also rewrite the
        `evalThreads` guidance in
        [docs/scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md)
        from conditional ("with this on, a ceiling") to unconditional.
  - [ ] **The `pool` label on `legion_plan_apply_total`** (plan §7.7) is
        deferred with Phase 0: a cardinality increase whose only consumer is
        a per-pool controller, and the data that decides whether one gets
        built ([calls #2](calls/adaptive-scheduler-concurrency-plan_calls.md)).
  - [ ] **Phase 5 backlog**, each separately justified by Phase 0/2 data:
        per-pool controllers (gated on §11.3's mixed-pool result — today one
        saturated pool can throttle planning for every other), feeding the
        fallback rate in as a secondary pressure signal, coupling the limit
        to scheduler-lane pipeline wait as a second congestion signal, and
        live reload of `adaptiveConcurrency` without a manager restart.
  - [ ] **Only `job_stop` is exempt from throttling.** `rollout_tick` and
        the four `node_*` kinds all reach the planner in this codebase, so
        they hold a permit — a node-loss reschedule proceeds at the admitted
        rate under sustained throttling. If that latency proves to matter,
        the fix is a priority lane for recovery kinds, not widening the
        exemption ([calls #1](calls/adaptive-scheduler-concurrency-plan_calls.md)).
- [ ] **Cut the raft round trips per eval, or widen the lane pipeline.**
      Same run: the scheduler lane took 67,165 raft ops for ~10,700 evals
      (~6 per eval — `claim_eval`, the plan apply, `complete_eval_with_event`,
      plus enqueues, each its own replicated proposal), and accumulated
      **206 seconds of `legion_raft_pipeline_wait_seconds`** at 3.07ms/op
      against the status lane's 0.09ms/op on comparable volume.
      `propose_apply` is a flat ~1.5ms on every lane, so this is queueing
      behind the lane's single `pipeline_loop` thread, not replication
      cost. Net effect: `legion_scheduler_plan_phase_seconds{phase="total"}`
      averages 9.32ms per eval (of which `apply` is 6.74ms, 72%) while
      achieved throughput implies ~75ms of wall time per eval per thread.
      The scheduling work itself is not the problem — `build_plan` is
      1.72ms and `available_nodes` 0.03ms over 1,000 nodes. Options:
      batch the eval lifecycle ops into fewer proposals, or give the
      pipeline more than one thread per lane. Wants measurement of which
      of the ~6 ops are actually separable before any design.
      **Half done (2026-10-12):** sandbox-scaling Phase 1 made eval claims
      leader memory and fused the settle into the plan apply, so a placing
      eval is one entry ([calls](calls/sandbox-scaling-plan_calls.md) #15,
      #23). What is left is the lane itself, and it now bounds keyed
      sandbox lifecycles at ~50/s on a 4-vCPU host, against the plan's
      ≥ 5× target of ~125/s: ~6 ms of serial lane time per entry (capture
      3.2 ms, propose → apply 2.7 ms), the pipeline thread ~54% busy, the
      manager at 1.1 of 4 cores, and the same in `single` and `lanes`
      ([plan §14.3](plans/sandbox-scaling-plan.md#143-phase-1-group-commit-and-write-trimming),
      [calls](calls/sandbox-scaling-plan_calls.md) #17). Levers: more
      placements per plan entry (sandbox placements average 1.09 plans per
      entry, one eval each), a cheaper capture (each write executes twice,
      once to capture and once to apply), or a pipelined lane.
      **More placements per plan entry, tried (2026-10-14):** one running
      eval per job folds a one-job burst into fewer plans, but on one host
      it changed no burst's wall time, so it is not merged
      ([plan](plans/multi-eval-plans-plan.md) §12 ·
      [calls](calls/multi-eval-plans-plan_calls.md) #1). What did ship is
      one fewer entry for submit, update, and scale-out evals.
      **Profiled (2026-10-13):** the largest piece of the capture was one
      statement, the keyed bind inside the plan apply, which scanned every
      instance of the job and rebuilt a temporary B-tree of every
      allocation of the config on every placement. It now reads live keyed
      rows from two indexes
      ([plan](plans/scheduler-lane-entry-cost-plan.md) Phase 1 ·
      [calls](calls/scheduler-lane-entry-cost-plan_calls.md) ·
      [docs](../docs/sandboxes-at-scale.md#limits-to-know-about)). Still
      open, each designed in that plan:
  - [ ] **Pipeline the lane** (plan §6.2): capture entry N+1 over an
        overlay of the in-flight entries while N commits, with a
        capture-term check at apply. Gated on auditing which capture bodies
        read the read pool or the read model, which do not see an
        in-flight entry (`stop_keyed_instance_apply` reads its instance
        through `get_instance`), and on a three-voter measurement.
  - [ ] **One fsync per entry, not two** (plan §6.3): the state DB runs
        `synchronous = FULL` in raft mode only to keep the applied marker
        ahead of the log's purge horizon. `NORMAL` plus a startup restore
        from the local snapshot would drop one fsync per entry. Needs a
        Linux fsync measurement and a power-cut test.
  - [ ] **A keyed bind still visits every live instance of the task**
        (plan §11), two index lookups each, so a task with tens of
        thousands of running instances pays milliseconds per placement.
        Parked instances are free.
- [ ] **Fused top-k candidate scoring.** Ranked candidate lists materialize a
      scored vector per placement; the one-candidate path (`plan_candidates:
      0`) uses the fused argmax loop that does not. A fused top-k loop would
      give the default path the same win — worth doing when the
      candidate-construction benchmark (+2–10% plan-only) says it pays
      ([calls #20](calls/late-binding-plan-apply-plan_calls.md)).
- [ ] Edge boot follow-ups — the feature shipped (phases 1–4;
      [plan](plans/edge-boot-plan.md) · [docs](../docs/edge-boot.md) ·
      [calls](calls/edge-boot-plan_calls.md)); these are what it left open.
  - [ ] **The node's resource ledger is never configured in production.**
        `WorkerTaskManager::configure_ledger` has only test callers, so
        `LocalLedger` runs with no capacity and admits every edge claim:
        verification step 7 in [edge-boot.md](../docs/edge-boot.md) checks
        against infinite room. Committed allocations are not recorded in it
        either (only ratified claims are, and never released). Configure it
        from the node's declared resources and track every allocation's
        start and stop, or drop the step. The claim window now bounds how
        many edge claims a node admits
        ([plan](plans/edge-admission-control-plan.md) §5.3).
  - [ ] **Phase 0's measurement matrix was never run.** The start-path
        breakdown (submit → eval claim → plan → apply → dispatch → prepare →
        boot → first byte) in both persistence modes, cold and warm image,
        single- and cross-region, belongs in
        `benchmarks/scheduler/README.md` per the AGENTS.md before/after rule.
        Until it exists the plan's open question — is `boot` or `stage` the
        right recommended default — stays open, and the public doc recommends
        neither. Needs a real multi-node raft cluster and a real registry; see
        [calls #18](calls/edge-boot-plan_calls.md).
  - [ ] A per-interface default-drop filter for the `sandbox` `boot` gate.
        Today Legion only holds the TAP link down, which is the 95% mechanism;
        a filter is stronger on hosts where link state alone is not enough, but
        owning firewall rules means owning backend choice, conflicts with the
        operator's own rules, and cleanup after a crash
        ([calls #10](calls/edge-boot-plan_calls.md)).
  - [ ] Lift the "no gates above `stage` for tasks with declared ports or
        connect upstreams" restriction. Both are assigned inside the apply, so
        this needs speculative port assignment reconciled at commit
        ([calls #13](calls/edge-boot-plan_calls.md)).
  - [ ] Speculative attach for volume drivers that can genuinely fence a
        concurrent attach, via a capability advertised in the volume contract.
        Today every non-node-local driver downgrades the gate to `stage`.
  - [ ] Island mode: bounded survival of unratified work across a
        control-plane outage, with its own consistency argument (plan phase 5).
  - [ ] Ticket pre-distribution and bulk minting for warm fleets (plan
        phase 5). Per-node ratification batching was evaluated and deferred:
        once ratifications stopped refusing each other, one node's claims
        already share group-commit entries, and RPC batching would not
        shrink the entry
        ([ratify throughput plan](plans/edge-ratify-throughput-plan.md) §8,
        [calls #6](calls/edge-ratify-throughput-plan_calls.md)).
  - [ ] **Forward `RatifyClaim` on the manager side.** A follower answers a
        ratification with `FailedPrecondition` ("not the raft leader"), and
        only the worker's ratifier (`ManagerClaimRatifier` in
        `src/worker/daemon.rs`) knows to try the other managers. A CLI or
        third-party caller has to find the leader itself. Fail with the
        typed not-leader status that carries `x-legion-raft-leader`, so the
        existing `leader_forward` layer proxies it, and keep the node's
        leader cache for the one-hop path
        ([calls #2](calls/edge-ratify-throughput-plan_calls.md)).
  - [ ] A pre-warm push for the worker spec cache. A node today caches a task
        config once it has been dispatched an allocation of it, so an edge pool
        must run at least one allocation of a job before a ticket for it can be
        redeemed there. An explicit push (`legion job prewarm --pool edge-eu`)
        would remove that ordering constraint and compose with the
        [artifact cache and pre-warm plan](plans/artifact-cache-and-prewarm-plan.md).
  - [ ] Replace the string matching in `manager::edge_boot::classify` with a
        typed error out of the plan apply, if a second consumer of the
        rejection reason ever appears ([calls #17](calls/edge-boot-plan_calls.md)).
  - [ ] Speculative dispatch and late-binding plan apply do not compose yet.
        A claim is dispatched to `candidates[0]`, the planner's first choice;
        when the apply walk falls back to a later candidate the winning node
        is not the one holding the claim, so the claim goes unratified and
        holds its staged work until the lease expires. Correct but wasteful,
        and invisible except as a gap between
        `legion_provisional_claims_total` and the ratified count. Options:
        dispatch to the top *n* candidates and refuse the losers, or hand the
        apply walk the claim set so it prefers a node already holding one.
        Needs the fallback-rank metric to say how often it actually bites.
  - [ ] **Ratification throughput at scale.** An edge start boots without a
        manager, but every start still needs one durable ratification
        (`ratify_claim` in `src/manager/server.rs` → `edge_boot::ratify`),
        and that caps sustained sandboxes/sec. Phase 1 of the
        [ratify throughput plan](plans/edge-ratify-throughput-plan.md)
        measured it in-process and removed the false conflicts that made
        concurrent ratifications refuse each other (Done, below). What is
        left needs a real cluster.
    - [ ] **Networked ratification load test.** A `legion-loadgen --mode
          ratify` that mints prefix tickets and drives `RatifyClaim` at a
          fixed open-loop rate (reuse `--rate`) from `--simulate` nodes, each
          with its own node token, against a 3-voter raft cluster on the AWS
          rig. Report the rate where p99 crosses `ratifyTimeout`. Needs the
          simulated fleet to expose per-node credentials. The in-process
          harness (`tests/ratify_throughput_measure.rs`) is the ceiling it
          should approach ([plan](plans/edge-ratify-throughput-plan.md)
          Phase 3).
    - [ ] **Profile the per-ratification apply.** With the false conflicts
          gone, the confirmed rate is flat from 64 to 256 callers while
          entries grow from about 20 to about 60 claims, so the bound is the
          serialized work each ratification does inside the entry (about
          2 ms of manager CPU per claim on an M5 laptop). Find where it goes
          before touching the cap or batching
          ([plan](plans/edge-ratify-throughput-plan.md) §8, §11).
    - [ ] The worker never retries a refused ratification, `internal`
          included: `drive_claim` applies every refused verdict as a kill.
          The refusal table in [edge-boot.md](../docs/edge-boot.md) used
          to say `internal` is retried inside the lease, and now says it is
          not. After the ratify throughput work, `internal` means a store
          error or an election rather than a false conflict, so this is
          rare. The `OVERLOADED` shed from edge admission control refuses
          before boot, so it did not need this loop. Add the retry if
          `internal` refusals turn out not to be rare.
    - [ ] **Managers stop speculating when their apply is slow** (edge
          admission control [plan](plans/edge-admission-control-plan.md)
          Phase 2). The node's claim window sheds edge starts only. A
          manager-originated speculative claim whose plan apply outlives its
          lease still dies on the node, and its committed allocation boots
          again later. `dispatch_speculative` should skip speculation while
          recent apply latency passes a fraction of the task's lease.
    - [ ] **Ratifications carry their deadline** (edge admission control
          [plan](plans/edge-admission-control-plan.md) Phase 3). The
          `RatifyClaim` handler is synchronous, so a ratification its node
          already killed at the lease is still committed when the manager
          reaches it. That binds the instance to a dead claim, and the
          router's retry gets `instance_claimed` until reconcile clears it.
          Put the claim's remaining lease on `RatifyClaimRequest` and refuse
          `expired` without committing.
    - [ ] Plan-commit cost growing with fleet size caps ratification the
          same way it caps scheduling. Fix it once, under "Make plan-commit
          cost flat in fleet size" in Scheduling & placement, and make the
          load test show how much of ratify latency is commit time.
- [ ] Worker groups + taint/toleration equivalent. Node pools exist as an ACL
      and config concept but nothing scheduling-side consumes them.
- [ ] Preemption follow-ups — the feature shipped
      ([plan](plans/preemption-and-eviction-plan.md) ·
      [docs](../docs/preemption-and-eviction.md)); these are what it left open.
  - [ ] Decide whether the per-pool preemption toggle should be ACL-adjacent
        rather than manager config. `disabledPools` lives in
        `schedulerSettings`, so a pool-scoped operator cannot exempt their own
        pool without cluster-config access — even though `pools` already
        exists as an ACL selector. Named as a decision, not an oversight
        ([plan §Open Questions 6](plans/preemption-and-eviction-plan.md)).
  - [ ] Victim metadata is loaded by a store query on the preemption path
        rather than carried on the in-memory `Allocation` rows, so a plan that
        preempts pays a round trip inside planning. Deliberate — widening
        `Allocation` for four fields used by one code path would touch the
        read model, the caches, the raft changesets and the wire — but if
        `legion_scheduler_plan_phase_seconds{phase="build_plan"}` grows on a
        preempting cluster, that is the fix
        ([call 18](calls/preemption-and-eviction-plan_calls.md)).
  - [ ] Preemption for a multi-task group being placed fresh preempts for its
        first member; if a later member then cannot fit, the whole plan fails
        and commits nothing. Safe, but a group that *could* have been placed
        by evicting more is not.
  - [ ] `legion node drain` rate limiting via the disruption vocabulary was a
        Phase 4 candidate and was **evaluated and not taken**: drain has its
        own deadline-bounded semantics, and giving it a budget re-opens the
        cluster-wedge risk `disruption.minAvailable` is deliberately scoped to
        avoid ([call 10](calls/preemption-and-eviction-plan_calls.md)).
        Revisit only with a concrete operator request.
  - [ ] Revisit fair share only with real multi-tenant demand. Bands +
        per-namespace band caps (the `bands` ACL selector) are the 80/20;
        hierarchical queues stay a non-goal.
- [ ] **Gang scheduling, after Phase 1.** Phase 1 (the contract, `min`,
      the quorum preemption floor, `job plan` gang reports) is in Done below;
      [plan](plans/gang-scheduling-plan.md) (IN EXECUTION) ·
      [calls](calls/gang-scheduling-plan_calls.md) ·
      [docs](../docs/gang-scheduling.md). Open, in the plan's order:
  - [ ] `gang.placementTimeout` (Phase 2): stop waiting and fail the job.
        Needs a "waiting since" that survives wake/re-block cycles (a
        blocked eval's `blocked_at` resets on every block), a timer wake
        (blocked evals only wake on capacity), and a decided meaning for a
        failed job. Rejected as an unknown field today
        ([calls](calls/gang-scheduling-plan_calls.md) 13).
  - [ ] `gang.recovery: 'gang'` (Phase 3): a member failure or loss stops
        the survivors and re-places the whole gang as one wave, consuming one
        move-budget unit. Today members fail and top up one at a time.
  - [ ] Phase 4: an integration test that makes the `createFirst` blue-green
        gang update a supported path, and the autoscaler consuming a blocked
        gang's shortfall as scale-up demand.
  - [ ] **Gang-aware wake.** A blocked gang wakes on any capacity on its
        class (one freed replica's worth for a gang of eight) and re-plans in
        full every time. Wake only when the class's free capacity could fit
        `min - running - placeable` replicas, once a measurement shows the
        wasted re-plans matter ([calls](calls/gang-scheduling-plan_calls.md) 2).
  - [ ] **Whole-gang preemption victims.** Preemption never takes a gang
        below its quorum, so a `gang = true` job in a preemptible band is not
        preemptible at all. Whole-gang victim selection (take every member,
        cost it as one victim) belongs to the preemption plan
        ([calls](calls/gang-scheduling-plan_calls.md) 5).
  - [ ] **Reservation / backfill for starving gangs.** A waiting gang holds
        nothing, so small jobs can keep taking freed capacity before a large
        gang sees enough at once. Watch `legion_gang_blocked_jobs` first;
        the fix is a plan of its own (reserve freed capacity for the oldest
        waiting gang, backfill around it).
- [ ] Schedule onto the same node as an *existing job*. Soft `same_node_as`
      colocation works within one job spec
      ([scheduler/policy.rs](../src/manager/scheduler/policy.rs)); cross-job
      placement doesn't exist.
- [ ] Node-level builtin resource overrides are silently ignored —
      `ResourcePool::from_node` skips any builtin name, so `resources: { cpu: 40000 }`
      in a worker config disappears without a warning
      ([#143](https://github.com/mikenomitch/legion/pull/143)). Open questions: does
      an override replace the fingerprinted value or act as a second ceiling, and
      how does it compose with `reservedResources`? Either way an ignored key
      should warn or fail validation.
- [x] Node drain as a first-class operation — `legion node cordon|drain|uncordon`,
      eval-mediated eviction that respects stateful move protection
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] Fix the slowest bits of the scheduler using the benchmarks in
      [docs/testing-and-benchmarks.md](../docs/testing-and-benchmarks.md).
      Candidate construction is index-served
      ([docs/scheduling-indexes.md](../docs/scheduling-indexes.md)); the
      per-plan snapshot is fixed twice over (Arc'd nodes + cached free
      pools — [plan](plans/scheduler-plan-snapshot-plan.md) — then the
      cached sorted node list + Arc'd pool values, 2026-07-30); the
      plan-submission insert is fused into the apply (one quorum commit,
      2026-07-29); the O(fleet) `version_sample` is lazy (D4) and plan
      applies group-commit (D5), 2026-07-30; eval handling (including
      `plan_with_conflict_retries`' backoff sleeps) runs on the blocking
      pool instead of parking tokio runtime workers (2026-07-30); the
      remaining measured candidates all landed 2026-07-30 — shared
      per-task `ConstraintSet`/`PermitSet` parsing, settings parsed once
      at `Scheduler::new`, and the index shard-sort skipped when the
      caller discards the order (before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
      The two costs D5 left on the table closed 2026-08-03 (phase 4 of
      [plan-path-scale-levers](plans/plan-path-scale-levers-plan.md)):
      a plan batch is now ONE transaction on every backend — one fsync
      locally instead of one per plan — and its post-apply `task_status`
      projection is one status-lane write for the batch instead of one
      per plan. The 2026-09-02 pass (next entry) took the per-placement
      constants and the cluster-fill slope; further wins want profiling,
      not this list.
- [x] **Planning cost no longer grows with cluster fill, and the
      per-placement passes stop re-answering answered questions**
      (2026-09-02, phase 4 of
      [scheduler-performance-plan.md](plans/scheduler-performance-plan.md);
      calls #1-#5 in
      [scheduler-performance-plan_calls.md](calls/scheduler-performance-plan_calls.md);
      doc in
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#node-candidate-construction)).
      A new `plan_under_fill` Criterion group reproduces the AWS
      `dense_fleet` slope locally (5,000 nodes, 0/10/40 allocations per
      node): on the base, `plan_only` went 2.55 → 7.07 ms across the fill
      because five-to-six per-plan walks over every allocation in the
      cluster filtered for the planned job. Now the read model keeps a
      per-job allocation index maintained by the same writes as the
      per-node lists, so a plan touches only its own job's rows; a local
      plan commit publishes its rows through the same delta the raft
      apply path projects; `eligible_nodes` no longer re-runs the permit
      per candidate per placement (a restriction-map allocation each,
      ~100k per plan on the 1,000-allocation cells); the scoring cache
      indexes by candidate address instead of hashing id strings; and the
      apply and eval paths' per-node statements go through the statement
      cache. Before/after (same host, Criterion, base
      `c3291c9`) in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
      **The AWS ladder is not re-run** — see the open re-measure item
      above. Deferred, deliberately: the two remaining O(fleet) terms per
      plan (the snapshot's two map clones and the per-plan `node_lookup`
      build) are constants in fleet size, not fill, and measured small
      next to the removed terms; the `plan_and_apply` cell under
      `local_disk` still pays the inline status-projection commit the
      daemon defers to its drain task.
- [x] Scheduler performance measurement stack
      ([plan](plans/scheduler-performance-plan.md),
      [calls](calls/scheduler-performance-plan_calls.md),
      [docs](../docs/testing-and-benchmarks.md)) — scenario schema v2
      (heterogeneous `node_profiles` + mixed `job_types` with
      resources/constraints, per-scenario SLOs), four realistic scenarios,
      shared runner, the scenario × backend matrix
      (`tests/scheduler_scale_benchmark.rs`: local_disk / raft1 / raft3 /
      streaming) with recorded release numbers, `PlanTimings` phase
      instrumentation + `legion_scheduler_plan_phase_seconds`
      ([docs/metrics.md](../docs/metrics.md)), and the fused
      `submit_and_apply_plan` (before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
- [x] Architecture review proposals for scale
      ([plan](plans/scheduler-architecture-review-plan.md)) — all four
      accepted proposals shipped 2026-07-30 via
      [plan-path-scale-levers](plans/plan-path-scale-levers-plan.md)
      ([calls](calls/plan-path-scale-levers-plan_calls.md)) and
      [bounded-candidate-scoring](plans/bounded-candidate-scoring-plan.md)
      ([calls](calls/bounded-candidate-scoring-plan_calls.md)): D4 lazy
      version sample + read-model generation fence, D5 plan-apply group
      commit (leader-elect coalescer), D6 as cheap snapshots (cached
      sorted node list, Arc'd pools), D1 opt-in `score_sample` with the
      packing-quality gate. Docs in
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md);
      before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
      Phase 4 (2026-08-03) closed what D5 deferred: the batch is one
      transaction on every backend (local commit amortization, reversing
      judgement call #6) and its `task_status` projection is batched onto
      the status lane once per group commit. D2 (cross-manager
      scheduling) and D3 (in-memory state) remain recorded keep/defer
      decisions; D8's architecture choice still waits on AWS-test data.
- [x] Heartbeat path at scale
      ([plan](plans/heartbeat-scale-plan.md),
      [calls](calls/heartbeat-scale-plan_calls.md),
      [docs](../docs/architecture-managers-workers.md)) — heartbeat scale
      benchmark (`tests/heartbeat_scale_benchmark.rs`), status-lane
      heartbeat group commit with per-id verdicts, skip-reprojection for
      bare refreshes (leader + follower projection), integer liveness
      clock (`last_heartbeat_unix`), batched lost-node recheck, partial
      index for the volume-destroy feed, worker heartbeat jitter and
      `heartbeatSeconds` worker-type config. Numbers in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
      Deferred (noted in plan): per-node lost windows negotiated at
      registration; batching `mark_nodes_as_lost`'s per-node
      availability-sync + eval enqueues after a mass failure; an ACL
      token-identity cache for worker-plane RPC rates.
- [ ] AWS comparative benchmark vs Nomad/K8s
      ([plan](plans/aws-comparative-benchmark-plan.md),
      [calls](calls/aws-comparative-benchmark-plan_calls.md)) — phases
      0-1 built and locally validated 2026-07-30; campaigns (phases 2-5)
      await an AWS account:
  - [x] Phase 0: `legion start worker --simulate N`
        ([mini-plan](plans/worker-simulation-plan.md),
        [docs](../docs/worker-simulation.md)) — one process, N wire-real
        nodes from a `node_profiles` file, real cadences, stub dispatch
        server; end-to-end test `tests/worker_simulation.rs`.
  - [x] Phase 1 rig, everything short of the cloud:
        [`benchmarks/aws/`](../benchmarks/aws/README.md) — Terraform
        (validate-clean; system-agnostic boxes + spot sim fleet + S3),
        per-system configs (Legion A lanes / Legion B streaming+S3 lease
        / Nomad / kubeadm+KWOK), scenario→jobs translator, measuring
        driver with fixture-tested counting math and a `watch`
        fault-drill mode, campaign runner + collect/fairness-record
        scripts, piggyback experiment scripts (storm, failover,
        streaming-RPO, 24h soak, upgrade-under-load), Grafana boards,
        projection fitting (`analysis/fit_curves.py`), CI gate
        (fixture tests + terraform validate), and the local no-AWS smoke
        (`scripts/local-smoke.sh`).
  - [x] Unattended campaigns (2026-08-02): `scripts/remote-campaign.sh`
        drives a campaign from the loadgen box under tmux, so a run
        survives a sleeping laptop, a dropped session, and a changing
        egress IP (`admin_cidr` pins ingress to one CIDR). Scripts pick
        public vs private addresses from `BENCH_ON_RIG`; topology comes
        from a rig manifest instead of terraform state; scale steps use
        the ASG API under a narrow `orchestrator` IAM role; the spot
        fleet is released on any campaign exit (`--keep-fleet` opts
        out). See the README's "Unattended runs" and calls entries
        16-18.
  - [x] Quota headroom + durable results (2026-08-02):
        `var.worker_on_demand_percentage` splits the sim fleet across the
        spot and on-demand vCPU quotas (separate AWS limits) so neither
        has to cover a whole step; `experiments/packing-density.sh`
        measures the real nodes-per-instance ceiling the 1,250 estimate
        stands in for, feeding `run-campaign.sh --nodes-per-instance`;
        `var.archive_bucket` mirrors every cell to a bucket terraform
        does not manage (write, not delete), so results outlive both the
        VM and `teardown.sh`; `scripts/campaign-status.sh` publishes
        run state + console log to S3 with a 60s heartbeat, so a watcher
        with only HTTPS and credentials — no SSH, no repo, no terraform
        state — can check on an unattended campaign and tell "stalled"
        from "quiet". README "Watching from somewhere else" and "Keeping
        results after the rig is gone"; calls entries 21 and 25-27.
  - [x] Bootstrap the operator box the campaign is driven from
        (`scripts/bootstrap-operator-box.sh`, `scripts/phase1.sh`; calls
        entries 28-29). One command takes an AWS key to a running
        phase 1: builds an Ubuntu x86_64 box outside the rig's terraform
        (it *runs* that terraform, and must outlive `teardown.sh`, which
        destroys the state), gives it an Elastic IP so `admin_cidr` stays
        true, carries an IAM instance role instead of a copied key,
        writes `terraform.tfvars` from the box's own facts, and runs the
        six phase-1 steps under tmux with per-step checkpoints.
        `stop`/`start` for between sessions (~$268/mo running vs ~$12/mo
        stopped); `repair-iam` re-applies the role in place, which is the
        only remedy available when the box holds the sole copy of the
        terraform state.
  - [ ] Phase 1 exit on real AWS: `bringup.sh` + 1k-node
        `run-campaign.sh --system legion-a --smoke`. **In progress on a
        real account (2026-08-02).** Step 1 (local-smoke) passes on a
        real box. Bugs found and fixed while getting there, all in
        `bootstrap-operator-box.sh` unless noted: no default VPC in the
        account; `Subnets[0]` landing in an AZ that does not offer the
        instance type (us-east-1e has no c7i); `RunInstances` retrying
        every failure and blaming IAM propagation; `cargo` absent from a
        non-interactive SSH PATH (rustup writes to `~/.bashrc`, which
        Ubuntu skips for non-interactive shells) — fixed in `phase1.sh`
        and in cloud-init, and documented in `development.md`; `clang` +
        `libclang-dev` missing for `rusqlite`'s `session` feature forcing
        libsqlite3-sys through bindgen; `push`'s `rsync --delete`
        deleting `terraform.tfvars` and — far worse — `terraform.tfstate`,
        which would have orphaned the whole rig; the operator role
        missing `iam:TagInstanceProfile` while terraform tags every
        resource via `default_tags`.
        Unverified against real AWS, and first in line to shake out:
        the `orchestrator` role's `SetDesiredCapacity` path, the
        loadgen box's private-IP reachability to CP + spot workers, the
        tmux launch path in `remote-campaign.sh`, and the S3 status
        stream + archive-bucket mirror (both written and unit-checked,
        neither exercised against a real bucket). Run
        `packing-density.sh` during this step: it needs the same
        standing rig and retires the 1,250 assumption before any cell
        is sized from it.
  - [x] **Manager heartbeat throughput: ~7x, by taking the store call off
        the tokio runtime.** `Manager::heartbeat` called the blocking
        `Store::update_node_heartbeat` inline on a worker thread. That
        parked the thread for a whole fsync-bound raft commit, and — the
        part that actually mattered — left every other heartbeat stuck in
        the gRPC layer where the coalescer could not see it, so batches
        stayed at size 1 and `HEARTBEAT_BATCH_MAX = 128` was never
        approached. Now offloaded via `spawn_blocking`, so the queue
        builds, batches form, and one fsync amortizes across many beats.

        Measured on gp3 EBS (`experiments/heartbeat-commit-probe.sh`,
        3 voters + 1000 sim nodes on one box, 2s cadence = 500/sec needed):

        | worker threads | before | after |
        | --- | --- | --- |
        | 1 | 48.3/sec (9%) | **349.8/sec (69%)** |
        | 4 | 60.5/sec (12%) | **422.6/sec (84%)** |

        `cargo test --all --locked`: 1405 passed, 0 failed.

        Note the before-numbers barely move with thread count (48 -> 60):
        adding threads does not help when every thread is blocked, which
        is why "raise worker_threads" was never the fix.

        Still open, tracked separately: the remaining 16% gap at 4 threads,
        which is the single `pipeline_loop` OS thread per lane plus the
        fsync itself — see the liveness plan below for the architectural
        answer, and note the control plane now runs on local NVMe
        (`m6id.xlarge`), which removes most of the fsync cost.
  - [ ] ~~The manager sustains only ~80 heartbeat RPCs/sec regardless of
        fleet size or shape.** Measured on the first real AWS rig
        (2026-08-03; 3x m7i.xlarge, 3-voter lanes, sim fleets on
        c7i.2xlarge). This is a Legion-side ceiling, not a simulator one:

        | fleet layout | total heartbeats/sec |
        | --- | --- |
        | 250 nodes on 1 sim host | 80 |
        | 250 nodes across 2 sim hosts (125 each) | 80.3 |

        Doubling the simulator hardware bought nothing — per-host
        throughput *fell* from ~60/s to ~40/s so the total stayed pinned.
        A shared downstream resource is the constraint, and it is the
        manager. Supporting evidence:
        - `legion.Manager/Heartbeat` handler p99 **50ms**, which is slow
          for a counter bump plus a node-timestamp update, and at ~80/s
          implies only ~4 requests in flight
        - manager CPU **19-23%** of 4 vCPU, sim hosts **0.2%** — nothing
          is CPU-bound, so this is latency/serialization, not work
        - `legion_worker_heartbeats_total` is 100% `outcome="ok"` — no
          errors, no retries
        - the fleet degrades smoothly with density: 125 nodes/host holds
          97% cadence at 0.23s p99; 250 holds 64% at 2.49s

        Prime suspect: `Manager::heartbeat` calls
        `store.update_node_heartbeat`, which on a raft store commits
        through the status lane. A per-heartbeat raft commit would be
        latency-bound exactly like this. `docs/multi-raft.md` describes
        heartbeat group commit — worth checking whether batching actually
        engages at these arrival rates, or whether each heartbeat pays a
        full quorum round trip.

        **The instrumentation gap is closed** (2026-08-02, Done section):
        the next run of this rig can answer the open questions from
        `/metrics` alone — whether batching engages
        (`legion_store_coalesce_batch_size` le="1" vs `_count`), whether
        submitters wait or commit (`_wait_seconds{role}` vs
        `_commit_seconds`), whether the per-lane raft pipeline is the queue
        (`legion_raft_pipeline_wait_seconds` / `_depth`), and whether the
        tokio runtime is saturated by the blocking call
        (`legion_grpc_in_flight` vs `legion_runtime_worker_threads`, and
        `legion_runtime_spawn_delay_seconds`, which explains the 30ms
        `HealthCheck` p99 if it is). Read order:
        [docs/metrics.md](../docs/metrics.md) § "Diagnosing a manager
        throughput ceiling".

        **Consequences.** At ~80 hb/s a single manager supports roughly
        160 nodes at the 2s default cadence before liveness degrades, so
        the campaign cannot reach its 1k-10k scale steps without either
        fixing this or raising `heartbeatSeconds` (which changes what is
        being measured and must be recorded in the fairness record).
        Every phase 2-5 campaign is blocked on understanding it — a run
        at 1k+ nodes today measures this ceiling, not the scheduler.

        **Not the cause, ruled out by experiment:** the simulator's
        gRPC channel pool. `SIM_NODES_PER_CHANNEL` was changed 256 -> 64
        (4x the connections at 250 nodes) and rebuilt; cadence went 66%
        -> 64%, i.e. no change. That commit is reverted.

        **Design answer, SHIPPED 2026-08-03** (see Done):
        [heartbeat-cadence-at-scale-plan.md](plans/heartbeat-cadence-at-scale-plan.md)
        — sizing formula + freshness metric, a runtime warning,
        per-node lost windows derived from each node's cadence, and
        opt-in manager-dictated adaptive cadence. It does not raise the
        ~80/s ceiling; it stops the ceiling from marking healthy nodes
        lost, and makes the cadence a run was measured at part of the
        fairness record. Raising the ceiling itself is D8, now planned in
        [liveness-off-the-raft-path-plan.md](plans/liveness-off-the-raft-path-plan.md)
        ([calls](calls/liveness-off-the-raft-path-plan_calls.md)) — the
        AWS probe settled the D8 question the architecture review was
        waiting on (that plan has now SHIPPED too): gp3 48-86 hb/s vs local NVMe ~2000 hb/s on identical
        code, so the cost is the per-commit fsync. The `spawn_blocking`
        offload above collected most of the cheap amortization (gp3 to
        422/s, 84% of cadence at 1000 nodes), so the plan's phase 1 is now
        a measurement — attribute the residual 16% between the fsync and
        the single `pipeline_loop` thread per lane — and phases 2-4 take
        steady-state liveness off raft entirely, replicating only status
        transitions. Still needed because 84% of cadence at 1000 nodes is
        6x short of the campaign's 5k step and 12x short of its 10k step,
        and because `R` remains a property of the disk.
  - [ ] **Re-measure the AWS campaign now that heartbeats are off the raft
        path.** Neither shipped plan's scale runs have been executed — they
        need the shared rig. Owed:
        `benchmarks/aws/experiments/heartbeat-commit-probe.sh` on **gp3** at
        250/1000/2500 nodes (the acceptance number is the gp3-to-NVMe ratio
        approaching 1.0), `packing-density.sh`, all twelve `run-campaign.sh`
        cells, `storm.sh`, `soak-24h.sh` (raft log growth and snapshot
        cadence change materially when heartbeats stop producing log
        entries — possibly a larger win than the throughput), and
        `failover-drill.sh` extended with "zero nodes marked lost across the
        drill". Also the cadence plan's phase-4 reproduction: 1000 simulated
        nodes at `R ~= 80`, adaptive off reproducing the collapse and
        adaptive on marking zero nodes lost.
        [liveness plan §10.1](plans/liveness-off-the-raft-path-plan.md).
  - [ ] **Attribute the residual heartbeat-commit gap, then decide on
        `liveness.batchWindow`.** The knob ships parsed, validated, and
        **inert** (`0ms`), because the measurement it is contingent on has
        not been run: split the remaining gap between the fsync and the
        status lane's single `pipeline_loop` thread, at 1000 and 4000 nodes
        on both storage classes, reading
        `legion_store_coalesce_batch_size{path="heartbeat"}` against
        `legion_raft_pipeline_wait_seconds{lane="status"}`. Ship the window
        only if batches stay well below `HEARTBEAT_BATCH_MAX` under load —
        otherwise delete the field.
        [liveness plan §8 phase 1](plans/liveness-off-the-raft-path-plan.md),
        [call 4](calls/liveness-off-the-raft-path-plan_calls.md).
  - [ ] **No way to remove a node record, so simulated fleets orphan
        rows.** `Store::remove_node` (`src/storage/store.rs:5070`) is
        implemented, cross-lane correct, and unit-tested, but has no
        production caller: there is no `DeregisterNode` RPC and no
        `legion node` verb for it, so a row created by registration lives
        until the raft store is wiped. That is a product gap as much as a
        rig one — an operator decommissioning hardware has no way to
        remove the row either. On the rig it means `stop-sim-fleet.sh`
        leaves every node behind (the K8s path, by contrast, does
        `kubectl delete nodes -l type=kwok`), and a later smaller cell
        runs against the leftovers. Contamination is bimodal and both
        halves invalidate the cell: below 50% orphans you get the eval
        storm above; above 50% the mass-disruption breaker
        (`src/constants.rs:109`) marks *nobody* lost, so the cell runs
        with node-loss detection silently off, leaving one `error!` line
        (`src/manager/daemon.rs:824`) as the only evidence. Rig-side
        mitigations that need no product change: make the fleet gate in
        `run-step.sh:71` assert an exact live-node census instead of
        `registered >= NODES` (`4000 >= 1000` is why nothing caught it),
        and wipe + reinstall the control plane between an experiment and
        a campaign — `phase1.sh` runs `packing-density.sh` then
        `run-campaign.sh` with no wipe between. Do NOT scope node-name
        prefixes per run before deregistration exists: name *reuse* is
        currently the only thing bounding the node table, so unique
        prefixes would make every run leak its whole fleet.
  - [ ] **A mass node-loss event starves every other kind of eval.**
        Measured on the AWS rig 2026-08-04: ~3000 sim nodes vanished at
        once and job submission slowed to ~6s per job, with
        `evals_processed_total{kind="node_lost_reschedule"}` at 2752
        against `{kind="job_submit"}` at 66 and a 764-deep queue. Four
        separate causes, each fixable on its own:
        **Fixed 2026-08-06** —
        [node-loss-storm-plan.md](plans/node-loss-storm-plan.md)
        ([calls](calls/node-loss-storm-plan_calls.md)), public note in
        [docs/node-liveness.md](../docs/node-liveness.md) § "What a mass
        loss costs". Status per cause:
    - [x] **Equal priority means submissions queue behind recovery.**
          `node_lost_reschedule` evals now enqueue at
          `default_priority − NODE_LOST_RESCHEDULE_PRIORITY_DROP`
          (50 − 10 at the defaults), so a new submission overtakes the
          recovery backlog. Cross-kind `claim_eval` fairness was the
          rejected alternative ([call 1](calls/node-loss-storm-plan_calls.md)).
    - [x] **Lost nodes with no allocations still cost a full eval.** The
          predicate now runs at enqueue time (one chunked indexed query
          per batch, `Store::nodes_with_allocations`); the recheck
          re-runs it every tick so a racing in-flight placement is picked
          up one tick later, and the handler's early-return stays as the
          backstop.
    - [x] **The fan-out is one raft proposal per node, per 2s tick,
          inline in the status checker.** Both `mark_nodes_as_lost` and
          `recheck_lost_nodes` now go through
          `Store::enqueue_lost_node_reschedules`: one dedup query, one
          allocation-filter query, one `enqueue_evals` round for the
          whole batch (per-node forwarding-aware fallback under split
          lane leadership), and the checker runs the mark and the recheck
          on the blocking pool so the tick's gauge refresh and checkpoint
          never stall behind them.
    - [ ] **Failed evals may not reach `evals_processed_total`.**
          Suspected on the AWS run and then RETRACTED — the counter is
          incremented on both paths at `src/manager/daemon.rs:2267`, and
          the evidence turned out to be two scrapes compared across
          different times. Left here as one deliberate check for the next
          campaign rather than as a known bug: submit a job whose
          constraints match no node, and confirm
          `evals_processed_total{kind="job_submit",status="error"}`
          moves by exactly one.
    - [x] **Recovery is invisible.** `legion_nodes_recovered_total{path}`
          now counts both paths (`heartbeat`, `reregistration`), the
          re-registration path logs, and `marked_lost − recovered` is the
          net loss ([docs/metrics.md](../docs/metrics.md)).
    - [ ] **Re-measure the storm on the rig.** The 2026-08-04 numbers
          (submission ~6 s/job behind 2,752 reschedules) are the before;
          repeat the ~3,000-node loss and expect submissions to keep
          landing while `legion eval list` shows the backlog at
          priority 40.
        Context that still stands: a single-AZ loss in a three-AZ cluster
        is ~33% of the fleet, *under*
        `LIVENESS_MASS_DISRUPTION_FRACTION` (`src/constants.rs:109`), so
        the full storm fires unsuppressed — by design; these fixes are
        what make that affordable.
  - [ ] **`mark_node_ready` writes `status = "ready"`, which every liveness
        query filters out.** `mark_node_ready_apply`
        (`src/storage/store.rs`) sets `"ready"` rather than `"active"`, so a
        node recovered through it drops out of `active_node_liveness_rows`,
        out of `legion_nodes{status="active"}`, and out of the liveness scan
        entirely. It also flips `node.availability` unconditionally, so it
        would un-cordon a drained node. Nothing in production enqueues the
        `node_join` eval that calls it, so it is currently dead code on the
        real path — which is why the heartbeat recovery does the composite
        itself instead
        ([call 2](calls/liveness-off-the-raft-path-plan_calls.md)). Fix:
        normalize the status string to `"active"` and adopt the same
        `unavailable`-only availability guard, then delete the duplication.
  - [ ] **Node loss clobbers an operator's drain.** `mark_nodes_as_lost`
        calls `sync_node_availability(ids, "unavailable")` unconditionally,
        so a node an operator had `drained` loses that intent the moment it
        goes lost — and comes back `available` rather than `drained`. The
        recovery path deliberately does not paper over this (it only clears
        `unavailable`), because the drain intent is already gone from the row
        by then. Fixing it properly means recording the pre-loss
        availability, or keeping loss and drain on separate axes.
  - [ ] **Per-pool heartbeat freshness.** `legion_heartbeat_freshness_ratio`
        is fleet-wide, so a cluster with a slow batch pool and a fast
        latency pool has two populations and one blended ratio, which can
        mask a problem in the fast pool. Named as a known limitation on
        [docs/heartbeat-cadence-and-scale.md](../docs/heartbeat-cadence-and-scale.md).
  - [ ] **Clamp the served heartbeat interval below the stateful
        self-fence.** Adaptive cadence is on by default now, and nothing
        checks the served interval against `selfTerminateAfter` (90s by
        default). At the default 2,500 beats/s the 89s rung arrives past
        ~68,750 nodes, where a healthy worker can fence its stateful tasks
        between beats, and windows pass the fence from the 21s rung (~16,250
        nodes). Fix shape: cap the served interval below the smallest
        `selfTerminateAfter` among running stateful allocations, without
        making the controller depend on more than `N` for everyone else.
        Documented as a known limitation in
        [heartbeat-cadence-and-scale.md](../docs/heartbeat-cadence-and-scale.md#known-limitations).
        [cadence plan §13](plans/heartbeat-cadence-at-scale-plan.md),
        [call 14](calls/heartbeat-cadence-at-scale-plan_calls.md).
  - [ ] **Measure adaptive-by-default on the AWS rig.** The default flipped
        on the cost evidence (~10,000 heartbeats/s took more than a core of
        the leader at 20k nodes) without the plan's own criterion: a run
        showing a fleet past its ceiling surviving with adaptive on. Owed: the
        10k and 20k steps at the default rate, recording served interval,
        lost window, freshness, leader CPU, and scheduler throughput against
        a `heartbeat = { adaptive = false }` run of the same steps. The
        adaptive half ran on 2026-10-10 (served 5-8s at 10k, up to 21s at
        20k, 34-55s at 50k, all cells complete), so the `adaptive = false`
        comparison is what is left.
        [cadence plan §13](plans/heartbeat-cadence-at-scale-plan.md),
        [call 2](calls/heartbeat-cadence-at-scale-plan_calls.md).
  - [ ] **Tighten failure detection at large fleets.** At the default
        2,500 beats/s target a 20k-node fleet is served 21s beats, so a dead
        node is declared lost after ~105s, against 10s on small fleets. The
        AWS 20k storm drill (2026-10-10) replaced all 7,017 allocations in
        110s, nearly all of it detection, with an 84s placement gap. That is
        still faster than Nomad (TTL ~nodes/50s, ~400s at 20k) and
        Kubernetes (40-50s unreachable plus the 300s default eviction
        toleration), but a 10x regression on Legion's own small-fleet
        failover. Two steps:
        - **Raise the default target.** 2,500/s was sized against the old
          per-beat cost, before cached heartbeat SQL. At 10k nodes on the
          new build, 5-8s intervals ran comfortably. Rerun 20k at
          `targetRatePerSecond` 5000 (8s interval, 40s window) and 10000
          (5s, 25s), then keep the highest rate that leaves placement
          throughput flat.
        - **Split liveness from state sync.** Every beat also rebuilds the
          node's full desired state, which is what makes beats expensive
          enough to throttle. A near-free liveness ping, like Kubernetes'
          Lease renewal, with desired state sent only on change (see the
          "unchanged" heartbeat item under Scheduling & placement), would
          let 20k nodes beat every 2-3s and keep detection at ~10-15s at
          any size. Needs a plan, since it is a wire change.
        Until then, a latency-sensitive pool can pin a low `maxInterval`
        and pay for it in manager load.

  - [ ] ~~Investigate: a 1k simulated fleet sustains ~59 heartbeat
        RPCs/sec where ~500/sec is expected, with nothing saturated.~~
        Resolved by the entry above. **Correction to that entry's last
        bullet:** the follower disagreement (leader 942 active vs both
        followers 0 active / 1000 lost) was transient — measured just
        after a fleet teardown. With a fleet running, all three managers
        report ~997 active / ~3 lost. Node status replicates correctly;
        there is no replication bug.
        Measured on the first real AWS rig (2026-08-03, 1000 sim nodes on
        one c7i.2xlarge against a 3-voter lanes control plane). The
        numbers rule out the easy explanations:
        - sim host CPU **0.2%** of 8 vCPU, memory 3% — the fleet is idle,
          so this is not the simulator running out of room
        - manager CPU **19-23%** of 4 vCPU across all three — not
          saturated either
        - `legion.Manager/Heartbeat` gRPC **p99 50ms** — the RPC is fast
        - `legion_heartbeats_total` is per-RPC (`server.rs` increments it
          once per handler call), so this is not group-commit batching
          making the counter read low
        - effective interval works out to ~17s against a 2s
          `WORKER_HEARTBEAT_DURATION_SEC`, and the drill's heartbeat p99
          pins at exactly 10s — which is `HEARTBEAT_RETRY_MAX`, hinting
          the loop is in retry/backoff rather than steady state
        - node liveness disagrees across the cluster: the leader reports
          942 active / 58 lost while **both followers report 0 active /
          1000 lost**. Either follower `legion_nodes{status}` is
          leader-only by design and mismeasured here, or status is not
          replicating — worth settling on its own.
        Next step is the fleet's own worker-side counters
        (`legion_worker_heartbeats_total{outcome}` on :9101, see
        docs/metrics.md § Worker) to separate "heartbeats attempted and
        failing" from "heartbeats never attempted". Until this is
        understood, `experiments/packing-density.sh` cannot produce a
        density and every scale step stays sized from the plan's 1,250
        estimate.
  - [ ] **Warn when a manager binds a wildcard host with no
        `server.advertiseAddr`.** Found the hard way on the first real
        campaign. With `host: '0.0.0.0'` and `advertiseAddr` unset,
        `ServerConfig::advertised_address()` derives
        `http://127.0.0.1:4747`, and that address is embedded in *every
        join token the manager mints*. A remote worker decodes the token
        and dials its own loopback, retrying connection-refused forever.
        Nothing on the manager side looks wrong — the failure appears
        only in the worker's journal, and an operator watching the
        cluster sees "0 nodes registered" with no error at all. Options,
        roughly in order of preference: refuse to mint a join token whose
        address is loopback while bound to a wildcard host; or log a
        startup warning naming the derived address; or return the
        condition in `CreateJoinToken`'s response. The config comment in
        `configs/manager.ncl` already says what the field is for, and
        that was not enough. See `benchmarks/aws/configs/legion/
        manager-a.ncl.tpl` for the fix the rig now carries, and note
        `persistence.raft.advertiseAddr` is a *different* setting — having
        set that one is exactly why the gap was easy to miss.
  - [ ] Export process-level memory from the manager so the campaign can
        report the manager's own RSS instead of whole-box memory.
        `docs/metrics.md` has no process family and nothing registers a
        process collector, so `process_resident_memory_bytes` does not
        exist on `:4749`; `collect.sh` therefore reports node_exporter
        whole-box memory, an upper bound that includes the OS and (in
        `used`) reclaimable page cache — ~40% of `used` on an idle
        control-plane box. Deliberately not done mid-campaign: it changes
        the binary under test and invalidates the fairness record for
        every cell already collected. Do it before the phase-2 campaign.
        See calls entry 30 and the Metrics section of the plan.
  - [x] Rebuild the Nomad and K8s paths for the cross-system campaign
        (2026-10-09, branch `bench/aws-comparison-refresh`): Nomad 2.0.5
        + nodesim `702a0d9` with one process per node profile (the old
        script registered every node as class `""`), the mock driver's
        real name `mock_driver` (the translator asked for `mock`, which no
        node offers), K8s 1.36.5 + KWOK 0.8.0 run out of cluster as one
        controller per instance slice, native HTTP submitters for both,
        and an apiserver watch replacing `kubectl get --watch`. Plan
        § Cross-system campaign; calls entries 6, 7, 10, and 11.
  - [x] Rerun the comparison with every system's store on the same NVMe
        (2026-10-14, PR #40). Only Legion's had been on it; Nomad's raft
        log and etcd sat on gp3. Neither competitor got faster. Both are
        CPU-bound on one control-plane box from 5k nodes up. Ran
        `web_fleet` 1k-10k and the storm drill at 5k on all three, the
        `dense_fleet` sweep and `batch_burst` at 5k, and two ablations (K8s
        on one apiserver, Legion on fixed 2s heartbeats). Also fixed: K8s
        metrics never scraped (single-file Prometheus mount), the driver's
        Legion parser counting zero (camelCase events), plugin and
        `legion-loadgen` builds on main, and a 5s scrape costing Nomad's
        leader a quarter of a core. [docs/benchmarks.md](../docs/benchmarks.md),
        calls entries 5, 8, 9, and 12-14.
  - [ ] Run the rest of the cross-system matrix (plan § Cross-system
        campaign): the CLI control row (row 5) on the NVMe rig, the 10k
        storm on all three, memory per cell (row 6) in the report, and
        `REPORT.md`.
  - [ ] Nomad's placement follower decodes every allocation event in
        Python and missed 14-34 events in the 50k-allocation cells. The
        driver now completes the cell on Nomad's own count, but its
        throughput undercounts by the shortfall. A compiled follower, or
        Nomad's own count as the primary source, would close it.
  - [ ] K8s at 200,000 pods: the apiserver on the box running the
        scheduler stopped completing TLS handshakes and the driver's watch
        could not reconnect, so the 40-per-node `dense_fleet` cell has no
        full result. Point the driver at a different apiserver than the
        scheduler's leader, then rerun that cell.
  - [ ] Legion's fixed-2s-heartbeat ablation at 5k served well under the
        2,500 heartbeats/sec a 2s cadence implies (freshness 0.46), so
        that row understates the cost. Find out why the simulated fleet
        did not settle onto 2s with `adaptive = false`.
  - [ ] The plugins' own `Cargo.lock` files drifted while plugin builds
        were broken (`plugins/tasks/noop` gains `legion-guest`, `imbl`,
        `archery` on a fresh build). Refresh and commit them.
  - [ ] Legion B (streaming) campaign, and the 20k validation burst.
  - [ ] Nomad cannot run `mixed_heterogeneous`: nodesim has no node meta.
        Patch nodesim to accept a `meta` map (small, upstreamable) if the
        report needs a heterogeneous row for Nomad.
  - [ ] Failover drill for K8s: `experiments/failover-drill.sh` has no
        fault command for the scheduler's lease holder yet.
- [ ] Scheduling-index follow-ups deferred from
      [plan](plans/scheduling-index-plan.md) (see its calls doc):
      index-served candidates for multi-task **group** co-placement (v1
      groups still scan — calls entry 10); pluggable index kinds;
      `count.per` served by partition enumeration; device-set
      (`ResourceValue::Items`) dims; task-level `index_hint`;
      incremental orthant enumeration with early termination.
- [ ] Worker-fingerprinted `disk` as a built-in schedulable resource — the
      default `fit` index already dims on `resources.disk` and picks it up
      automatically wherever it exists; today that is only operator-declared
      `config_attrs.resources.disk` (plan open question, lean yes; also
      leaned on by the artifact-cache plan's disk accounting).
- [ ] Add to AGENTS.md that scheduler changes ship with a before/after perf
      benchmark
- [x] Close the last plan-fencing window (2026-07-28): the scheduler now
      samples epochs from an in-memory mirror kept coherent with the
      allocation cache (epochs read before allocations, published together
      under one lock; the raft changeset projection refreshes both from the
      epoch rows it carries; rejections self-heal the mirror). An
      adversarial pass showed the old DB-sampled gap was NOT microseconds
      under raft — it spanned the whole changeset projection — and was a
      real oversubscription. Plan apply also re-validates node
      *availability* on the fresh row, closing the cordon/lost race the
      epoch CAS structurally cannot see.
      [Calls #4](calls/durability-and-ops-execution-plan_calls.md)
- [x] Apply-time re-validation of node resource TOTALS (2026-07-30): the
      plan-apply transaction reconstructs each placement node's free pool
      from the fresh node row and the allocation rows the transaction can
      see (`ResourcePool::free_from_rows`), mirroring the planner's ledger
      (Fresh plans exclude the replaced job's own rows), and rejects
      placements a capacity shrink no longer fits — closing the window
      where a worker re-registration shrinking capacity (or
      `reservedResources`/`maxAllocations` changes) between snapshot and
      apply bumped no epoch and was never re-checked. Ports were already
      apply-safe; resources now match. Found by the 2026-07-28 adversarial
      pass.
- [ ] Volume-pin staleness in planning: pins are read mid-plan-build with
      no epoch coupling, so a pin invalidated mid-plan (e.g.
      `volume delete --force`) can still direct a placement; later
      reconciliation covers it. Low severity, same pass.
- [x] A raft-mode CONCURRENT-planning oversubscription test through the
      real `Scheduler::plan_and_apply` path (2026-07-30):
      `concurrent_planners_cannot_oversubscribe_under_raft[_lanes]` in
      [tests/raft_apply_roundtrip.rs](../tests/raft_apply_roundtrip.rs)
      drives two barrier-released planners against raft's
      commit-then-project ordering under both topologies — the exact
      window the epoch mirror closes. Regression insurance; the local_disk
      twin is `concurrent_plans_cannot_oversubscribe_a_node`.
- [ ] Let a node override its builtin resource capacity (`cpu`, `memory`, `cores`)
    - `ResourcePool::from_node` skips builtin names when reading a node's
      `resources` block, so `resources: { cpu: 40000 }` in a worker config is
      silently ignored rather than applied or rejected. Capacity can only come
      from the fingerprint today, which leaves no escape hatch when a platform
      fingerprints badly (see the Apple Silicon CPU frequency case) and no way
      to deliberately under- or over-subscribe a node.
    - Decide whether an override replaces the fingerprint or is a separate
      ceiling, and how it composes with `reservedResources`. Whichever way it
      lands, an ignored key should warn or fail validation instead of passing
      silently.
- [ ] **Fresh plans overcommit while the old config still runs (audit SC7).**
      Apply counts the replaced job's allocations as free, and the old config
      is removed only after apply. Deliberate today (resubmitting a job that
      uses more than half a node would fail otherwise). Fix shape: remove the
      replaced allocations inside the same apply transaction, using the
      node-epoch self-conflict handling evictions already have, with care for
      stateful release and rebind ordering. [calls #17](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **The capacity pass has triggers it doesn't listen to yet (audit SC4).**
      Registration and uncordon queue a nodeless `node_join`. Allocations freed
      by a stop or scale-in, and a lost node recovering through heartbeats, do
      not, and batch tasks are never woken. Full blocked evals stay with the
      gang-scheduling plan. [calls #10](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **Single-node (non-raft) mode doesn't heal a crash mid-reschedule.**
      A crash between removing allocations and reconciling them is healed only
      by regeneration, which runs on raft leadership acquisition. Found while
      fixing audit SC3.

## Task runtimes

Plans: [task-runtime-plugins](../docs/task-runtime-plugins.md).

- [ ] **Sandbox boot throughput, what is still open**
      ([plan](plans/sandbox-boot-throughput-plan.md) ·
      [calls](calls/sandbox-boot-throughput-plan_calls.md) ·
      [results](../benchmarks/aws/sandbox-boot/RESULTS.md)):
  - [x] **The worker's lock under churn** (plan phase 4, calls 14). A
        frame-pointer build with per-call-site lock timing on a c5.metal
        found the task-manager lock busy 60 to 80% of the time at 250
        starts a second: `assign` rewrote the share slice's `cpuset.cpus`
        and created the cgroup leaf under the lock, and the supervisor
        destroyed stopped VMMs under it. The leaf is now created in the
        unlocked launch, the slices are set up once, a session's reap runs
        its destroy and cgroup removal off the lock, a removed allocation
        directory is renamed aside and deleted by a janitor thread, and a
        start's log-sink manifest and stage line are written after the
        lock is released. The lock is busy about 20% of the time at the
        same rate ([results](../benchmarks/aws/sandbox-boot/RESULTS.md#bare-metal-second-campaign-2026-10-14)).
  - [ ] **A large node is bound by its root disks** (plan phase 4, calls
        13). A 96-vCPU node sustains about 150 boots a second with worker
        state on EBS (out of IOPS) and about 200 on RAM-backed XFS, where
        `root_disk` reaches 1.3 s at the median under 300 offered a second
        (3 ms alone), with a quarter of the CPU idle. Each start reflinks
        the image's one base file and has `debugfs` write the sandbox's
        files into the clone, and the clones' copy-on-write allocations
        meet in XFS's buffer and inode locks. Dropping `debugfs`'s fsync did
        not move the ceiling. Next: write the per-sandbox files without
        touching shared extents (for example a small per-sandbox payload
        disk, or a preallocated payload file written in place), or spread
        clones over several copies of the base, then measure again. Also
        measure on a node type with local NVMe.
  - [ ] **Firecracker and TAP sandboxes are not measured.** The rig ran
        cloud-hypervisor guests with no NIC. A TAP guest's start adds `ip`
        calls that take the kernel's `rtnl` lock, which Modal found to
        dominate their tail at high start rates. The Firecracker run (the
        same sequential, burst, and sustained cases, about an hour and $1)
        decides whether `auto` should prefer it for sandboxes that need
        neither cross-data-dir restores nor a boot log (calls 15,
        [docs](../docs/task-runtime-sandbox.md#choosing-a-vmm)).
  - [ ] **`perf/lazy-memory-restore` is unmeasured** (pushed, 7e93ebb8).
        On-demand memory restore needs cloud-hypervisor v52+. The rig ran
        v51.2. Its restore matrix (1, 4, 8 GiB, lazy vs eager, v51 to v53 and
        Firecracker, suspend and fork right after a lazy resume) is the next
        run, on a box with 32 GiB or more.
  - [ ] **Every sandbox restore waits 5 s for an egress proxy socket**, and
        that hides lazy against eager. Before it resumes a restored guest,
        the node waits up to `DATAPATH_WAIT` (5 s, `sandbox_checkpoint.rs`)
        for the datapath's proxy socket. Every guest has a vsock, so every
        checkpoint records gated networking, and a guest that is not gated
        never gets the socket. On main already. Measured on
        cloud-hypervisor v53.0 with a 2.4 GB checkpoint (2 GiB guest, 900
        MiB touched, `jobs/restore-vm-{lazy,eager}.ncl`): `restore_ms`
        about 5,040 for both modes, a 5.0 s resume end to end against
        15.6 s on main's worker
        ([results](../benchmarks/aws/sandbox-boot/RESULTS.md#memory-restore-2026-10-14)).
        Same root cause as the "non-gated sandbox waits the full 5s" item
        under the spot-check rerun above. Derive the wait from the
        allocation's network mode, then rerun the lazy and eager cells.
  - [ ] **Parallel branches not stacked** (calls 1), each still an
        uncommitted diff in its worktree unless noted: `sandbox-warm-pool`
        and `edge-prewarm` (warming, against the pre-warm stance until the
        cold path is measured: it now is, at 180 to 230 ms),
        `elastic-session-envelopes` (envelopes that grow with use),
        `edge-admission-backpressure`, `edge-ratify-throughput`,
        `chunked-checkpoints`, `checkpoints-on-stream`, `multi-eval-plans`,
        `scheduler-lane-entry-cost`, `flat-commit-cost`, and
        `web-fleet-throughput`. Each needs its own measured case.
  - [ ] **A memory-snapshot start for sessions** (E2B's 80 ms shape):
        restore a session from a per-node template snapshot instead of
        booting. Opt-in, filled by real starts, and falling back to a boot,
        per the pre-warm stance.
  - [ ] **Kernel size.** cloud-hypervisor copies the 48 MB reference kernel
        into guest memory per boot (`payload_loader`, about 6% of a busy
        node's CPU). Document, or ship, a minimal guest kernel config.

- [ ] **Task start critical path, deferred phases** (found while measuring
      the node's start path. [plan](plans/task-start-critical-path-plan.md)
      phases 4 and 5, [calls 4](calls/task-start-critical-path-plan_calls.md)):
  - [ ] Start reports wait for a heartbeat. A health transition (first
        `healthy`, a failure) reaches the manager on the next beat, about 1 s
        at the default 2 s cadence and up to 21 s on a large fleet's adaptive
        cadence. Rollouts and service discovery wait on it. Carry it on a
        worker-to-manager `StreamWork` report frame rather than waking the
        heartbeat per start.
  - [ ] A streamed gated placement gets its datapath a beat late. It starts
        as soon as its delta lands (`daemon::launch_streamed_work`), but its
        listeners, network policy, upstream endpoints, and credentials all
        come from the next heartbeat (`heartbeat_tick` → `apply_upstreams`,
        `apply_net_policies`). [task-runtime-sandbox.md](../docs/task-runtime-sandbox.md#gated-networking)
        says the host end is bound before boot, which holds only on the
        heartbeat path. Binding listeners from the mirror alone is not enough,
        because the policy is what lets traffic through: carry the policy
        assignment on the delta.
- [ ] **A `net_capture_unavailable` health sticks on a task without checks.**
      `heartbeat_tick` sets it when a gated allocation's capture is not ready,
      and nothing sets health back when capture recovers: only the health
      monitor writes healthy, and a task without checks is written healthy
      once, at start. The namespace-ordering fix removed the common trigger
      (the first beat binding before the namespace existed), but a transient
      bind failure still leaves the task reported unhealthy.
- [ ] **A failed `boot` check at start counts at once.** The first check runs
      the moment the task starts (`tokio::time::interval`'s first tick), and
      `boot_grace_time` only applies to `health` checks, so a `boot` check with
      `unhealthy_after_count = 1` (as in `configs/jobs/maximal.ncl`) marks the
      task unhealthy before it could have bound its port
      (`src/worker/task_health.rs`, the failure branch of the monitor loop).
      Give boot checks a grace, or start their cadence one `every` late now
      that startup probes catch the early pass.
- [ ] **The worker state file is rewritten whole on every start.**
      `persist_state` serializes every allocation (pretty JSON with full task
      configs), reads `/proc` for each pid, and fsyncs, under the task-manager
      lock, on each start, stop, and restart (`finish_unlocked_launch` →
      `persist_state_unless_provisional`). The cost grows with the node's
      allocation count, so a burst of starts on a dense node is quadratic.
      Sessions use `sessions.db` and skip it. Group-commit it off the lock, or
      keep a record per allocation. `legion_task_start_stage_seconds{stage="register"}`
      shows when it matters.
- [ ] **TAP sandboxes shell out to `ip` on the start path.** A fresh TAP is
      `ip tuntap add`, `ip link set master`, `ip link set up`, and the `boot`
      gate adds a `set down` and a `set up` (`sandbox_checkpoint::HostTaps`,
      `sandbox::set_tap_link`). Each is a fork and exec. The ioctls
      (`TUNSETIFF`/`TUNSETPERSIST`, `SIOCBRADDIF`, `SIOCSIFFLAGS`) would do,
      and the code already uses two of them. Worth it if the `network` stage
      shows up on a real KVM node.
- [x] **Task process supervision** — the worker reaps task children, applies a
      new `restart` policy (`always | onFailure | never`, `delay`,
      `successExitCodes`) sharing one budget with health-check restarts, and
      reports terminal outcomes so a batch task's `Success` comes from its exit
      code instead of its placement (`PROTOCOL_VERSION` 2, with a per-node
      legacy fallback for older workers) —
      [plan](plans/host-process-supervision-plan.md) ·
      [calls](calls/host-process-supervision-plan_calls.md) ·
      [docs](../docs/task-supervision.md)
- [x] **Worker-owned cgroups for `exec`** — opt-in `resourceEnforcement` on the
      worker config turns a task's `resources` into real kernel limits; an
      explicit `config.cgroup` still means "join this one"
      ([docs](../docs/task-supervision.md#resource-enforcement-cgroups)).
      **Superseded in place** (2026-07-31) by the container-runtime plan's
      node-wide subsystem: the same config knob now governs `exec` *and*
      `container` through one `legion/share` + `legion/reserve` hierarchy,
      with `cpu.weight` in place of the old `cpu.max` quota and cpuset
      pinning for `cores` ([docs](../docs/resource-enforcement.md),
      [calls 16](calls/container-runtime-exec-and-signals-plan_calls.md)).
- [x] **`exec` hardening block** — `noNewPrivileges`, capability bounding-set
      drops, `privateTmp`, `umask`, `nice`, applied in privilege order around
      the user switch ([docs](../docs/task-supervision.md#process-hardening))
- [ ] Deferred from the supervision plan: privileged runtime tests for the cgroup
      and hardening paths (CI covers derivation, validation, and the unconfined
      fallback; the root-only paths are manual against a Linux worker container);
      PID/IPC/UTS namespaces and seccomp for
      `exec` (a PID namespace needs a double-fork, which changes the pid the
      worker tracks — see
      [calls entry 10](calls/host-process-supervision-plan_calls.md)); enforce
      `resources.disk`; scheduler preference for enforcing nodes.
- [ ] Possible, not planned: activation and readiness for `exec` — an
      `sd_notify`-compatible readiness socket, `WatchdogSec`-style pings, and
      socket activation via pre-bound fds
      ([plan §8 phase 4](plans/host-process-supervision-plan.md))
- [ ] A **WASM** runtime
- [ ] A **`converge`** runtime — idempotent host resources (package, file,
      template, directory, user, service, mount) with per-resource
      `unchanged | changed | failed` reporting, i.e. the Chef-replacement shape —
      [plan](plans/host-convergence-plan.md) ·
      [calls](calls/host-convergence-plan_calls.md) (PLANNED: file-shaped
      resources → system resources → node-config jobs → why-run/drift →
      `host` ACL resource). Phase 1 depends on the exit-status work above.
- [ ] **Make a sandbox `memory_mib` that disagrees with `resources` a submit
      error.** Phase 2 of the
      [suspend plan](plans/suspend-resume-and-oversubscription-plan.md#10-rollout-and-backward-compatibility)
      derives guest size from `resources` and ships one release where an
      explicit, disagreeing `memory_mib` is only a warning and still sizes the
      guest (calls entry 16). Flip it to an error in the next release, and drop
      `memory_mib` from the schema after that
      ([docs](../docs/task-runtime-sandbox.md#guest-size)).
- [ ] **Sandbox base root disk cache.** One base root disk per image per
      node, cloned per sandbox, with the sandbox's own files written into
      the clone by debugfs: plan
      [rootfs-base-cache-plan.md](plans/rootfs-base-cache-plan.md) ·
      [calls](calls/rootfs-base-cache-plan_calls.md) ·
      [docs](../docs/task-runtime-sandbox.md#the-root-disk-cache).
  - [x] Phase 1 (in flight): the cache, per-sandbox files by debugfs on
        every start (which also stops host writes through an image's
        symlinks), `rootfs_cache::ensure_image_base` for prefetchers, and
        queued pulls sharing a tag `HEAD`.
  - [x] Verify on real KVM: cloned disks boot on cloud-hypervisor, at about
        10 ms a clone on XFS and 60 to 80 ms on ext4, through thousands of
        session starts on the sandbox-boot rig
        ([results](../benchmarks/aws/sandbox-boot/RESULTS.md)).
  - [ ] Verify on Firecracker, and checkpoint and restore a cloned disk.
  - [ ] Phase 2: sandboxes with file, tree, or layer artifacts still build
        their disk in full on every start (calls entry 5), including a kernel
        shipped as an artifact.
  - [ ] Phase 3: the `container` runtime still extracts every image per
        allocation (overlayfs over a node-wide lower dir).
  - [ ] Phase 4: show the cache in `legion node content`, and count hits and
        misses.
- [ ] **Linux container port mapping / private network namespaces.** Containers
      keep host networking; assigned ports reach them without mapping, but
      there is no isolation. Named non-goal of the container-runtime plan —
      it belongs to the networking work below.
- [ ] **NUMA/SMT-aware core placement.** The core allocator hands out whole
      logical CPUs lowest-free-first with no topology awareness: no NUMA-node
      affinity, no SMT-sibling avoidance. Nomad gates this behind Enterprise;
      revisit with a workload that measurably needs it
      ([docs](../docs/resource-enforcement.md#not-implemented)).
- [x] **Resource enforcement on by default** (PR #15). `enabled` defaults to
      `true` and an absent block means the defaults. A Linux worker that
      can't create `cgroupParent` warns once at start and runs unconfined
      ([docs](../docs/resource-enforcement.md#defaults-and-turning-it-off),
      [call 4](calls/host-process-supervision-plan_calls.md)).
- [ ] **A delegated (non-root) cgroup hierarchy root.** Enforcement assumes a
      root worker creating `legion/` at the cgroup root; a configurable root
      for delegated setups is unbuilt
      ([docs](../docs/resource-enforcement.md#defaults-and-turning-it-off)).
- [ ] **`legion alloc signal`** — an on-demand signal verb. The plumbing now
      exists (`TaskPlugin::signal`, routed through the registry); today signals
      are only delivered by declared policy (`changeSignal` on identity
      refresh, `stopSignal` on teardown). Additive; named non-goal of the
      container-runtime plan.
- [ ] **The `sandbox` runtime still cannot exec, signal, or deliver identity
      files** — all three need the guest agent the sandbox plan owns. The
      container runtime's `TaskPlugin::signal` override is the seam its
      guest-agent path would implement. Exec is now planned as Phase 1 of
      [sandbox-exec-plan.md](plans/sandbox-exec-plan.md).
- [ ] **Sandbox scaling to very large session counts**:
      [plan](plans/sandbox-scaling-plan.md) (IN EXECUTION) ·
      [calls](calls/sandbox-scaling-plan_calls.md) ·
      [docs](../docs/sandboxes-at-scale.md),
      [session mode](../docs/session-mode.md). Phases 0–6 shipped (Done,
      below): a keyed sandbox lifecycle is 8 raft entries, work streams to
      nodes, a full cluster blocks creates instead of failing them, unnamed
      ids leave no tombstone, idle keyed instances archive to the bucket,
      and an unnamed session starts and stops with no raft entry. Session
      mode shipped without idle sessions, identity, or a session log; those
      and the other Phase 6 gaps are the items below.
  - [ ] **The lifecycle rate reached ~2× Phase 0, not the ≥ 5× target.**
        ~50 lifecycles/s against ~25/s on the 4-vCPU reference host; the
        scheduler lane's serial per-entry cost is the bound. Tracked with
        its levers in "Cut the raft round trips per eval, or widen the lane
        pipeline" under Scheduling & placement
        ([plan §14.3](plans/sandbox-scaling-plan.md#143-phase-1-group-commit-and-write-trimming),
        [calls](calls/sandbox-scaling-plan_calls.md) #17).
  - [ ] **Run the sandbox campaign on the AWS rig.** Every Phase 0–3 number
        is from a single-host substitute. `benchmarks/aws/scripts/run-sandbox-campaign.sh`
        (3 managers, one load box, ~$1/hour) produces the ceiling,
        per-lifecycle raft and CPU cost, memory per resident sandbox, and
        labelled projections; record them in plan §14 and
        `benchmarks/scheduler/README.md`
        ([calls](calls/sandbox-scaling-plan_calls.md) #20, #60).
  - [ ] **Run the bottleneck measurements.** "Bottleneck measurements
        without a big fleet" in `benchmarks/aws/README.md` (added
        2026-10-12, never run) covers 14 bottlenecks on a laptop, a sim
        fleet, or one KVM worker. Its Gaps list names the missing harnesses
        (loadgen session mode, sessions in `--simulate`, a fixed-delay fake
        launch, sessions.db and ticket-cache benches, a real-runtime
        restore loop).
  - [ ] **Retried publishes are not pushed.** A failed publish becomes work
        again when its `publish_retry_at` passes, with no row change, so the
        node picks it up on its next heartbeat rather than over the stream
        ([calls](calls/checkpoints-on-stream-plan_calls.md) #7).
  - [ ] **Edge-boot provisional deltas on the stream.** Speculative dispatch
        still pushes with `PostWork` and the verdict arrives on the
        `RatifyClaim` reply
        ([calls](calls/sandbox-scaling-plan_calls.md) #39).
  - [ ] **Checkpoint budget reports and `snapshot warm` as blocked-eval wake
        events.** Only registration, node changes, readiness, and freed
        allocations wake today
        ([calls](calls/sandbox-scaling-plan_calls.md) #42).
  - [ ] **Measure one running eval per job where the lane is the
        limit.** Built in the `perf/multi-eval-plans` worktree and left out:
        locally a plan applies in under 2 ms, so a burst at any real arrival
        gap never piles up and there is nothing to fold. Run the rig's
        keyed `ladder` cells with that build against this one, and merge it
        only on a measured gain. Phase 3 (settling evals a running plan
        already covered) waits behind it
        ([plan](plans/multi-eval-plans-plan.md) §12,
        [calls](calls/multi-eval-plans-plan_calls.md) #1 and #8).
  - [ ] **A resubmit and a scale-in still settle in a separate entry.** The
        fused settle is armed in `job_submit` only for a job's first config
        and in `job_scale` only for a scale-out, because their trailing
        writes (the previous config's removal, the capacity pass) follow the
        plan ([calls](calls/multi-eval-plans-plan_calls.md) #5).
  - [ ] **Rollout ticks and node-scoped passes do not block.** They keep the
        old terminal outcome when nothing fits
        ([calls](calls/sandbox-scaling-plan_calls.md) #41).
  - [ ] **The raft seam test does not read `src/storage/store/*.rs`.**
        `tests/raft_seam.rs` parses only `store.rs` and `acl_store.rs`, so a
        replicated write added in a store submodule (`eval_claims.rs`,
        `blocked_evals.rs`) is not checked for a lane classification
        ([calls](calls/sandbox-scaling-plan_calls.md) #48).
  - [ ] **Idle sessions: suspend an unpromoted session to a bucket
        manifest.** A session cannot be suspended, forked, or snapshotted
        today; it has to be promoted first, which costs an entry and a row
        (plan §6.5, [calls](calls/sandbox-scaling-plan_calls.md) #18). The
        Phase 5 manifest format and `manifests/` layout are the starting
        point (#50). Phase 7's wake for sessions needs it.
  - [ ] **Workload identity for sessions.** A session task may not declare
        `identities`. Needs the signed lease of plan §6.3 in place of the
        start-time lease, and the envelope-scoped token check of §6.8
        ([calls](calls/sandbox-scaling-plan_calls.md) #18, #33, #34).
  - [ ] **The session log's shipper and per-session events.** Sessions
        are counted (`legion_session_starts_total`,
        `legion_session_stops_total`), not journaled. The node log itself
        (`sessions.db`) shipped in #36 for worker restarts, but nothing
        ships it off the node (plan §6.4,
        [calls](calls/sandbox-scaling-plan_calls.md) #19, #32, #37).
  - [x] **Edge-started sessions.** Session tickets and
        `Worker.StartSession` (#36,
        [session-durability plan](plans/session-durability-and-routing-plan.md)
        §6.4, [docs](../docs/session-mode.md#starting-at-the-edge)).
  - [ ] **More ways to promote.** `promote.onVolume` and promotion by
        `snapshot create --name` (plan §6.6) are not built; only a caller id
        at create and `legion instance promote` promote
        ([calls](calls/sandbox-scaling-plan_calls.md) #18).
  - [ ] **Ports, services, and network policy for sessions.** A session
        task that declares them is refused at submit, since the node
        datapath serves allocations and a session has none
        (`src/structs/session.rs`, `validate_session_task`).
  - [ ] **Sessions at scale on simulated nodes, and more than one
        manager.** Simulated workers do not run sessions and
        `legion-loadgen` has no session mode, so plan §13's 10^4-start run
        on `raft_lanes`, the leader-kill "complete within one full-report
        period" check, and the peer fan-out are tested only by fake-node
        integration tests and unit tests
        (`tests/session_integration.rs`,
        [calls](calls/sandbox-scaling-plan_calls.md) #58).
  - [ ] **Router locality.** The router ranks by free room only; plan §6.4
        also preferred nodes whose content store holds the image
        ([calls](calls/sandbox-scaling-plan_calls.md) #27). Investigated
        2026-10-13 and deferred. Checkpoint locality has nothing to prefer:
        only one node ever holds a `store` session's parts, and a resume
        already asks it first (`locate_session`). Image locality is a small
        win in one regime (simulated cold starts 37% to 25% with 10 images
        and a cache holding 5), mostly fixed by sizing
        `content.budgets.artifacts`, and a large weight makes warm nodes
        warmer. Revisit once checkpoint chunks are shared across
        generations and evicted parts stay as unpinned cache.
  - [ ] **`legion_sessions_envelope_free{job}`.** Plan §15 promised an
        envelope free-room gauge; only start and stop counters exist.
  - [ ] **Ended sessions are remembered per manager.** A retried stop that
        reaches another manager answers `NOT_FOUND`
        ([calls](calls/sandbox-scaling-plan_calls.md) #55).
  - [ ] **Batch session creates.** `CreateInstance` with `count` is refused
        on a session task ([calls](calls/sandbox-scaling-plan_calls.md)
        #57).
  - [ ] **Elastic envelopes: preemptible elastic room.** An envelope that
        grew holds the extra room until its sessions use less than half of
        it for `shrinkAfter`; a higher-priority placement can only preempt
        the whole envelope, not its grown part
        ([plan](plans/elastic-session-envelopes-plan.md) Phase 2,
        [calls](calls/elastic-session-envelopes-plan_calls.md) #5).
  - [ ] **Elastic envelopes: a job update starts each new envelope at its
        floor.** The node grows it on its next heartbeat, but sessions can
        hold more than the new envelope until then, and a placement in that
        window can overcommit the node. Carry the old envelope's size into
        its replacement
        ([calls](calls/elastic-session-envelopes-plan_calls.md) #6).
  - [ ] **Elastic envelopes on a real fleet.** Phase 1 is tested on fake
        nodes against a single-voter raft store and in a simulated diurnal
        day of the node controller. Not run on real workers, through a
        follower's forwarded write on a multi-manager cluster, or in a
        fleet-wide burst where every node grows in the same heartbeat
        ([plan](plans/elastic-session-envelopes-plan.md) Phase 3).
  - [ ] **Phase 5's 10^6 acceptance run, and archive in the op-count
        test.** Archiving was tested at 20 instances
        (`tests/archive_integration.rs`, local store); plan §13's 10^6
        archived instances leaving 10^6 slim rows, a resume under the
        published-resume time, and a `tests/raft_lifecycle_ops.rs` stage
        proving the archive adds no entry to its publish are not measured
        ([calls](calls/sandbox-scaling-plan_calls.md) #8).
  - [ ] Phase 7: wake on request for **sessions**. Shipped for keyed
        instances, archived ones included, by the
        [wake-on-request plan](plans/wake-on-request-plan.md); waking
        sessions from manifests is that plan's Phase 3, after idle sessions
        (above).
  - [ ] Phases 8 to 10: snapshot-clone density (with suspend plan Phase 8),
        a sub-VM tier seam, and cells only past the plan §6.11 trigger.
- [ ] **Wake on request**: [plan](plans/wake-on-request-plan.md) (IN
      EXECUTION) · [calls](calls/wake-on-request-plan_calls.md) ·
      [docs](../docs/gateway.md). Phase 1 (the gateway) is done, below.
  - [ ] Phase 2: gateway metrics (`legion_gateway_requests_total{outcome}`,
        wake latency, held requests) on a `/metrics` listener. Today the
        gateway only logs.
  - [ ] Phase 3: wake `session`-mode sessions from bucket manifests, after
        sandbox-scaling Phase 6.
  - [ ] Phase 4: node-local wake on connect (suspend plan §6.10), for
        clusters without a gateway tier.
  - [ ] Pass the client address to the instance (`Forwarded`). The gateway
        splices after the first head, so it cannot add a header to later
        requests on a kept-alive connection (plan §11).
  - [ ] Reload the gateway's TLS certificate without a restart (from a file
        watch or `certFrom = { kv }` like ingress chains).
  - [ ] A real microVM resume behind the gateway on KVM hardware; CI only
        runs the gateway against a real manager with a simulated worker
        (`tests/wake_integration.rs`).
- [ ] **Sandbox exec**: [plan](plans/sandbox-exec-plan.md) (IN EXECUTION) ·
      [calls](calls/sandbox-exec-plan_calls.md). Exec into Linux microVM
      sandboxes, and exec sessions that skip the manager. Implemented
      together with
      [worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md)
      (plans in [#16](https://github.com/mikenomitch/legion/pull/16); built
      in [#32](https://github.com/mikenomitch/legion/pull/32), in flight).
  - [x] Found during the plan: the worker listener had no authentication
        and no TLS, so any process on the worker's host could call
        `ExecTask` against any allocation on that node. Closed by
        worker-plane-auth Phase 1 (the mTLS worker plane, above).
  - [x] Amend the worker-plane-auth plan (exec plan §9): a ticket caller
        class for `ExecTask`, a classification test that allows two classes,
        and the JWKS verifier on every worker that admits tickets. Folded
        into [worker-plane-auth §6.1](plans/worker-plane-auth-plan.md#61-the-worker-listener-is-mutual-tls-always)
        (2026-10-11), which added one dependency: ticket verifiers need the
        JWKS over TLS, so direct exec also waits on worker-plane-auth Phase 4.
  - [x] Phase 0 (partly): `tests/exec_session_benchmark.rs` measures
        proxied and direct sessions (open latency, manager CPU and bytes per
        session) at 1 to 500 concurrent sessions on one host, plus mint
        throughput. Numbers in
        [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md#exec-sessions-proxied-vs-direct-sandbox-exec-phase-0--2026-10-12)
        ([calls entry 26](calls/sandbox-exec-plan_calls.md#26-phase-0-is-measured-on-one-host-at-up-to-500-sessions)).
  - [ ] Phase 0 at scale: 1,000 and 10,000 concurrent sessions, and the
        ≤ 20 ms / ≤ 80 ms open latency from the mint reply, same AZ, on the
        AWS rig with real workers (simulated workers cannot exec). Run
        `LEGION_EXEC_BENCH_CONCURRENCY=1000,10000 cargo test --release --test
        exec_session_benchmark -- --ignored --nocapture` against rig workers,
        and add a mode that reuses one worker connection per node, since
        direct opens here pay a TLS handshake each.
  - [x] Phase 1: exec into Linux microVMs over vsock. Every microVM gets the
        vsock device and the agent (`--no-relay` unless gated), the agent's
        exec listener on port 1100 (pty or pipes, the task's env and working
        directory, the process group killed on disconnect, 64 sessions),
        `VsockExecSession` on the worker with the version hello and bounded
        frames, `--instance` on `legion exec` / `legion action run` / MCP,
        the `exec` idle signal on microVMs, and the generic ending for a
        session cut by a suspend (calls 21 to 25). Docs:
        [task-runtime-sandbox.md § Exec](../docs/task-runtime-sandbox.md#exec),
        the runtime table in exec-and-actions.md, suspend-and-resume.md, the
        agent sandboxes guide.
  - [ ] Phase 1 on KVM (no KVM in CI): with Firecracker 1.17.0+ and
        cloud-hypervisor, a vsock guest kernel, and the static guest build,
        run `LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux
        LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest
        cargo test --lib sandbox_exec_over_vsock -- --ignored --nocapture
        --test-threads 1` (pipe and pty sessions, exit status, task env).
        Then by hand: exec after a memory resume reaches the new allocation
        with `--instance`, a session open across a suspend ends with the
        reset reason, a `none` sandbox takes exec while the guest still
        cannot dial the host, an open shell keeps an idle sandbox awake, and
        an action runs from the UI terminal.
  - [ ] Phase 1: a checkpoint captured before every microVM had a vsock
        device cannot be restored now that every restore passes the vsock
        override ([calls entry 22](calls/sandbox-exec-plan_calls.md#22-every-microvm-runs-the-agent-only-a-gated-one-runs-the-relays)).
        Legion has no installed base; note it for anyone with dev
        checkpoints.
  - [x] Phase 2: direct exec. `MintExecTicket` (no raft write, the same
        authorization as `ExecAllocation`), the `ticket` class on
        `ExecTask` behind the worker type's `exec = { direct = true }`,
        `legion exec --direct`, `legion exec-ticket mint`, the
        `legion_exec_ticket_mint` MCP tool, `exec.tickets` lifetimes, audit
        of finished sessions on the heartbeat, and revocation by re-checking
        open sessions on the heartbeat (calls 6, 9 to 15). `--instance` on
        `legion exec`, `legion action run`, and the MCP exec tools landed
        with it (`ExecStart.instance_id`). Docs:
        [exec-and-actions.md § Direct exec](../docs/exec-and-actions.md#direct-exec).
        Tests: `tests/worker_plane_e2e.rs` (direct session, exit status,
        revocation within a heartbeat), `src/worker/direct_exec.rs`,
        `mint_exec_ticket_admits_exactly_what_exec_allocation_admits_test`.
  - [ ] Direct exec: bind a ticket to the client's key (a `cnf` claim and a
        proof over the `jti` in the first frame), so a stolen ticket is
        useless (plan §13, first open question). Today whoever redeems first
        wins.
  - [ ] Direct exec: sessions minted by a JWT auth-method credential have
        no accessor to re-check on the heartbeat; they end at `max_session`,
        which the mint clamps to the credential's expiry
        ([calls entry 12](calls/sandbox-exec-plan_calls.md#12-a-direct-session-never-outlives-the-credential-that-minted-it)).
  - [ ] Direct exec: the JWKS refresh and clock-skew settings live under
        the worker's `edgeBoot` block even when only direct exec uses them
        ([calls entry 11](calls/sandbox-exec-plan_calls.md#11-the-workers-direct-exec-switch-is-a-top-level-exec-block-in-the-worker-type)).
  - [x] Found while building Phase 2: the pinned JWKS fetch built a runtime
        inside the server's runtime and panicked the first time a worker
        verified a boot or exec ticket against a live manager; edge boot's
        tests used a static JWKS and never hit it. Verification now runs in
        `spawn_blocking` and the fetch on its own thread
        ([calls entry 10](calls/sandbox-exec-plan_calls.md#10-exec-tickets-share-the-boot-ticket-jwks-cache-and-verification-runs-off-the-async-runtime)).
  - [ ] Found while building Phase 2: `legion ledger -j` prints a bare
        array, not the standard `{ok, data}` envelope AGENTS.md requires
        (`src/cmd/ledger.rs`).
  - [x] Phase 3: delegated minting keys. `CreateExecKey`, `ListExecKeys`,
        `DeleteExecKey` (`acl:write` / `acl:read`, idempotency keys), the
        `exec_key` control-lane table, keys delivered to workers by version
        on the heartbeat, key-signed tickets checked against the job glob and
        ceilings and refused for host-level runtimes, sessions ended when
        their key is deleted, `legion exec-key create|list|delete`,
        `legion exec-ticket sign`, and the MCP key tools (calls 16 to 20).
        Docs: [exec-and-actions.md § Delegated minting keys](../docs/exec-and-actions.md#delegated-minting-keys).
        Tests: `tests/worker_plane_e2e.rs` (a key reaches the worker, its
        ticket is refused for a host-level job and another job's, and stops
        verifying after delete),
        `a_delegated_key_mints_only_inside_its_glob_and_ceilings_test`.
  - [ ] Delegated keys: a positive end-to-end session on a non-host runtime
        (a `container` task on runc). The e2e suite runs `exec` tasks, which
        keys never reach, so the accept path is covered at the verifier
        only: on a Linux host with runc, run a container job under
        `exec = { direct = true }`, create a key for its job, sign with
        `legion exec-ticket sign`, and present the ticket to `Worker/ExecTask`.
  - [ ] Phases 4 and 5: the gateway tier (shared with the scaling plan's
        wake on request), and exec into unnamed sessions after the scaling
        plan's Phase 6.
- [ ] **Gated sandboxes by default, with memory checkpoints**:
      [plan](plans/gated-sandbox-checkpoints-plan.md) (IN EXECUTION) ·
      [calls](calls/gated-sandbox-checkpoints-plan_calls.md). Phases 1 to 5
      and upstreams in gated sandboxes are built (see Done, Task runtimes) on a host without KVM, Phases 1
      to 4 merged in PR [#24](https://github.com/mikenomitch/legion/pull/24).
      What is left:
  - [ ] Phase 0, the gate: boot a gated sandbox on a real VMM for the first
        time. On a KVM host with Firecracker 1.16.1+, cloud-hypervisor v52+,
        a kernel with `CONFIG_VIRTIO_VSOCKETS=y` and `CONFIG_IP_PNP=y`, and
        the static guest binary:
        `LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux
        LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest
        cargo test --lib sandbox_gated_boots -- --ignored --nocapture
        --test-threads 1` (G0.1), then the G0.2 throughput numbers. Full
        steps in [verification § Gated sandboxes](suspend-resume-linux-verification.md#gated-sandboxes).
  - [ ] Flip the Phase 2 default once G0.1 and G2.1 pass: set
        `SANDBOX_GATED_BY_DEFAULT = true` in `src/structs/network.rs`, update
        the public docs that say `auto` means `host` for sandboxes
        (`docs/network-handlers.md`, `docs/task-runtime-sandbox.md`, the
        agent sandboxes guide), and drop `LEGION_SANDBOX_GATED_DEFAULT`
        ([calls entry 2](calls/gated-sandbox-checkpoints-plan_calls.md#2-the-flip-to-gated-by-default-is-built-but-held-closed-until-phase-0-passes)).
  - [ ] Run the KVM checks for the built phases: G1.1 to G1.5 (gated memory
        checkpoints on both VMMs), G2.1 (glibc and musl images), G3.1
        (model-call wake), G4.1 (TAP re-addressing), G5.1 (`curl` an
        advertised port of a gated sandbox, suspend, resume, `curl` again;
        repeat on a TAP sandbox), G5.2 (a gated sandbox reaches an upstream
        by `LEGION_UPSTREAM_<KEY>` and by `<key>.svc.legion` across suspend,
        resume, and a fork), and record them in the verification doc.
  - [ ] Found while building Phase 5: decide the Firecracker floor for
        ingress. 1.17 fixes host-initiated RX sticking after a bare pause
        and resume (#6100), which a live memory snapshot or fork does to
        the origin; nothing enforces it yet, and G5.1 should measure it
        ([calls entry 20](calls/gated-sandbox-checkpoints-plan_calls.md#20-ingress-takes-no-firecracker-117-floor-until-phase-0-measures-6100)).
  - [ ] Found while building Phase 5: an ingress `proxy` hop cannot reach a
        port inside a sandbox guest (the same gap as a gated task's private
        port; [docs](../docs/network-handlers.md#ingress-into-a-sandbox)).
  - [ ] TAP-wired and loopback-only sandboxes cannot reach their
        `connect.upstreams` (nor, so, use credential injection), and submit
        does not refuse them. Serve a TAP guest's upstreams on its bridge
        address, or refuse at submit
        ([calls entry 25](calls/gated-sandbox-checkpoints-plan_calls.md#25-upstreams-on-tap-and-loopback-only-sandboxes-stay-unreachable-and-are-not-refused)).
  - [ ] An upstream removed from the job while an instance is suspended
        leaves a dead listener in the restored guest (the agent retargets
        the upstreams it is told about and never drops one)
        ([calls entry 14](calls/gated-sandbox-checkpoints-plan_calls.md#14-a-restore-retargets-the-guest-agents-upstream-relays-not-the-nodes-listener)).
  - [ ] Found while building Phase 5: after a restore that moved a port,
        the new allocation's own `LEGION_PORT_<label>` (its env, and what
        `alloc get` shows) differs from the port the restored guest listens
        on; `resume.env` and the datapath use the recorded port
        ([calls entry 13](calls/gated-sandbox-checkpoints-plan_calls.md#13-a-restored-sandbox-keeps-the-ports-it-was-captured-with-and-the-datapath-dials-those)).
  - [ ] Found during the plan: gated `container` and `exec` tasks without
        upstreams get no VIP block and so no resolver
        ([plan §6.5](plans/gated-sandbox-checkpoints-plan.md#65-gated-is-the-sandbox-default-and-it-denies-by-default)).
        Gated sandboxes have one now. The container and `exec` half is
        worker-plane-auth Phase 3's.
  - [ ] Found during the plan: the egress proxy records service-graph
        bytes only when a splice ends, so the graph misses long streams
        until they close (`src/worker/netd/proxy.rs` `splice`). Phase 3
        fixed the idle half (bytes are stamped as they move); the graph
        half is open.
  - [ ] Found while building: a gated resolver forwards queries while the
        allocation's policy is still pending (the first heartbeat after
        placement), a narrow DNS channel the proxy does not have
        ([calls entry 22](calls/gated-sandbox-checkpoints-plan_calls.md#22-a-gated-resolver-forwards-while-the-allocations-policy-is-pending)).
        Revisit if boot-time resolution failures turn out to be rare.
  - [ ] Found while building: TAP guests are IPv4 only (`sandbox.bridges`
        takes an IPv4 subnet), and a guest kernel without `CONFIG_IP_PNP`
        boots with no address until its first restore
        ([docs](../docs/task-runtime-sandbox.md#addressing)).
  - [ ] Found while building: the UI does not show a TAP sandbox's guest
        address (`InstanceInfo.guest_address`); the CLI and JSON do.
- [ ] **The container runtime's `#[ignore]`d OCI tests fail under runc
      1.5.1** on a cgroup v1 Linux host (2026-10-12, `LEGION_OCI_RUNTIME=runc
      cargo test --lib oci_runtime -- --ignored`). `runs_container_and_captures_logs`,
      `pins_a_container_to_its_assigned_cores`, and `exec_supports_a_tty_session`
      pass. Failing: `stop_lets_a_trapping_container_drain` and
      `stop_force_kills_at_the_deadline` call the plugin's `stop()`, which
      is the hard `delete --force` teardown since graceful stop moved to
      `stop_with` plus the worker's reaper, so the tests look stale.
      `exec_runs_inside_the_container` (empty output),
      `sees_task_dir_files_and_their_rewrites`, and
      `enforces_a_declared_memory_limit` (cgroup v1) are unexplained. Rerun
      on a cgroup v2 host with `runc`, then fix the tests or the runtime
      (`src/task_plugins/container.rs`).
- [ ] `exec`: unveil support
- [ ] `exec`: network namespaces (netavark? CNI?) — see *Networking* below
- [ ] **Suspend, resume, fork, and oversubscription** (generic primitives
      for dense sandboxes, also useful to ordinary services):
      [plan](plans/suspend-resume-and-oversubscription-plan.md) (IN EXECUTION,
      Phases 1-7 and the Track 2 resume guards shipped and integrated, their
      Linux and KVM checks pending, Phase 8 deferred).
      Supersedes the phasing of the
      [sandbox snapshots plan](plans/sandbox-snapshots-and-fast-start-plan.md),
      whose mechanics remain the reference (its §0 storage convergence shipped).
  - [x] Phase 1: measure and govern (sandbox VMMs in the allocation cgroup,
        per-allocation usage in the heartbeat, `legion node usage`, usage in
        `instance list`, per-node gauges, `legion_node_usage` MCP tool)
        ([docs](../docs/resource-enforcement.md#measuring-usage)).
        Follow-ups it left:
    - [ ] Run the Linux verification for Phase 1:
          [suspend-resume-linux-verification.md § Phase 1](suspend-resume-linux-verification.md#phase-1)
          (VMM in its allocation cgroup, `memory.max` = guest + overhead, the
          ~300 MiB `exec` usage check, the PSI and meminfo readers, teardown,
          metrics). Built and unit-tested with fixtures on macOS only. The
          meminfo and PSI readers pass on a Linux host (2026-10-12); the
          cgroup v2 reader and the rest still need a cgroup v2 host.
    - [ ] Usage lives on the manager that received the heartbeat. With
          several managers, `GetNodeUsage` on another one reports
          `usage_reported: false`. Forward or replicate it beside liveness
          digests if that proves confusing.
    - [x] `last_activity_unix` counts CPU and datapath traffic but not exec
          sessions (plan §6.2). Phase 5 added exec sessions and ingress
          bytes ([docs](../docs/suspend-and-resume.md#idle-policy)).
    - [ ] No reserved-memory or node PSI Prometheus gauge (only physical,
          available, used, CPU).
  - [x] Phase 2: burst and oversubscribe: `resources.memoryMax`
        (`memory.max`/`memory.low`), sandbox guest size from `resources`,
        worker `resourceEnforcement.oversubscribe` ratios, the memory pressure
        response (`reclaim`, `evict-bursting`, `evict-reserved`), the "who goes
        first" preview in `legion node usage`, `node.pressure` events, and
        `cause = node_pressure`
        ([docs](../docs/resource-enforcement.md#memory-pressure),
        [calls 26, 29, 56, 57, 58, 66, 67, 93, 100](calls/suspend-resume-and-oversubscription-plan_calls.md)).
        Follow-ups it left:
    - [ ] Run the Linux verification for Phase 2:
          [suspend-resume-linux-verification.md § Phase 2](suspend-resume-linux-verification.md#phase-2)
          (`memory.low`/`memory.max` on real cgroups, the ratio on a live
          node, the two-bursting-tasks eviction, `memory.reclaim`, sandbox
          guest size). Built and tested against fakes on macOS only.
    - [ ] A pressure-evicted allocation's replacement can be placed back on
          the node that evicted it. A short-lived anti-affinity for the
          evicting node would stop the bounce (calls entry 58).
    - [ ] `reclaim` needs Linux 5.19 (`memory.reclaim`). On an older kernel
          it frees nothing from host-process tasks and the response moves on
          to eviction.
    - [x] `reclaim` frees almost nothing from a sandbox (guest RAM is
          anonymous). Phase 5 added the balloon (both VMMs) and free page
          reporting (cloud-hypervisor)
          ([docs](../docs/resource-enforcement.md#sandboxes-and-the-balloon)).
    - [x] The `suspend` pressure step is accepted and does nothing until
          Phase 5. It acts since Phase 5.
    - [ ] Rerun `scheduler_speed` and `candidate_construction` before/after
          on a quiet host for the PR. The numbers in the
          [plan §9](plans/suspend-resume-and-oversubscription-plan.md#9-testing-plan)
          were taken on a shared, loaded machine and are noise-dominated.
  - [x] Phase 3: local checkpoints (`TaskPlugin::checkpoint/restore`,
        Firecracker `--api-sock`, node content store, `snapshot` table/CLI/ACL,
        `compat` constraints, `local-dir` `snapshot` op)
    - [x] Phase 3a: node content store (`src/content_store.rs`): one
          content-addressed store per worker at `<state>/content/sha256/<hex>`,
          per-class budgets (`content.budgets { artifacts, snapshots }`), LRU,
          idle TTL, owner pins, `minFreeDisk`, digest verification. Digest-pinned
          artifacts are served from it on the second allocation.
          [docs](../docs/artifacts.md#the-node-content-store) ·
          [calls 38, 72, 106, 107, 117](calls/suspend-resume-and-oversubscription-plan_calls.md#72-the-node-content-store-caches-only-digest-pinned-single-files)
    - [x] Run the Phase 3a Linux checks
          ([§ Phase 3a](suspend-resume-linux-verification.md#phase-3a)): a
          cache hit on a Linux worker, and `minFreeDisk` against a real
          filesystem's `statvfs`. Both pass on a loop ext4 (2026-10-12); the
          `minFreeDisk` refusal now names the free bytes it saw.
    - [ ] On a small disk the default content budgets clamp above the
          disk's size (1 GiB artifacts and 2 GiB snapshots on a 1.2 GB
          filesystem); only `minFreeDisk` keeps the store inside it. Clamp
          the defaults to the filesystem
          ([§ 3a.2 results](suspend-resume-linux-verification.md#3a2-minfreedisk-on-a-real-filesystem)).
    - [x] Phase 3b and later: write snapshot parts into the store with
          `PutOptions::pin(PinOwner::snapshot(id))` and release them with
          `ContentStore::release_owner` when a snapshot is deleted (3c).
    - [x] 3b: runtime checkpoint and restore (worker and runtime only).
          `TaskPlugin::checkpoint_support/checkpoint/restore` (task-plugin ABI
          3), `SnapshotManifest` + `compat_matches` in
          `src/structs/checkpoint.rs`, Firecracker `--api-sock` behind
          `TaskInstance.checkpoint_launch`, Firecracker and cloud-hypervisor
          API clients, device links and the identity disk, `cpu.*` /
          `sandbox.vmm*` fingerprint attributes, `local-dir`
          `snapshot`/`restore`/`clone`/`snapshot-delete` in both modes, the
          conformance suite's snapshot checks. Docs:
          [task-runtime-sandbox.md](../docs/task-runtime-sandbox.md#checkpoints),
          [volumes.md](../docs/volumes.md#snapshot-restore-and-clone).
    - [x] Wire 3b into the worker and manager (3c): `checkpoint_launch` from
          the `snapshots` block, capture (`checkpoint` → volume `snapshot` →
          manifest with `SnapshotCompat::with_cpu_from_attrs`), parts
          materialized into a `CheckpointDir` before `restore`, and
          `compat_matches` as a placement check with a named reason.
    - [x] Ingest checkpoint parts into the node content store (3a) by digest,
          and hardlink them back (after verifying) for restore (3c).
    - [x] 3c: the manager side. `snapshot` and `snapshot_alias` tables and
          `instance.restore_from` (scheduler lane), `CreateSnapshot`,
          `ListSnapshots`, `GetSnapshot`, `DeleteSnapshot`, `RestoreInstance`
          (idempotent, full proto comments), the heartbeat checkpoint channel
          (`checkpoint_requests` / `snapshot_reports`, re-sent until applied,
          forwarded to the lane leader), the worker `checkpoint` capability,
          the `snapshot` ACL resource, `legion snapshot
          create|list|get|delete|restore` with `--wait`, `legion_snapshot_*`
          MCP tools, `snapshot.*` events, `snapshots { ttl, keep, memory }`
          with expiry, restore pinned to the snapshot's node with `compat`
          checked at placement, submit-time and call-time refusal of memory
          checkpoints on gated sandboxes, TAP recreation on restore, and the
          `exec` disk round trip test.
          [docs](../docs/suspend-and-resume.md) ·
          [Linux verification](suspend-resume-linux-verification.md#phase-3c)
    - [ ] Run the Phase 3c Linux checks
          ([§ Phase 3c](suspend-resume-linux-verification.md#phase-3c)): the
          acceptance check (`snapshot create --memory` then `snapshot restore`
          brings back a RAM-only counter on both VMMs), content-store pins,
          the paused volume snapshot, TAP recreation and `TUNSETIFF` busy
          detection, and the restored VMM's cgroup.
    - [ ] The worker holds its task-manager lock while the VMM writes guest
          memory, so a multi-GiB capture on slow disk delays that node's
          heartbeats. Run the runtime's `checkpoint` without the lock (the
          volume half and content-store ingest already do).
    - [ ] Memory snapshots copy only the instance volume. A sandbox that also
          attaches a block `volumes = [...]` claim resumes with that claim's
          current contents under a page cache from capture time. Refuse such
          a memory snapshot, or snapshot claims too.
    - [x] Restoring into another instance shipped in Phase 6 as new
          instances (`instance fork`, `instance create --from-snapshot`).
    - [ ] Restore is keyed-only. Slot instances can be snapshotted but not
          restored or forked.
    - [x] Allocation ids are never reused: the `allocation` table was rebuilt
          with `AUTOINCREMENT` in Phase 4, so a restore's or resume's
          replacement always has a new id
          ([call 28](calls/suspend-resume-and-oversubscription-plan_calls.md#28-allocation-ids-are-never-reused-phase-4)).
    - [ ] `snapshot.restored` does not carry the restore's duration, although
          the worker reports it. `instance get` shows it now (`last_restore_ms`,
          Phase 4) and `instance.resumed` carries it for resumes. Add it to the
          snapshot event too (plan §6.13).
    - [ ] A restore waiting for placement explains itself only through the
          placement error (eval events, `job info`). Add a `restore-pending`
          detail to `instance get`, like `volume-pinned`.
    - [ ] Snapshot metrics: Phase 4 added `legion_checkpoint_bytes{kind}` (every
          capture) and suspend/resume latency histograms. Still missing: manual
          snapshot capture and restore latency, and content-store bytes by
          class (plan §6.13).
    - [ ] Scheduler benchmarks for 3c were run with the stateless bench jobs,
          which never reach the new restore-pin lookup (keyed tasks only).
          Add a keyed-instance group to `scheduler_bench` that covers it.
    - [ ] Run the KVM checks in the
          [Linux verification plan](suspend-resume-linux-verification.md#phase-3b)
          (`sandbox_memory_checkpoint_round_trip_*` are `#[ignore]`d), and
          record restore latency per VMM.
    - [x] Memory checkpoints of gated sandboxes
          ([call 55](calls/suspend-resume-and-oversubscription-plan_calls.md#55-gated-sandboxes-are-not-memory-checkpointable-yet)).
          Built in [gated-sandbox-checkpoints-plan.md](plans/gated-sandbox-checkpoints-plan.md)
          Phase 1: every restore retargets the vsock to its own allocation's
          socket ([calls entry 5](calls/gated-sandbox-checkpoints-plan_calls.md#5-a-restore-retargets-the-vsock-socket-to-its-own-allocation-through-vmm-overrides-not-a-stable-path)).
          Its KVM checks are open under the gated plan.
    - [ ] Hashing a multi-GiB memory part adds seconds to a checkpoint (after
          the guest resumes, so not paused time). Let the content store hash
          on ingest instead of hashing twice.
  - [x] Phase 4: suspend and resume. The `suspended` instance state and the
        §7 instance columns, `SuspendInstance` (`idempotency_key`,
        `--checkpoint memory|disk`, `job:write` plus `snapshot:write` for
        memory), resume through `StartInstance` with `cold`, the task
        `suspend` block (`checkpoint`, `reserve`, `onRestoreFailure`,
        `resumeTimeout`, `onResume`, `durability = "local"`), the suspend
        transaction on the capture report, `onRestoreFailure` on a failed
        restore, a lost checkpoint, and `resumeTimeout` (manager sweep),
        `reserve` as a held `suspended` allocation row, disk suspend for
        `exec`/`container`, the guest resume handler (`legion guest-resume`:
        RNG reseed, clock, hostname, MAC, identity files, `onResume`), fresh
        allocation ids and tokens on every restore, `instance.*` events,
        `allocation.removed` `cause = suspended`, the CHECKPOINT column and
        last suspend/restore in `instance get`, suspend/resume/bytes metrics,
        `legion_instance_suspend` MCP tool, mixed-version
        `FailedPrecondition`.
        [docs](../docs/suspend-and-resume.md) ·
        [calls 28, 32, 33, 35, 45, 51, 61, 79, 82, 123](calls/suspend-resume-and-oversubscription-plan_calls.md) ·
        [Linux verification](suspend-resume-linux-verification.md#phase-4).
        Follow-ups it left:
    - [ ] Run the Phase 4 Linux checks
          ([§ Phase 4](suspend-resume-linux-verification.md#phase-4)): the
          acceptance numbers (suspend under 1.5 s, resume under 300 ms for a
          1 GiB guest on both VMMs), identity refresh inside a real guest,
          RNG divergence, and a disk suspend of a `container` task. Publish
          the latencies in `docs/testing-and-benchmarks.md`. The Linux-only
          resume handler code (`RNDADDENTROPY`, `clock_settime`,
          `sethostname`, `BLKFLSBUF`) was never compiled on Linux by this
          phase: let CI catch it first.
    - [ ] The resume handler is the worker's binary, so a guest whose libc
          cannot run it (musl images with a glibc worker) restores without the
          refresh. Ship a static (musl) build of the handler, or a separate
          small static binary.
    - [ ] A memory suspend keeps the guest paused while its parts are hashed
          into the content store, so it may miss the 1.5 s target on slow
          disk. Hash once, on ingest (same follow-up as 3c's double hash).
    - [ ] Running processes in a restored guest keep the env they started
          with; only `/legion/resume.env`, `/legion/identity/<ENV>`, and
          `onResume` see the new token. File-delivered identities are not
          refreshed in the guest at all (sandboxes cannot deliver identity
          files yet).
    - [ ] `reserve = true` holds are released by a job update or job stop (the
          instance stays suspended, holding nothing). Carry holds across a
          rollout if a platform needs it.
    - [ ] A checkpoint is marked `lost` only when its node is removed. A node
          that is merely down leaves the resume waiting (`resumeTimeout`
          applies under `onRestoreFailure = "cold"` only).
    - [x] Add a keyed-instance group to `scheduler_bench` covering
          `count_keyed_allocations_desired` and the restore-pin lookup. The
          `restore` group plans keyed restores and the `snapshot_budget` group
          (Track 2) plans new keyed instances of a memory-suspend task.
    - [x] Phase 5 seams: `suspend_reason` takes `idle` and `pressure`;
          `PressureStep::Suspend` in `src/worker/pressure.rs` is still a no-op.
          The worker-initiated suspend must reuse `capture_for_suspend` and
          report through the existing channel. Done in Phase 5.
  - [x] Phase 5: automatic suspend. The task `suspend.idle { after, signals,
        cpuBelow }` policy (signals `network`, `connections`, `cpu`, `exec`;
        `hint` refused until Phase 8), worker idle detection reported as
        `AllocationUsage.idle_since_unix` / `idle_after_seconds` (ingress
        connections and bytes from the datapath, exec sessions, cgroup CPU),
        the manager idle sweep (`suspend.idleSweep`, default 15s, in memory,
        `begin_suspend` forwardable to the lane leader), `suspend_reason`
        (`manual | idle | pressure`) threaded through `NewSnapshot` and
        `begin_suspend`, `InstanceInfo.idle_since` / `idle_after_seconds`,
        the `suspend` pressure step for `suspend.onPressure = true` (most idle
        first, before `evict-bursting`, worker-minted ids, local suspensions
        applied by the manager and acknowledged by the desired set, moved-on
        instances reclaimed), the `reclaim, then suspend` preview, the sandbox
        balloon (Firecracker and cloud-hypervisor API calls, `deflate_on_oom`,
        free page reporting on cloud-hypervisor), `instance.suspended`
        `reason`, `node.pressure` step `suspend`, and the
        `legion_instance_suspends_total` / `legion_idle_sweep_suspends_total`
        metrics. No scheduler change, so no benchmarks.
        [docs](../docs/suspend-and-resume.md#automatic-suspend) ·
        [calls 19, 20, 31, 51, 59, 75, 90, 102, 124](calls/suspend-resume-and-oversubscription-plan_calls.md) ·
        [Linux verification](suspend-resume-linux-verification.md#phase-5).
        Follow-ups it left:
    - [ ] Run the Phase 5 Linux checks
          ([§ Phase 5](suspend-resume-linux-verification.md#phase-5)): the
          acceptance checks (an idle sandbox suspends within `after` + 30s, a
          busy one never does, and a pressured node suspends opted-in
          instances before `evict-bursting`), the quiet-connection check, a
          local suspension across a lost heartbeat, and the balloon on both
          VMMs. A real `exec` idle suspend was not feasible on macOS: there
          are no cgroups, so no usage reports and no idle tracking.
    - [ ] A pressure suspend's report is held in worker memory until the
          manager applies it. A worker restart in that window loses the
          record: the checkpoint stays pinned with no row and the allocation
          relaunches cold. Persist pending local suspensions in the worker
          state store
          ([call 19](calls/suspend-resume-and-oversubscription-plan_calls.md#19-the-suspend-step-acts-first-reports-a-local-suspension-and-the-desired-set-is-its-acknowledgment-phase-5)).
    - [ ] Turn on `free_page_reporting` for Firecracker guests once the pinned
          release is confirmed to accept it (verification item 5.5). Off
          today because older supported releases reject the field.
    - [ ] The idle policy cannot act on a node without resource enforcement,
          and a raw host port (no ingress chain) is invisible to `network` and
          `connections`. Both are in the public limitations
          ([calls 20, 59](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [ ] Revisit the 3-minute `idle.after` default with data from real agent
          fleets (plan §12, open question 5).
    - [ ] The idle sweep tries each idle allocation at most once a minute and
          logs skips at debug. Surface a persistent skip (a node that cannot
          checkpoint) on the instance as a `detail`.
  - [x] Phase 6: fork. `ForkInstance` (`count` up to 64, `checkpoint`,
        `spread` refused with a hint while checkpoints were local, made to
        work at integration,
        `idempotency_key`, `job:write` plus `snapshot:write`), forks named
        `<origin>-f<n>` waiting as `pending_op = fork` until the live
        capture lands, then pinned to the origin's node; `CreateInstance(s)`
        `from_snapshot` (`snapshot:read`); named snapshots with `legion
        snapshot alias set|delete` (`SetSnapshotAlias`,
        `DeleteSnapshotAlias`); per-fork volume through the `local-dir`
        `clone` op; per-fork MAC, TAP (`lgf...` on the origin TAP's
        bridge, Firecracker `network_overrides`, cloud-hypervisor
        `config.json`), hostname, token, and entropy; `onRestoreFailure` for
        forks (`fail` stops); fork checkpoints spent once restored;
        `pause_ms` on snapshots; `instance.forked`, `instance.fork_failed`;
        `legion_instance_fork` and `legion_snapshot_alias_set` MCP tools.
        [docs](../docs/suspend-and-resume.md#fork) ·
        [calls 22, 23, 24, 46, 64, 76, 77, 83, 86, 91, 114, 116, 125](calls/suspend-resume-and-oversubscription-plan_calls.md) ·
        [Linux verification](suspend-resume-linux-verification.md#phase-6).
        Follow-ups it left:
    - [ ] Run the Phase 6 Linux checks
          ([§ Phase 6](suspend-resume-linux-verification.md#phase-6)): three
          real memory forks diverge in hostname, RNG, MAC, and token; TAP
          creation and bridging on Firecracker 1.12+ and cloud-hypervisor;
          page sharing between Firecracker forks; a `container` disk fork.
          Run `sandbox_fork_divergence_*` (`#[ignore]`d, KVM). The
          Linux-only `TapHost::master` and `set_master` were never compiled
          on Linux by this phase.
    - [ ] cloud-hypervisor forks get their TAP through a rewritten
          `config.json`. If a cloud-hypervisor version restores net devices
          from `state.json` instead, switch to `net_fds` on restore.
    - [ ] cloud-hypervisor forks share no memory pages. Lazy memory restore
          now uses `memory_restore_mode=ondemand`, but that copies each page
          into each guest's own RAM (`UFFDIO_COPY`), so forks still share
          nothing. Sharing needs `memory_restore_mode=copyonwrite`, which is
          on cloud-hypervisor `main` and not in a release as of v53
          ([lazy memory plan](plans/lazy-memory-restore-plan.md) Phase 4).
    - [ ] Guest addresses are copied into forks: a static IP collides on the
          bridge. Now stated up front in the fork docs and the agent
          sandboxes guide. Reassign addresses in the resume handler (DHCP
          renew, or an address from the identity disk) if platforms need it.
    - [x] A disk fork or a cold-booted fork of a fixed-TAP task reused the
          origin's TAP. A disk fork of a TAP-wired sandbox is now refused at
          the API, and a fresh boot on a held TAP fails with a named reason
          ([call 87](calls/suspend-resume-and-oversubscription-plan_calls.md)).
          A per-instance TAP for fresh boots is still open, so a fixed-TAP
          task runs one instance per node.
    - [x] A fixed TAP is refused at submit (and `job scale`) wherever two
          guests could share a node: a keyed task, `count` above one, or a
          `createFirst` rollout. Disk forks of auto-TAP sandboxes are now
          allowed, and the fixed-TAP disk-fork refusal remains only for jobs
          stored before the rule
          ([calls 135, 136](calls/suspend-resume-and-oversubscription-plan_calls.md#135-a-fixed-tap-is-refused-wherever-two-guests-could-share-a-node-not-removed),
          [docs](../docs/task-runtime-sandbox.md#networking)). Superseded:
          the gated plan's Phase 4 removed fixed TAPs altogether, with these
          refusals.
    - [x] A fork held the origin's pre-capture workload-identity token, good
          for one TTL. Fork and manual snapshot captures now raise the
          instance's identity epoch (`legion.io/epoch`), the `legion` auth
          method refuses earlier tokens, and the origin's node re-issues at
          once from `TaskAllocation.identity_epoch`. A suspend does not move
          the epoch
          ([calls 137, 138](calls/suspend-resume-and-oversubscription-plan_calls.md#137-fork-and-snapshot-captures-retire-the-origins-earlier-tokens-with-a-per-instance-identity-epoch),
          [docs](../docs/workload-identity.md#forks-and-snapshots)).
    - [ ] ACL tokens minted with `legion login --method legion` before a fork
          are copied into every fork and stay valid for `acl.loginTtl` (24h).
          Record the instance and epoch on such tokens, or clamp them to the
          identity token's expiry
          ([call 139](calls/suspend-resume-and-oversubscription-plan_calls.md#139-acl-tokens-minted-from-an-identity-token-survive-a-fork)).
    - [x] A resume, fork, or rollback whose restore failed under
          `onRestoreFailure = "cold"` was visible only as an event and a
          `detail` string. It is now `InstanceInfo.cold_boot`, persisted on
          the instance until the next resume or restore starts: `active
          (resumed cold)` in `instance list`, a `resumed cold at ...` line in
          `instance get` and `start --wait`, `-j` and MCP output, and a UI
          badge ([call 140](calls/suspend-resume-and-oversubscription-plan_calls.md#140-a-cold-boot-in-place-of-a-restore-is-a-persisted-record-on-the-instance),
          [docs](../docs/suspend-and-resume.md#when-a-resume-cannot-restore)).
    - [x] A fork stopped while it waited for its checkpoint booted on an
          empty volume when started. A new instance now keeps its restore
          source across a stop and restores when started
          ([call 84](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [x] Fork ids under `requireId = true`: verified derived
          `<origin>-f<n>` ids work, and an origin id too long to suffix is
          refused there instead of given a minted id
          ([call 77](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [ ] Caller-supplied fork ids (`ForkInstanceRequest.instance_ids`),
          for platforms whose `requireId = true` naming must choose
          fork ids themselves.
    - [x] Cross-node fork and `--spread`: done at the Phase 6 and 7b
          integration. `--spread` publishes the memory checkpoint and places
          each fork on a node no sibling runs on
          ([call 24](calls/suspend-resume-and-oversubscription-plan_calls.md),
          [docs](../docs/suspend-and-resume.md#spreading-forks-across-nodes)).
    - [ ] Spread is enforced on a fork's first placement only. A spread
          fork rescheduled later (node lost, stop and start) can land next
          to a sibling. Persist the spread group on the instance if that
          matters.
    - [ ] A spread fork placement can run preemption, then find every
          candidate taken by siblings and skip, wasting the eviction. Skip
          preemption for a spread restore whose siblings cover the
          preemptable nodes.
    - [ ] A spread fork of a TAP-wired sandbox on another node joins the
          bridge of the task's configured TAP there, and fails by name
          without one. Verify on Linux with two bridged nodes (verification
          item 6.7).
  - [x] Phase 7: content locality (publishing, location index, `locality`
        score, `durability = "store"`, `snapshot warm`). Executes the artifact
        cache plan's Phases 1 and 3.
    - [x] Phase 7a: the parts that need no snapshots.
          [docs](../docs/artifacts.md#content-locality) ·
          [calls 39, 92, 94, 103, 104, 105, 121, 122](calls/suspend-resume-and-oversubscription-plan_calls.md#39-weightslocality-defaults-to-10-the-day-phase-7a-ships)
      - [x] `ContentPublisher` trait with `s3` and `file` backends
            (`src/content_publish.rs`), worker `contentStore { url, auth }`,
            idempotent publish by digest, published URL recorded per digest
            in the content store index, reads through the ordinary fetchers
            (`published_artifact`). `ContentPublishing::publish` is the seam
            7b calls.
      - [x] Location index: `content_locations` deltas with an epoch on the
            heartbeat, full set on a worker's first heartbeat and on
            `content_resync`, entries of at least 1 MiB, held in memory on
            the manager (`src/storage/content_index.rs`)
      - [x] `locality` score term, `schedulerSettings.weights.locality`
            (default 1.0), wired into the cached-batch path and
            `score_node_for_context`. Content set is pinned artifact digests
            plus the `Scheduler::restore_content` hook (empty today).
      - [x] `legion_scheduler_locality_placements_total{outcome}` metric
      - [x] `scheduler_bench` `locality` group (content present / absent)
    - [x] Phase 7b: durable checkpoints and cross-node resume.
          [docs](../docs/suspend-and-resume.md#durability-surviving-the-node) ·
          [calls 8, 15, 36, 42, 49, 62, 69, 73, 78, 88, 96, 108, 126](calls/suspend-resume-and-oversubscription-plan_calls.md#8-a-published-checkpoint-needs-instance-data-that-can-move-so-node-local-volumes-are-refused-rather-than-copied-phase-7b) ·
          [Linux verification](suspend-resume-linux-verification.md#phase-7b)
      - [x] `suspend.durability = "store"` and `snapshot create --publish`:
            the snapshot is `local` once captured (the instance is
            `suspended` at once), the node publishes each part through
            `ContentPublishing::publish` on the checkpoint channel
            (`op = publish`), and the row moves to `published` with each
            part's URL in the manifest. `snapshot.published`,
            `snapshot.publish_failed`, `legion_checkpoint_publish_*` metrics.
      - [x] Refused loudly: a node-local instance volume at submit (built-in
            drivers) and admission (plugin drivers advertising `nodeLocal`),
            and a node without the `content-publish` capability at
            `instance suspend`. A failed upload stays `local` with the
            reason.
      - [x] Cross-node restore fetch: missing parts are fetched on the
            staging thread through `ArtifactStager::fetch_into_store`
            (`published_artifact`), verified by digest, stored in the node
            content store, pinned to the allocation until it starts.
      - [x] Node loss: `published` snapshots survive node removal, and a
            published resume does not wait for a lost node.
      - [x] Placement: no hard pin for a published restore, `compat` checked
            on every candidate, `locality` over the manifest parts. Keyed on
            `restore_from`, so fork and `create --from-snapshot` (Phase 6)
            get cross-node placement once their snapshot is published.
      - [x] `legion snapshot warm <id|name> --constraint k=v` (`WarmSnapshot`,
            `snapshot:read`, MCP `legion_snapshot_warm`, capabilities
            catalog, `acl_endpoints`), `op = warm`, `snapshot.warmed` and
            `snapshot.warm_failed`.
      - [x] Fixed: index candidate truncation dropped the pinned node of a
            stateful task (a `local` restore, a node-local volume) on fleets
            large enough to arm the index. Stateful tasks no longer truncate
            (call 88, `tests/restore_placement_indexed.rs`).
      - [x] `scheduler_bench` `restore` group (`local_pinned`,
            `published_floating`), run once on a loaded host
            ([plan §9](plans/suspend-resume-and-oversubscription-plan.md#9-testing-plan)).
    - [ ] Run the Phase 7b Linux checks
          ([§ Phase 7b](suspend-resume-linux-verification.md#phase-7b)): a
          real MinIO publish of a memory checkpoint, and a resume on a second
          KVM node after the first is destroyed (the §13 Phase 7 acceptance
          check). Record publish and fetch times.
    - [ ] Volume migration: a `local-dir` instance volume cannot follow a
          published checkpoint, so `durability = "store"` is refused with
          one (call 8). A driver op that publishes and recreates a volume
          would lift this.
    - [x] Encrypt published checkpoint parts (plan open question 4, call
          36): every part is sealed under a per-part key derived from the
          keyring's checkpoint key, with no plaintext mode
          ([plan](plans/checkpoint-encryption-plan.md) ·
          [calls](calls/checkpoint-encryption-plan_calls.md) ·
          [docs](../docs/suspend-and-resume.md#published-checkpoint-encryption)).
          Done entry under KV & secrets.
    - [ ] Rotate the checkpoint key by hand (`legion keyring checkpoint-key
          rotate`): mint a new current key and keep old ones while a
          manifest's `keyId` names them. Shares its reference count with
          published-part GC below
          ([calls 8](calls/checkpoint-encryption-plan_calls.md#8-manual-checkpoint-key-rotation-is-deferred-with-keyid-in-the-manifest-now)).
    - [ ] A restore fetches every non-zero chunk of a part it lacks, even
          when the node holds an earlier checkpoint of the same session or
          another sandbox of the image. Keep a node-local chunk index and
          copy held chunks instead, and keep released checkpoint parts as
          evictable cache rather than deleting them at once
          (`release_owner_pins`), so a session moving back to a node fetches
          only what changed
          ([chunked publish plan Phase 2](plans/chunked-checkpoint-publish-plan.md#8-implementation-phases),
          [calls 8](calls/chunked-checkpoint-publish-plan_calls.md#8-a-restore-still-fetches-every-non-zero-chunk)).
    - [ ] A publish rehashes the whole part even when one page changed. On
          Firecracker, use the dirty bitmap to reuse clean chunks' digests
          ([chunked publish plan Phase 3](plans/chunked-checkpoint-publish-plan.md#8-implementation-phases)).
    - [ ] Measure chunked publish and fetch against a real MinIO and S3
          bucket, and memory dirtiness on real Firecracker and
          cloud-hypervisor guests. The chunk numbers so far come from a QEMU
          TCG guest and the `file://` store
          ([calls 4](calls/chunked-checkpoint-publish-plan_calls.md#4-the-default-chunk-size-is-1-mib)).
    - [ ] Measure sealed publish and fetch against a real MinIO in the Phase
          7b Linux checks (above), next to the plaintext numbers.
    - [ ] Hash the node content store with `ring` (imports, `verify`,
          `copy_out`). The `sha2` crate runs several times slower than the
          CPU's SHA instructions on Apple silicon, and switching the sealer's
          hash alone took a 2 GiB publish from 4.6 s to 1.35 s
          ([calls 6](calls/checkpoint-encryption-plan_calls.md#6-publishing-hashes-while-it-seals-with-ring-replacing-the-separate-verify-pass)).
    - [ ] Garbage-collect published parts: Legion never deletes from the
          content store (call 62). A reference-counted sweep, or document a
          lifecycle-rule recipe per provider. Chunked publishing makes an
          age rule of "longest-lived checkpoint plus `dedupWindow`" safe
          ([calls 2](calls/chunked-checkpoint-publish-plan_calls.md#2-chunks-dedupe-per-job-and-per-dedup-window-not-cluster-wide)),
          so a sweep is now about cost, not safety. Spent spread-fork checkpoints
          add to this: their node copy is reclaimed
          ([call 83](calls/suspend-resume-and-oversubscription-plan_calls.md)),
          their published parts are not.
    - [x] Retry a failed publish: the manager re-sends `op = publish` after
          one minute, then five, three attempts in all
          ([call 42](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [ ] A manual `snapshot publish <id>` for a snapshot whose attempts are
          spent.
    - [x] Published checkpoints are unpinned on their node once the upload
          succeeds, so the content store evicts them like any cache entry,
          and a resume there pins what is present or fetches what was
          evicted. `node usage` counts only `local` checkpoints as suspended
          on a node
          ([call 131](calls/suspend-resume-and-oversubscription-plan_calls.md#131-published-checkpoints-are-unpinned-on-their-node-content-store-follow-up),
          [docs](../docs/suspend-and-resume.md#what-happens)).
    - [ ] Check a published resume for room to fetch its parts. The
          checkpoint-room filter covers fresh placements only, so a resume
          onto a node without its parts and without room fails in the fetch
          ([call 134](calls/suspend-resume-and-oversubscription-plan_calls.md#134-a-resume-is-still-not-checked-for-room-for-its-parts-content-store-follow-up)).
    - [x] Placement did not know which workers can fetch published parts.
          Workers advertise `checkpoint-fetch`, and published restores and
          warms only go to them
          ([call 50](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [ ] A pressure suspend stays `local` even with `durability =
          "store"`, and an idle suspend that cannot publish is skipped
          ([call 44](calls/suspend-resume-and-oversubscription-plan_calls.md)).
          Publish pressure checkpoints once the node has recovered, and
          surface the idle skip on the instance.
    - [ ] Warms are leader-local and lost on manager restart (call 96).
          Show queued and finished warms (`snapshot get` or a status RPC).
    - [x] The binder paired restoring instances with allocations in its
          own order, so restores with different `compat` placed in one plan
          could be crossed. Placements now carry the planned instance id
          ([call 16](calls/suspend-resume-and-oversubscription-plan_calls.md)).
    - [ ] With `plan_candidates.count = 0` (format-1 plans) placements carry
          no instance id, so restores bind in the binder's order again.
          Carry the id in format 1 too, or drop the kill switch.
    - [x] Fill `Scheduler::restore_content` with a restore manifest's sized
          memory parts (`RestorePin.manifest_json`), so a restoring
          instance's checkpoint counts toward its placement's content set.
    - [ ] Rerun the `locality` bench group, `scheduler_speed`, and
          `candidate_construction` before/after on a quiet host. The 7a
          numbers in the
          [plan §9](plans/suspend-resume-and-oversubscription-plan.md#9-testing-plan)
          came from a shared, loaded machine.
    - [ ] Multi-manager clusters: the location index lives on whichever
          manager serves a node's heartbeats, so the scheduling manager
          scores nodes that heartbeat a peer as cold. Forward content
          reports to the scheduler-lane leader (as liveness digests do) or
          index on every manager.
    - [ ] `contentStore.auth.nodeIdentity`: exchange a node identity token
          for AWS credentials (STS `AssumeRoleWithWebIdentity`). Rejected
          with a hint today.
    - [ ] Image layers in the content set, once the content store holds OCI
          blobs by digest (cache plan Phase 1 per-blob storage).
  - [x] Integration of Phases 5, 6, and 7b: proto fields renumbered per
        message, migrations ordered (`pause_ms`, then `durability` and
        `published_at`, then the publish retry columns), one ranked calls
        list, and the cross-phase fixes: `--spread` forks, the restore binder,
        `checkpoint-fetch`, the publish retry, published snapshots as named
        seeds, TAP and stopped-fork fail-loudly fixes, and the `cpus` (MHz)
        correction. [docs](../docs/suspend-and-resume.md#spreading-forks-across-nodes)
    - [ ] `resources.cpus` reads as a CPU count but is an alias for `cpu` in
          MHz, so `cpus = 1` reserves 1 MHz. Submit warns under 10 today.
          Deprecate the alias (a warning on any use, then a refusal in a
          later release).
    - [ ] Run the integration's Linux checks
          ([§ Phase 6](suspend-resume-linux-verification.md#phase-6) items
          6.7 and 6.8): a spread fork across two KVM nodes, and the named
          TAP failures.
  - [x] Resume guards (Track 2): `suspend.reserve` as `false`/`true`, a
        ratio, or `{ memory, cpu }` (a partial hold is the suspended
        allocation row with its resources cut to the share), make-room suspends for a
        resume pinned to a full node (manager `suspend.makeRoom`, reason
        `make_room`, `instance.make_room`, `legion_make_room_suspends_total`),
        the `snapshots` content budget reported on the heartbeat and enforced
        as a placement filter for new memory-suspend instances, held share in
        `instance get`, and the `suspended:` and `checkpoints:` lines in
        `node usage`.
        [docs](../docs/suspend-and-resume.md#capacity-for-resumes) ·
        [calls 5, 6, 10, 37, 47, 60, 109, 111, 128](calls/suspend-resume-and-oversubscription-plan_calls.md#5-a-resume-on-a-full-node-makes-room-by-suspending-idle-suspendable-work-there-resume-guards) ·
        [Linux verification](suspend-resume-linux-verification.md#track-2).
    - [ ] Run the Track 2 Linux checks
          ([§ Track 2](suspend-resume-linux-verification.md#track-2)): a
          make-room round with real sandbox memory captures, and a `snapshots`
          budget filled by real checkpoints. Tested with the heartbeat
          standing in for the worker on macOS only.
    - [x] `scheduler_bench` A/B against main, two rounds in opposite order
          ([PR #15](https://github.com/mikenomitch/legion/pull/15)).
          Candidate construction and plan-only are flat within noise.
    - [ ] Plan-and-apply is about 8% slower than main (`plan_under_fill`
          plan_and_apply +6 to +11%, `scheduler_speed` 1k×1k +7 to +13%, both
          rounds), while plan-only is flat, so the cost is in the apply
          transaction. Profile it: likely the extra allocation columns and
          the oversubscription and held-share bookkeeping per placement.
    - [ ] The budget filter checks one checkpoint per placement against the
          last report, so a plan placing many memory-suspend instances can
          still overfill one node's budget. Count checkpoints the plan itself
          places if that shows up
          ([call 47](calls/suspend-resume-and-oversubscription-plan_calls.md#47-the-checkpoint-budget-is-a-placement-filter-on-soft-state-for-fresh-placements-of-memory-suspend-tasks-only-resume-guards)).
    - [ ] A held share is fixed when the suspend lands: a changed
          `suspend.reserve` does not re-charge instances already suspended
          ([call 37](calls/suspend-resume-and-oversubscription-plan_calls.md#37-the-suspendedreserve-floor-lives-under-resourceenforcement-needs-no-cgroups-and-applies-when-a-suspend-lands-resume-guards)).
    - [ ] Make-room only knows an instance is quiet from nodes that send usage
          reports. Without `resourceEnforcement.enabled`, only
          `onPressure = true` makes an instance eligible. Its cooldown
          (2 minutes) is a constant.
    - [x] `ListNodeUsage` fills the suspended summary too: both RPCs build
          their reply through one `node_usage_reply`, which calls
          `fill_suspended_capacity`.
  - [x] Track 1, the agent-sandbox use case
        ([guide](../docs/guides/agent-sandboxes.md) ·
        [calls 14, 17, 30, 43, 52, 53, 54, 81](calls/suspend-resume-and-oversubscription-plan_calls.md#14-suspend-and-snapshots-ride-the-allocation-payload-and-the-worker-knows-its-instance) ·
        [Linux verification](suspend-resume-linux-verification.md#track-1)).
    - [x] Auto TAPs: `config.network = { mode = "tap", tap = "auto", bridge
          }` creates a TAP per instance with the fork naming, attaches it to
          the bridge, recreates it on restore, and deletes it on destroy.
          Many memory-checkpointable sandboxes with real networking per node.
          [docs](../docs/task-runtime-sandbox.md#networking)
    - [x] Idle detection on microVMs: bytes the guest sends on its TAP count
          for `network`. `connections` is refused on sandboxes that are not
          gated, `exec` is refused on a sandbox pinned to a VMM and warned
          otherwise. [docs](../docs/suspend-and-resume.md#what-each-signal-sees-on-a-sandbox)
    - [x] Sandbox identity refresh with no reboot: file delivery under
          `/legion/identity` through the identity disk, env + `noop` on
          memory-checkpointable sandboxes, a warning for `restart`.
          [docs](../docs/workload-identity.md#sandboxes)
    - [x] macOS idle suspend: `container stats` feeds the usage sampler, and
          an ignored end-to-end test (`apple_container_idle`) suspends an idle
          Apple-container sandbox and resumes it cold on its volume.
          [docs](../docs/suspend-and-resume.md#macos-dev-loop)
    - [x] Fixed on the way: the allocation payload never carried `suspend`
          or `snapshots`, so no real node reported idle time, honored
          `onPressure`, or launched a sandbox ready for memory checkpoints
          (now `TaskAllocation.task_suspend`/`task_snapshots`). Apple dev
          sandboxes could not start (`--` before the command) and two local
          clusters removed each other's containers (names now carry the
          node).
    - [ ] Run [§ Track 1](suspend-resume-linux-verification.md#track-1) on a
          KVM host: per-instance TAPs on a bridge with a pool, TAP counters,
          an in-place identity disk refresh in a running guest, and forks of
          a TAP sandbox.
    - [ ] No signal keeps a TAP sandbox awake through a quiet outbound call
          (a harness waiting on a model). `connections` is refused there.
          The `hint` signal (Phase 8) is the real fix.
    - [x] Auto-TAP egress is whatever the operator enforces on the bridge.
          Per-sandbox egress policy needs gated networking, which now takes
          memory checkpoints (gated plan Phase 1), so agent sandboxes can
          have both.
    - [ ] Dev mode (Apple `container`) does not mirror env-delivered tokens
          into `/legion/identity`, so env tokens there keep their boot value.
    - [ ] The `container stats` call runs synchronously on the heartbeat
          path every 10s while a dev sandbox runs. It once hung there
          indefinitely on a wedged `container` service; it is now killed past
          5s (`apple_container::STATS_TIMEOUT`), but still costs up to that on
          the heartbeat. Move it off the loop.
    - [x] Track 1's calls are inserted at their ranks and every citation
          renumbered.
  - [ ] Phase 8: fast paths, each needing its own review (wake on connect,
        `hint` idle signal, UFFD lazy memory, diff snapshots). Deferred.
    - [x] Lazy memory restore, Phase 1, in flight in the "lazy memory
          restore" PR: restores trust the content store's digest instead of
          rehashing every part, cloud-hypervisor v52+ restores on demand
          (`memory_restore_mode=ondemand`), Firecracker `eager` reads the
          memory file ahead, the first resume after a cloud-hypervisor
          `--restore` waits for the restore instead of failing at 10 s, and
          `config.memory_restore = "lazy" | "eager"` (default `lazy`).
          [plan](plans/lazy-memory-restore-plan.md) ·
          [calls](calls/lazy-memory-restore-plan_calls.md) ·
          [docs](../docs/task-runtime-sandbox.md#how-memory-comes-back)
    - [ ] Run the lazy memory KVM checks
          ([plan §9](plans/lazy-memory-restore-plan.md#9-testing-plan)):
          `restore_ms` at 1, 4, and 8 GiB on both VMMs and cloud-hypervisor
          v51 to v53, lazy against eager, a touch-all workload after the
          resume, and the no-userfaultfd fallback. Nothing in Phase 1 ran on
          a real VMM. Put the measured numbers in the "Restore time"
          paragraph of
          [How memory comes back](../docs/task-runtime-sandbox.md#how-memory-comes-back).
          If an on-demand restore fails where the userfaultfd probe said
          yes, add a one-time copy retry gated on the VMM exiting before the
          resume ([calls #4](calls/lazy-memory-restore-plan_calls.md)).
    - [ ] Lazy memory Phase 2: start a published restore while its memory
          streams from the bucket (each part is an index of separately
          sealed chunks since chunked publish, so a page server can fetch
          the chunk a fault needs). Today every part is fetched whole
          before the restore starts.
    - [ ] Lazy memory Phase 3: record each guest's post-resume working set
          and prefetch it on the next restore (REAP), the fix for the
          post-resume fault cost of lazy restore.
    - [ ] Nothing rescans node content store blobs now that a restore no
          longer rehashes snapshot parts. Decide whether a background scrub
          belongs in the store ([plan](plans/lazy-memory-restore-plan.md)
          open question 2).
    - [ ] Content store hashing runs in software on aarch64 workers:
          Legion builds `sha2` without its `asm` feature, which is what
          turns on the ARMv8 SHA-256 instructions there (about 2 CPU-seconds
          per GiB, measured). Every capture, import, and fetch check pays
          it. `ring`'s SHA-256, already a dependency, uses the hardware on
          both aarch64 and x86_64 and would remove it without a new crate
          ([plan §1](plans/lazy-memory-restore-plan.md#1-problem-statement)).
- [ ] Split a task runtime's implementation into parts — isolation vs. runner vs.
      artifact consumption? (idea, needs a plan). The container-runtime plan's
      `resource_enforcement()` interface carves off the first slice of this —
      resource placement moves to worker core, runtimes consume a computed
      assignment — so revisit this idea once that has two real consumers.
- [ ] Shared files between tasks in the same allocation —
      [proposal](plans/shared-alloc-dir-proposal.md), unimplemented, and blocked
      on allocations holding more than one task
- [ ] Use a virtual filesystem for the alloc directory (idea)
- [ ] **Finish moving worker blocking work off the task-manager lock (audit
      W3).** Launches are off it. Still under it, on async threads:
      `reap_terminating`, `record_failure` and the self-fence path (destroy and
      volume unmount), plus `prepare`/`start` for `ratify_claim`,
      `restart_allocation`, edge-boot provisional starts, and snapshot
      restores (a resume, fork, or `RestoreInstance` start; they record their
      outcome on the checkpoint channel from inside the locked start, see
      `begin_task_allocation` and `spawn_staging_thread` in
      `src/worker/task_manager.rs`). A memory restore no longer rehashes
      every part under the lock, and a lazy one (the default) holds it for
      about the same time at any guest size, but an `eager` one still holds
      it for the whole memory read
      ([lazy memory plan](plans/lazy-memory-restore-plan.md)). `heartbeat_tick`
      also locks a std mutex from async code. Teardown needs a "tearing down"
      state so starts still wait behind stops. [calls #11](calls/architecture-audit-2026-10-09_calls.md).
      The node-check readiness probe (`container system status`) moved off
      the lock in the 2026-10-12 macOS fixes, and every runtime CLI call now
      has a deadline (`crate::util::output_with_timeout`). Teardown on
      reconcile still runs `container stop` under the lock, now bounded at
      30s rather than unbounded.
- [ ] **Linux container ids are not node-scoped.** macOS Apple containers are
      `legion-<node>-<alloc>` and labeled, so stale cleanup checks ownership
      before `rm --force`. Linux OCI ids stay `legion-<alloc>` (shared with
      the cgroup leaf and hostname) and stale cleanup deletes by name, so two
      clusters sharing one host and runtime state dir can still collide.
      See `container_name` in `src/task_plugins/container.rs`.
- [ ] **The worker boot fence is silent (audit W6).** A task fenced at boot is
      never reported as self-fenced, so the manager learns only from its
      absence. A volume left mounted by a boot-fenced task, or by any task
      that died while the worker was down, stays mounted until a later dispatch
      or reclaim. [calls #12](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **Worker graceful shutdown.** The manager now handles SIGTERM (audit M3),
      the worker does not.
- [ ] **The sandbox VMM is a closed enum matched about 76 times, and some
      matches guess.** Several `match`es in
      `src/task_plugins/sandbox_checkpoint.rs` (pause, resume, snapshot,
      around lines 716-733 and 1719) send anything that is not Firecracker
      to cloud-hypervisor's API, so a third VMM would compile and then call
      the wrong one; and the VMM name aliases (`"fc"`, `"ch"`, ...) are
      parsed in four places that disagree (`microvm.rs`,
      `sandbox_checkpoint.rs`, `structs/network.rs`,
      `manager/server/suspend_rpcs.rs`). Make the matches exhaustive and
      parse names once now; a `Vmm` trait when QEMU or libvirt is scheduled
      ([swappable seams plan](plans/swappable-seams-plan.md) §1).

## Rollouts

Plan: [rollout-strategy-plugins](../docs/rollout-strategy-plugins.md) · docs: [rollouts.md](../docs/rollouts.md)

- [ ] A **blue-green** strategy
- [ ] A **canary** strategy
- [ ] **Rollback as a new job version (audit SC1).** Rollback stays a terminal
      rollout state, so a forced reschedule, a KV-driven restart, or `job scale`
      after a rollback still targets the rolled-back config. Nomad re-submits the
      old spec as a new version. Needs a plan (job history and unchanged-resubmit
      detection change). [calls #5](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **`StopRollout` with `revert` can leave the rollout non-terminal.** If the
      re-placement fails, the RPC errors and the rollout's ticks may re-place
      the new version. Pre-existing, documented in
      [rollouts.md](../docs/rollouts.md).

## Artifacts

Plan: [artifact-system-plan.md](plans/artifact-system-plan.md) · docs: [artifacts.md](../docs/artifacts.md)

- [ ] **Artifact cache + pre-warming/pre-pull + scheduler locality scoring** —
      [plan](plans/artifact-cache-and-prewarm-plan.md) (PLANNED). Phases 1 and 3
      are now scheduled inside the
      [suspend/resume plan](plans/suspend-resume-and-oversubscription-plan.md)
      (Phases 3 and 7), amended to add a publish path and an off-raft location
      index in place of the `legion.artifact_cache` dynamic attribute.
  - [ ] Phase 1 — content-addressed cache. Shipped as the
        [node content store](../docs/artifacts.md#the-node-content-store) in
        suspend plan Phase 3a, with per-class budgets.
    - [x] CAS + sqlite index, sink swap (verified fetches are written into
          the store), size budget + `minFreeDisk`, LRU GC loop, pinning,
          single-flight, cache hit/miss in `artifact_states`
    - [ ] Mutable-source caching: a source-keyed entry with TTL revalidation
          through `ArtifactFetcher::resolve()`, and the jobspec `ttl` field.
          Today only artifacts with a `checksum` use the store
          ([call 65](calls/suspend-resume-and-oversubscription-plan_calls.md#65-the-node-content-store-caches-only-digest-pinned-single-files)).
    - [ ] Cache trees (git, `file://` directories) and OCI images/ORAS layers;
          OCI per-blob storage so tags sharing base layers store them once
    - [ ] `legion artifact ls/gc` CLI (and MCP tool) over the node store
    - [ ] Show cache hits in the UI's artifact staging rows (the `cache`
          field is already in `artifact_states`)
    - [ ] Flip `containerImageDesugar` to on by default (needs image caching)
  - [ ] Phase 2 — warm RPCs and policies
  - [x] Phase 3 — cache advertisement + `artifact_locality` scoring. Shipped
        as suspend plan Phases 7a and 7b, with the plan's amendments.
        [docs](../docs/artifacts.md#content-locality)
    - [x] Advertisement: heartbeat deltas into an off-raft location index
          instead of `ReportArtifactCache` and the `legion.artifact_cache`
          dynamic attribute. Enqueues no evals.
          [docs](../docs/artifacts.md#content-locality)
    - [x] Locality scoring as `schedulerSettings.weights.locality` (default
          1.0, `0` restores baseline placement) instead of
          `artifact_locality: 0.3`
    - [x] "Warming one node makes it win placement": `legion snapshot warm`
          (suspend plan 7b) fetches a published snapshot onto matching nodes,
          the location index advertises it, and placement prefers them
          (`a_published_resume_prefers_the_node_holding_its_parts`).
          Warming artifacts is Phase 2's `WarmArtifacts`, still open.
  - [ ] Nodes-page cache card in the UI (left over from Phase 3)
  - [ ] Multiple layers of pre-fetching; tag nodes as pre-warmed; pre-fetch as
        part of the deploy process
  - [ ] Unblocks `artifacts.containerImageDesugar` being on by default
- [ ] `identity` / `nodeIdentity` artifact auth end-to-end. The signing side
      shipped with workload identity; fetch-time use still fails with an
      actionable error.
- [ ] `onFetchFailure` policy distinct from `on_unhealthy` — today fetch
      flakiness consumes restart/move budgets
- [ ] Push container images directly into a worker's artifact cache, so a small
      cluster needs no registry at all
- [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
      image consumption (see *Windows support pass*)

## Stateful & volumes

Plans: [stateful-workloads](plans/stateful-workloads-and-instance-identity-plan.md), [volume-plugin-system](plans/volume-plugin-system-plan.md) · docs: [volumes.md](../docs/volumes.md), [stateful-workloads.md](../docs/stateful-workloads.md)

- [ ] **Session durability and routing back** (builds on session mode; plan
      merged in [#35](https://github.com/mikenomitch/legion/pull/35),
      implementation in [#36](https://github.com/mikenomitch/legion/pull/36)). A worker
      restart ends every unpromoted session, a session cannot sleep,
      finding one fans out to every manager, and every start goes through
      a manager. Plan:
      [session-durability-and-routing-plan.md](plans/session-durability-and-routing-plan.md)
      (IN EXECUTION) · calls:
      [session-durability-and-routing-plan_calls.md](calls/session-durability-and-routing-plan_calls.md).
  - [x] Phase 1: the node starts sessions concurrently (launch out from
        under the task-manager lock, no state-file fsync per session start,
        bounded launch pool).
  - [x] Phase 2: node-local `sessions.db`, adopt sessions on worker restart.
        Full reports are not yet chunked past the message limit; the
        manager's decode limit was raised to 64 MiB instead.
  - [x] Phase 3: the start node's tag in the minted session id, tag-first
        routing with fan-out as the fallback.
  - [x] Phase 4: edge starts with no manager on the path
        (`ListSessionNodes` for an outside consistent-hash picker, session
        tickets, `Worker.StartSession`).
  - [ ] Measure the manager-routed path across three managers, and remove
        its derived-id fan-out (plan §8 item 4). Needs the loadgen session
        mode in the bottleneck plan's gaps
        (`benchmarks/aws/README.md`, "Bottleneck measurements").
  - [x] The start hot path, measured and cut (PR_LINK): the router holds a
        start's room from the pick (no herding of concurrent creates), breaks
        ties at random (no herding across managers), refreshes its view
        single-flight, reads the job in one query with cached task configs,
        and dials workers from the read model's node row and a CA read at
        most once a second. On the node, a session's process start time
        (for its `sessions.db` row) is read off the task-manager lock, a
        session that stages artifacts boots off the lock, an edge start's
        `wait` is woken by its claim instead of polling, and a resume
        refused for room on the session's own node marks it full and skips
        it. Measured with
        `session_start_cost_breakdown_measure`
        ([plan §6.5](plans/session-durability-and-routing-plan.md#65-everything-else-between-here-and-tens-of-thousands-a-second),
        calls 40 to 47).
  - [ ] A session's start still does its filesystem work under the
        task-manager lock: the allocation, task, and log directories
        (`prepare_instance`, `claim_allocation_log_dir`) and the log sink's
        start (`notify_log_sinks_started`), about half a millisecond a start
        on a loaded macOS host, and a session stop rewrites and fsyncs the
        worker state file (`signal_and_terminate`). Each one holds up every
        other start, stop, and heartbeat on the node. Sampled with
        `/usr/bin/sample` on a worker under `legion-loadgen --mode
        sessions`.
  - [x] A session or instance resuming from a checkpoint boots off the
        task-manager lock, like any other start, and the launch's finish
        records the outcome on the checkpoint channel. Several resumes on a
        node now run side by side, and a cloud-hypervisor that hangs
        mid-restore holds one launch slot, not the node
        ([calls](calls/lazy-memory-restore-plan_calls.md) #7,
        `UnlockedLaunch` in `src/worker/task_manager.rs`).
  - [x] Phase 5a: sessions sleep on their own node with `durability =
        "local"`: `suspend.keepFor`, idle suspend, `Worker.SuspendSessions`
        / `Worker.ResumeSession` routed by tag from `instance suspend` /
        `instance start`, eviction (keepFor, envelope gone, minFreeDisk)
        with the `evicted` answer, UNAVAILABLE naming a silent node
        ([docs](../docs/session-mode.md#idle-sessions)).
  - [x] Phase 5b: `durability = "store"` for sessions: suspend and live
        checkpoints publish sealed parts (a per-job session sealing key on
        the envelope payload) and write the session manifest at
        `<contentStore.url>/sessions/<job>/<prefix>/<id>.json`; every
        resume claims the next generation with a conditional write
        (`file://` and `s3://`); `Worker.ResumeSession` with job/task and
        `StartSession.resume_id` resume a session the node never held;
        `instance start` falls back to the manifest when the tagged node
        lost it, is full, or does not answer, probing a running copy first
        (calls 28 to 39;
        [docs](../docs/session-mode.md#moving-a-session)). Not yet run:
        see the open items below.
  - [ ] Phase 5b against real S3 (or MinIO): the `s3://` manifest backend
        (`PutMode::Create` / `Update` with `S3ConditionalPut::ETagMatch`)
        is unit-tested only against `object_store`'s in-memory store
        (`src/session_manifest.rs`), not against a bucket.
  - [ ] Phase 5b with the `sandbox` runtime: publishing and restoring a
        session's memory parts (sealed with the session key, fetched on
        another node) is not covered by a test; the unit tests run `exec`
        sessions, whose disk checkpoint has no parts.
  - [ ] Phase 5b edge move: `StartSession.resume_id`'s probe and
        `StopSession` of the old copy over worker-to-worker TLS has no test
        (it needs two real workers with mesh certificates); the manager
        fallback is covered in `tests/session_integration.rs` with fake
        nodes and a fake bucket.
  - [ ] An edge picker (`legion session nodes`) should set `force_move`
        itself when the node a session's tag names is listed `down` or
        `lost`; today the caller passes `--force-move`.
  - [ ] Manifests of `store` sessions that are never resumed or stopped
        stay in the bucket (calls entry 33); consider a manager-side sweep
        of manifests whose job is gone.
  - [ ] A moved `store` session runs the receiving node's envelope job
        version, not the one it started with (calls entry 29).
  - [ ] Finding a moved session's job scans every job's session tasks
        (calls entry 37); two jobs with a `store` session task of the same
        name are ambiguous.
  - [ ] Evicted and unreachable session answers are given before the job's
        ACL scope is known; have the node return the job id with them so
        the precise scope is enforced first.
  - [ ] Memory checkpoints of sessions (`sandbox` runtime) are not covered
        by a test: the unit tests run `exec` sessions, whose checkpoint is
        a disk one (`a_session_suspends_and_resumes_on_its_node_test`).
        Nor are the periodic checkpoint round, checkpoint on SIGTERM, or
        restore after restart tested end to end; only their decision
        functions are.
  - [x] The managers' batched duplicate sweep and the generation-first
        winner rule (plan §6.7, mechanism 3; #36).
  - [x] No split brain for `store` sessions (plan §6.7): manifest create
        for idempotent `store` starts (`ALREADY_EXISTS` naming the winner),
        superseded-on-write, the fence list (`sessions.fenceListEvery`,
        default 30s), probe-before-move, and self-fencing after
        `stateful.fenceAfter` (default 2m) with resume of owned generations
        on reconnect ([docs](../docs/session-mode.md#no-split-brain)).
        Unit-tested on the `file://` backend.
  - [x] Optional `suspend.checkpointEvery` (memory checkpoints only,
        staggered, skipped past one interval and counted in
        `legion_session_checkpoints_skipped_total`) and
        `suspend.onWorkerStop = "checkpoint"` (a SIGTERM/SIGINT handler in
        the worker, restore on restart) (plan §6.6,
        [docs](../docs/session-mode.md#idle-sessions)).
  - [ ] Phase 6 (conditional, plan §6.8): batched raft session directory,
        only past its trigger.

All deferred from the volume plan v1:

- [ ] **CSI bridge plugin** — the driver contract was designed for it; nothing in
      core has to change
- [ ] **Snapshot / restore / clone / expand** — op names are reserved in the
      contract. Overlaps with sandbox snapshot phases 1–2 and 5 above; build them
      together.
  - [x] `snapshot`, `restore`, `clone`, `snapshot-delete` in the contract,
        dispatched to exec drivers, implemented by `local-dir` in both modes
        (reflink or sparse copy), and covered by the conformance suite
        (suspend plan Phase 3b,
        [volumes.md](../docs/volumes.md#snapshot-restore-and-clone))
  - [ ] A `legion volume snapshot` surface, if one is wanted beyond checkpoints
  - [ ] `expand`
  - [ ] `clone` × node locality: same-node clone native, cross-node fan-out
        snapshot-mediated, `capabilities` reporting `cloneScope: node | cluster`
- [ ] **Multiple volumes per instance** (`stateful.volumes: [...]`) — the schema is
      already keyed by (instance, name), so this is purely additive
- [ ] **Cross-node data migration** — deliberately manual today (backup → delete →
      re-mint → restore) until snapshot/restore land
- [ ] Mount concurrently with artifact staging rather than after it (a real
      start-latency win; nothing in the contract forbids it)

## Service discovery & the mesh

Plans: [service-mesh-plan.md](plans/service-mesh-plan.md) (**authoritative for the mesh**; shipped M1–M10 with two named deferrals), [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) (v1, shipped) · docs: [service-discovery.md](../docs/service-discovery.md), [network-handlers.md](../docs/network-handlers.md)

v1 (catalog, ports, env, `list`/`resolve`/`doctor`) shipped, and so has the
v2 mesh: `connect.upstreams`, the per-node forwarder, pluggable balancing
(`local_first` included), intentions, node-local DNS and VIPs, templates,
mTLS on workload identity, RED metrics with the service graph, and
drain-before-stop — phase by phase, with what each left open, under
[Networking](#networking) below. What the mesh plan's adoption order still
has open:

- [ ] Datapath plugin ABI + bridge/CNI shim (`mode: 'auto'` picking a real
      overlay). The datapath half — overlay, CNI, fingerprinters — is still
      unplanned; it remains the service-discovery plan's item 2.
- [ ] Sandboxed WASM middleware filters on the L7 path (retries, timeouts,
      weighted split and outlier ejection shipped in M2 inside the upstream
      listener). They are the handlers plan's phase 8 — whichever of the two
      plans grows a sandbox first should host the other's filters rather
      than growing a second one.
- [ ] `legion net doctor` citing live-traffic evidence: it walks policy →
      mode → datapath status (M6) but does not yet pull the task's edges
      from the service graph (M8) into its verdict.
- [ ] **A watchable catalog for outside load balancers and meshes.**
      `ResolveService` is unary and `StreamEvents` carries no health
      transitions, so an external proxy (Envoy, nginx, a Consul sync,
      external-dns) has to poll. A `WatchService` stream, or health events on
      `StreamEvents`, would let it follow the catalog; xDS stays out of scope
      ([swappable seams plan](plans/swappable-seams-plan.md) §1).

## Networking

Plan: [network-handlers-plan.md](plans/network-handlers-plan.md) (ingress/egress
chains) · calls: [network-handlers-plan_calls.md](calls/network-handlers-plan_calls.md)

The chain plan covers the proxy/middleware half of this section — ingress,
egress, mTLS, and sandbox capture. The datapath half (overlay, CNI,
fingerprinters) is still unplanned and remains the service-discovery plan's
item 2.

Plan phases (revision 2), in order. Three concepts — policy objects, `gated`
capture, handlers — plus `legion netd` as the process that outlives the
worker:

- [x] Phase 0 / M1 — policy + datapath core (2026-09-02): `netpolicy` types
      and the deterministic merge with provenance, `net_policy` table + the
      seven RPCs + `AclResource::Net`, `legion net policy
      apply/list/describe/delete/show/test` (+ capabilities + MCP tools),
      connect-time rules evaluated in the forwarder before any dial, external
      upstreams (`name` + `to`, TLS originated for `tls://`/`https://`),
      content-addressed policy delivery on the heartbeat with a fail-static
      node cache, `net.denied` / `net.policy_*` events, the three
      `legion_net_*` metric families, `docs/network-handlers.md`,
      `configs/policies/sandbox-egress.ncl`, `configs/jobs/sandbox-egress-demo.ncl`.
      Still open inside M1: the UI (Network page, task/service detail
      sections) and the `accessLog` writer (lands with HTTP mode).
- [x] Phase 1 / M2 — HTTP/1.1 mode (2026-09-02): request-time rules
      (path/method/header) on upstreams in HTTP mode, structured 403s
      (`Legion-Denied-By`, JSON body), built-in `dlp` with a byte cap and
      `log`, the NDJSON access log, and the upstream `http` traffic block —
      per-request timeout, budgeted retries for idempotent methods, weighted
      split by service tag, outlier ejection (the waypoint, folded into the
      upstream listener). h2/h3/WebSocket upgrades rejected or unsupported,
      deliberately. `authz` / `proxy` handlers still skip with a debug log
      until the external-handlers phase.
- [x] M3 — name-is-config (2026-09-02): per-allocation loopback /24 on
      Linux (`127.42.<b>`), `.1` a resolver answering `<key>.svc.legion` with
      the allocation's upstream VIPs (`.2`+) on canonical ports, egress policy
      applied at resolution (`NXDOMAIN` + `net.denied` for a denied name),
      external names forwarded to the host resolvers; `LEGION_DNS`,
      `LEGION_UPSTREAM_HOST_*`, `LEGION_UPSTREAM_VIP_*`; containers get a
      generated `resolv.conf`. macOS: no VIPs, documented. Open inside M3:
      the generated `resolv.conf` for hardened `exec` (lands with gated).
- [x] Phase 2 / M4 — `gated` on Linux for `container` and `exec`
      (2026-09-02): worker-owned empty network namespaces
      (`/var/run/netns/legion-<alloc>`, created ahead of reconcile on the
      heartbeat, removed on departure), every datapath listener bound inside
      (upstream ports, VIPs, resolver, egress proxy), an egress proxy
      speaking HTTP CONNECT / absolute-URI HTTP / SOCKS5 with policy, proxy
      env injected, containers join by OCI namespace path, exec `setns`s
      before exec; effective `network.mode` resolved at placement (task or a
      `require` policy's `mode`); fail-closed while policy is pending or the
      edge-boot gate is shut (`network_gate_for` → Enforced for gated
      allocations, the gate as a marker file), closing edge-boot's deferred
      per-destination policy for the gated state. Not done: a
      `net_capture_unavailable` health signal (a missing namespace fails the
      start with a clear error instead — it is retried each heartbeat), and
      transparent capture (tools that ignore `HTTP_PROXY` have no route).
- [x] Phase 3 / M5 — datapath across worker restarts, and capture health
      (2026-09-02), in a bounded form: the worker persists the datapath's
      inputs (`netd_state.json`: desired set, endpoint feed, policy
      assignments and bodies, owner-only, written when changed) and replays
      them before its first heartbeat, re-binding every listener for the
      allocations that survived the restart; a gated allocation whose
      capture is not ready is reported unhealthy (`net_capture_unavailable`).
      **Not done, deliberately:** the `legion netd` process split with
      `SCM_RIGHTS` listener hand-off (zero dropped connections across a
      worker upgrade) — in-flight connections drop at restart and the gap is
      the process start time; see service-mesh calls entry 17.
- [x] Phase 4 / M9 — `gated` for `sandbox` over vsock (2026-09-02): a gated
      sandbox boots with no NIC and a virtio-vsock device on both VMMs
      (`--vsock` / `"vsock"`), the worker binds the allocation's egress proxy
      and resolver on the VMM's host sockets (`vsock.sock_1080`, `_53`)
      before boot, and the generated init starts `legion guest-agent` (this
      binary copied into the rootfs) which relays `127.0.0.1:1080` and
      `127.0.0.1:53/udp` (DNS-over-TCP framing) to the host over `AF_VSOCK`;
      TAP + gated rejected, dev-mode backends refused for gated; the boot
      gate is the policy-withheld marker; status via `is_running`. **Not
      verified on a real VMM** (no KVM in this environment): host side and
      agent are unit-tested over Unix sockets with the VMMs' naming, so
      `auto` still resolves to `host` for sandboxes until an end-to-end boot
      runs in CI. Also open: the agent shares nothing yet with VM forking or
      microVM `exec` (the vsock device is there; the exec channel is not),
      and a musl-static agent for images whose libc cannot run the worker
      binary.
- [x] Phase 5 / M6 — ingress chains, drain-before-stop, `legion net doctor`
      (2026-09-02): a task with an ingress chain (its own or a `require`
      policy's) gets a private twin for every advertised port at placement;
      the datapath binds the advertised port on the host, judges each
      accepted connection against the chain (peer address; the request head
      in HTTP mode), splices or proxies to the twin inside the gated
      namespace, and fails closed while gated with policy pending or the
      boot gate shut. `stop.drain`: a service task leaves the catalog, keeps
      serving one heartbeat interval (or `stop.drain`), then takes the
      ladder; starts are held back meanwhile. `legion net doctor` /
      `legion_net_doctor` walk policy → mode → per-allocation node status
      (`NetAllocStatus` on the heartbeat; shown under **Datapath** in the
      UI's task Network section). Not done: the autoscaler's wake-on-request
      hook (the listener exists; the scale-from-zero signal is the
      autoscaler plan's), PROXY protocol v2 to the task (M7), persisting the
      drain/grace sets across a worker restart (a restart mid-window
      signals at restore, as grace already did).
- [x] Phase 6 / M7 — external handlers and mTLS (2026-09-02): `authz`
      webhooks (facts in, cached verdict out, `onError`) and `proxy` hand-offs
      (PROXY v2 with Legion TLVs on TCP, absolute-form + `Legion-*` headers on
      HTTP; targets `service` / `node` / sidecar `port`, endpoints fed on the
      heartbeat as `for_handler`) on every path — upstream splices, HTTP mode,
      the gated egress proxy, ingress; per-hop health in `NetAllocStatus`,
      `legion net doctor` and the UI; `legion net handler test`. TLS: ingress
      chains terminate `server`/`mutual` with BYO files or the mesh; the mesh
      is a manager-held CA (`net_mesh_ca`, control lane) issuing 24h node
      certificates over `IssueNodeCertificate`, node-to-node mTLS with the
      workload identity attributed in a PROXY v2 TLV, `requireIdentity`,
      `mesh_tls` on the endpoint feed, `legion net ca show|rotate`. Not done:
      the kTLS/`splice` passthrough (a terminated connection is copied in
      user space — measure before adding), `local_first` balancing (M10 with
      templates), certificate revocation beyond the lifetime, an ingress
      `proxy` hop into a gated task's namespace, and workload certificates on
      demand (`certFrom = "legion"` on a task's *own* TLS — deferred with the
      plan's "workload certs on demand").
- [x] M8 — service graph (2026-09-02): every calling datapath (upstream
      listeners, the gated egress proxy, HTTP mode) records per-(upstream,
      tag) samples — requests/connections, errors, denials, latency, bytes —
      drained on the heartbeat (`net_edges`, restored on failure, bounded at
      4,096 keys); the manager folds them into 15 minutes of per-minute
      buckets per (caller job, task, service, tag); `ListServiceEdges` merges
      declared upstreams (`task_config.connect`, latest job config) with the
      allow verdict and the observed RED; `legion service edges`, MCP
      `legion_service_edges`; UI: the Network page opens with the graph
      (callers → services, width by rps, dashed silent, red for denials /
      errors / disallowed) over the edges table, Service detail gains
      **Callers**. Not done: ingress-side attribution (edges are
      caller-recorded), persistence across manager failover (the window
      refills from heartbeats), and a manager-side unit test of the fold —
      the ledger and the RPC path are tested, the fold arithmetic is not.
- [x] M10 — templates and `local_first` (2026-09-02): `connect.templates`
      render files from the upstreams and the endpoint feed (`service`,
      `range endpoints`, `env`) before launch and on every heartbeat that
      changes the result, atomic writes, `changeMode` noop/signal/restart
      with `coalesce` and `splay`, fail-static by construction, validated at
      submit (unknown action, undeclared upstream, escaping destination);
      render problems in `NetAllocStatus.problems`. `balance = "local_first"`
      in the registry, widening when no local endpoint is healthy. Not done:
      `{{ kv }}` inside connect templates (KV injection already renders
      manager-side), conditionals, and rendering into a *sandbox* guest
      (the file lands in the host task dir; the guest sees it only where the
      runtime mounts that dir).
- [ ] Phase 7 — UDP (SOCKS5 `UDP ASSOCIATE`) and transparent capture (TUN +
      userspace stack in the gated namespace)
- [ ] Phase 8 — sandboxed WASM handlers (in-process `cdylib` handlers are
      deliberately not planned — see calls entry 4)
- [ ] Publish `pool` and `nodeId` into worker `config_attrs`
      ([daemon.rs:281](../src/worker/daemon.rs)) so `count.per` can express
      one-per-node handler jobs — also what the pool-scheduling item above
      needs

Deliberately not in the plan (see its §5): TLS interception / MITM — external
upstreams are the body-visible route; rate limits and quotas; a cluster-edge
load balancer.

Still unplanned, and not covered by the plan:

- [ ] Overlay network. Unix-domain-socket transport is on the table.
- [ ] CNI, or a custom networking plugin that can speak CNI
- [ ] Networking fingerprinters (so service discovery can pick a datapath per node)
- [ ] Port forwarding
- [ ] Cluster-edge ingress load balancer. The plan's ingress is node-local,
      in front of one allocation; a cluster edge is separate work
- [ ] `clientCa = { kv = "<path>" }` for `mutual` ingress, so a private client
      CA can live in KV like the server certificate. Not in
      [kv-cert-delivery-plan.md](plans/kv-cert-delivery-plan.md).
- [ ] A built-in ACME client (answering HTTP-01 from the ingress datapath).
      Only worth it once the cluster edge above exists. Until then, the
      renewer job in `configs/jobs/cert-renewer.ncl` covers it.

Credential injection: [egress-credential-injection-plan.md](plans/egress-credential-injection-plan.md)
· calls: [egress-credential-injection-plan_calls.md](calls/egress-credential-injection-plan_calls.md)
· docs: [network-handlers.md#credential-injection](../docs/network-handlers.md#credential-injection).
Phases 1 and 2 shipped (see Done, KV & secrets).

- [x] **Reach `connect.upstreams` from a gated `sandbox` guest**, so
      credential injection keeps API keys out of gated sandboxes and their
      memory checkpoints (vsock relays; see Task runtimes, gated sandboxes).
      TAP-wired and loopback-only sandboxes are still open there
      ([calls #11](calls/egress-credential-injection-plan_calls.md)).
- [ ] Credential injection Phase 3: webhook provider
      (`from = { handler = "<name>" }`) and non-header placements (query
      parameter, basic auth).
- [ ] Credential injection Phase 4: opt-in per-request
      `net.credential_used` audit events.
- [ ] **HTTP `proxy` hand-offs travel in plaintext.** A request handed to a
      `service =` proxy on another node goes as plain HTTP in absolute form
      (`attempt_request` in `src/worker/netd/http.rs` skips TLS and the mesh
      when `hand_off` is set), so an injected credential crosses the network
      unencrypted. Send hand-offs over the mesh when the proxy endpoint is
      remote ([calls #10](calls/egress-credential-injection-plan_calls.md)).
- [ ] Credentials on policy objects (one key for every sandbox on a pool),
      with `UpsertNetPolicy` enforcing `kv:read` as it does for `certFrom`
      (plan §11).
- [ ] **`ForwarderContext` is still one large type behind `Netd`.** The
      worker now reaches the datapath only through `netd::Netd`
      (`NetdInput` / `NetdWants` / `NetdReport`), but inside it one context
      still holds policy, credentials, KV certificates, mesh TLS, the authz
      cache, ledgers, and both host guards. Split it when the process split
      lands ([swappable seams calls #2](calls/swappable-seams-plan_calls.md)).
- [ ] **A wire encoding for `NetdInput`**, the remaining design work of the
      `legion netd` process split (service-mesh calls 17). Secret-bearing
      variants (credentials, KV certificates, the mesh key) must never be
      written to `netd_state.json`, which holds only `DesiredNetwork` and
      policy bodies.
- [ ] **Gated namespaces of allocations that died during a worker restart
      are never removed.** `Netd::restore` only knows the allocations that
      survived, so it cannot tear down the others' namespaces under
      `/var/run/netns`. Sweep `legion-*` namespaces with no desired
      allocation on the first acknowledged heartbeat
      ([swappable seams calls #20](calls/swappable-seams-plan_calls.md)).

Datapath performance: [datapath-performance-plan.md](plans/datapath-performance-plan.md)
· calls: [datapath-performance-plan_calls.md](calls/datapath-performance-plan_calls.md).
Pooling, `splice`, `least_latency`, and the benchmarks shipped (see Done,
Datapath performance). Deferred, by name:

- [ ] **kTLS for mesh hops**, so TLS flows (mesh hops, ingress TLS
      termination) splice too. Needs a dependency or a hand-rolled TLS 1.3
      control-record handler, the kernel `tls` module, and a probe with a
      fallback; about 20% of bulk throughput by Tensorlake's numbers
      (plan §6.6, [calls #3](calls/datapath-performance-plan_calls.md)).
- [ ] **Multiplexed node-to-node tunnels for L4 mesh traffic.** Each L4
      connection to a mesh endpoint still pays its own mTLS handshake;
      ztunnel avoids that with pooled HTTP/2 CONNECT (HBONE) tunnels. A
      wire-format change for the mesh, so it needs its own plan (plan §6.7).
- [ ] **HTTP/2 between nodes in HTTP mode**, so one pooled connection
      carries concurrent requests (Linkerd's transparent upgrade). Pooling
      today reuses HTTP/1.1 connections one request at a time.
- [ ] **Pipe-page limits on very busy nodes.** Each direction that switches
      to splice holds a pipe. Past `/proc/sys/fs/pipe-user-pages-soft`, Linux
      creates small pipes and splice moves less per call. Measure on a node
      with tens of thousands of concurrent bulk flows
      ([calls #6](calls/datapath-performance-plan_calls.md)).

## Persistence & cluster operations

Plans: [persistence-plan.md](plans/persistence-plan.md), [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md), [manager-workload-identity-plan.md](plans/manager-workload-identity-plan.md) · docs: [state-store.md](../docs/state-store.md), [raft-operations.md](../docs/raft-operations.md), [persistence-streaming.md](../docs/persistence-streaming.md)

- [ ] **A newer binary cannot read an older binary's persisted proto JSON.**
      Proto structs derive serde with no field defaults (`build.rs`), so a
      field added to a message makes every file written before it fail to
      parse. Seen on the AWS rig as `missing field envelopeResources`
      reading `netd_state.json` after a worker upgrade. Any on-disk file
      that stores a proto struct as JSON has the same problem. Add a
      container-level `#[serde(default)]` for proto messages, and a test that
      parses a fixture written without a newer field.
- [ ] **A manager with no raft leader holds a read instead of failing it.**
      In the AWS spot check, with the leader's raft core dead, `legion job
      list` hung with no reply. The CLI now gives up at its 30s request
      deadline with a retryable `DeadlineExceeded`
      ([calls #2](calls/join-token-file-and-cli-timeouts_calls.md)), but the
      manager should answer `Unavailable` promptly when it has no leader to
      read through. Where the request blocks is not yet found.
- [ ] **`--restore-from` should pull from the replica URL.** It takes a local
      file, so disaster recovery onto a fresh box needs a separate tool and
      credential to download the replica first. Accept an `s3://` URL and
      restore through `rustyriver::restore` on `build_s3_store`, the same
      credential path the takeover restore already uses.
      [Plan §9 phase 2](plans/manager-workload-identity-plan.md#9-implementation-phases) ·
      [calls #8](calls/manager-workload-identity-plan_calls.md#8---restore-from-stays-a-local-path)
- [ ] **No credential introspection for streaming persistence.** The resolved
      source (`config`, `environment`, `web_identity`, `container_role`,
      `instance_metadata`) is only in the boot log. Add it to the persistence
      status RPC with proto comments, the capability catalog and an MCP
      readback.
      [Plan §9 phase 3](plans/manager-workload-identity-plan.md#9-implementation-phases) ·
      [calls #6](calls/manager-workload-identity-plan_calls.md#6-the-resolved-source-is-logged-at-boot-there-is-no-introspection-rpc-yet)
- [ ] **EKS Pod Identity, `~/.aws/credentials` and SSO for streaming
      persistence.** `object_store` 0.11's AWS chain does not have them, so
      Pod Identity is refused at boot rather than downgraded to the node's
      role. Revisit when rustyriver moves to an `object_store` that supports
      `AWS_CONTAINER_CREDENTIALS_FULL_URI`; the hand-mirrored resolution in
      `src/storage/s3_credentials.rs` has to move with it.
      [Calls #5](calls/manager-workload-identity-plan_calls.md#5-misconfigurations-that-would-change-the-principal-fail-the-boot)

- [ ] **Delete the deferred read-model hooks.** Since
      [read-model coherence](plans/read-model-coherence-plan.md) Phase 4,
      every `cache_*` / `refresh_read_model` call inside a mutation body is
      a no-op (`capture::read_model_deferred`), and the changeset projection
      is the only way into the read model. Remove the ~30 call sites once a
      release has run with the shadow check on
      ([calls entry 3](calls/read-model-coherence-plan_calls.md)).
- [ ] **Shadow check for raft replicas.** The read-model shadow check only
      covers `local_disk` mutations: raft lanes apply concurrently and
      project outside the capture mutex, so there is no quiet point to
      compare at without a per-lane barrier
      ([plan § 9](plans/read-model-coherence-plan.md)).
- [ ] **Task-config cache: scrub it and close the read-through race.**
      `cached_task_configs_for_job_config` caches what it read on a miss, so
      a read that straddles a `destroy_job` can re-insert the destroyed list
      after projection removed it, and the timed scrub does not visit task
      configs ([calls entry 7](calls/read-model-coherence-plan_calls.md)).

- [x] **A sync forward to a new lane leader panicked the manager.**
      `MultiRaftDriver::forward_lane_write` dialed the leader through
      `ChannelPool::get` *before* entering the raft runtime, and
      `Endpoint::connect_lazy` builds a tonic channel by spawning its
      connection task — which panics ("there is no reactor running") on a
      thread with no runtime context. Every synchronous caller reaches this
      path when its write lands on a follower
      (`Store::forward_after_rejection` → `enqueue_eval`), and the channel
      pool hid it: only the *first* forward to a given leader address dials,
      so it fired only just after leadership moved to a manager this node had
      not forwarded to before. The dial now happens inside
      `block_on_engine`. This was previously logged here as
      `raft_lanes::killing_the_all_lane_leader_reconverges_onto_one_survivor`
      being "timing-sensitive under parallel test load" — that diagnosis was
      wrong; the test was reporting a real crash. Reproduced at ~1-in-7 runs
      of that suite before the fix, 20/20 clean after.

- [x] **Real Raft persistence** — quorum-committed writes, per-lane log stores,
      snapshots/compaction, real membership
      ([#137](https://github.com/mikenomitch/legion/pull/137)). Ships
      changeset replication rather than the per-method determinism audit in
      [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md);
      the as-built design and its judgement calls are in
      [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md).
  - [x] Multi-raft lanes (control / scheduler / status) + lane routing —
        single-writer table ownership enforced at capture time, co-located
        lane leadership, per-lane snapshots
  - [x] **Lane-leadership convergence is bounded and cannot break a lane**
        (2026-07-28). The JC-40 loop campaigned for LEADERLESS lanes, which
        bypasses openraft's own election policy and restarts the node's
        candidacy faster than an election can finish — 132 terms in 210s on a
        2-core runner with the lane never electing. It now campaigns only for
        a lane led elsewhere, with per-lane exponential backoff and escalating
        stand-downs, and co-location is documented as BEST EFFORT rather than
        an invariant. Also fixed the root cause the livelock was hiding: a
        joiner counted as a voter while its lane membership was still JOINT,
        so bring-up looked complete mid-join.
        [Plan §JC-40 correction](plans/durability-and-ops-execution-plan.md) ·
        [calls](calls/durability-and-ops-execution-plan_calls.md) ·
        [docs/multi-raft.md](../docs/multi-raft.md)
  - [x] **Standing-split resilience** (2026-07-28, plan §8): a lane split no
        longer breaks the write path. Worker-plane writes (heartbeats,
        health, failure/volume reports) and internal lane halves FORWARD to
        the owning lane's leader over a new `ForwardLaneWrite` transport RPC
        — this also fixes worker liveness across plain `single`-topology
        failover, where the pre-fix cascade marked healthy workers lost in
        ~10s and self-fenced their stateful allocations at ~90s. Job submits
        are ACCEPTED by the control leader; the deferred projections
        (status seeds, volume claims, rollouts — with gating preserved —
        and evals) are completed by per-lane reconcilers on the checker
        cadence. `/readyz` still 503s on a standing split (operator
        signal), with an honest body and a restart-safe, flap-proof
        divergence timer.
        [Plan §8](plans/durability-and-ops-execution-plan.md) ·
        [calls #1, #3, #6, #12](calls/durability-and-ops-execution-plan_calls.md) ·
        [docs/multi-raft.md](../docs/multi-raft.md)
  - [x] **Adversarial verification pass on lanes v2** (2026-07-28): four
        skeptic sweeps over the election governor, the split behavior
        matrix, the snapshot/apply engine, and the plan fence. Confirmed
        fixes shipped: the catching-up hold is capped (uncapped, it starved
        convergence on any busy lane — the standing split was the steady
        state under load); anchor flap no longer resets the campaign loss
        ladder; first-campaign spacing is enforced; `raft status` counts
        joint-intersection voters instead of reporting `voters: []`
        mid-join; remove-member probes reachability, bounds its membership
        changes, and refuses self-removal (two-step decommission
        documented); the bootstrap seed-snapshot purge re-arms across a
        crash; status-lane orphan sweeps need two consecutive observations;
        standalone events propose on a locally-led lane; `rollout_tick` no
        longer stamps rollouts `failed` on transient lane errors; the
        plan-apply status follow-up no longer fails applied plans (the
        `job_update` 5s churn loop). The engine core (changeset capture,
        per-lane snapshots, ownership guard, log durability) survived
        attack — 8 claimed guarantees verified against the openraft 0.9.24
        source.
        [Calls #7–#16](calls/durability-and-ops-execution-plan_calls.md)
  - [x] The lane-isolation benchmark — measured 2026-07-28, three
        consecutive runs: cross-lane load degrades scheduler p99 50–93×
        under `single` (to ~100–130ms) vs 1.6–2.4× under `lanes` (2.4–3.4ms
        — essentially flat), with identical unloaded baselines and 4–5×
        placement throughput retained. `lanes_vs_single_scheduler_isolation_ab`
        in [tests/raft_perf_smoke.rs](../tests/raft_perf_smoke.rs);
        numbers in [docs/multi-raft.md](../docs/multi-raft.md) § "Lane
        isolation A/B".
  - [x] Contention instrumentation for the manager write path (2026-08-02):
        the families that were missing when the ~80 hb/s ceiling was
        investigated — group-commit effectiveness
        (`legion_store_coalesce_batch_size` / `_wait_seconds{role}` /
        `_commit_seconds` / `_wait_rounds_total`, labeled `heartbeat` and
        `plan_apply`), the per-lane pipeline queue in front of raft
        (`legion_raft_pipeline_wait_seconds`, `legion_raft_pipeline_depth`),
        and runtime saturation (`legion_grpc_in_flight`,
        `legion_runtime_spawn_delay_seconds`,
        `legion_runtime_worker_threads`). Read order in
        [docs/metrics.md](../docs/metrics.md) § "Diagnosing a manager
        throughput ceiling"; calls in
        [calls/manager-contention-instrumentation_calls.md](calls/manager-contention-instrumentation_calls.md).
        Instrumentation only — the blocking heartbeat path is unchanged.
  - [x] Raft engine metrics + per-lane bottleneck tracing (2026-07-30):
        the full `legion_raft_*` series — per-lane term / commit_index /
        applied_index / apply_lag / is_leader gauges on the checker tick,
        plus `capture_seconds` vs `propose_apply_seconds` histograms and
        `ops_total{lane,outcome}` at the replication call site, with a
        per-op debug trace of the capture/propose phase split
        ([docs/metrics.md](../docs/metrics.md)). Snapshot timings and
        per-peer replication lag stay reserved.
  - [ ] Per-lane tuning knobs and a Raft health UI page (the remaining
        two-thirds of the old "knobs, tracing, UI" item)
  - [ ] Speculative: Raft autopilot
  - [ ] Idempotency records are not atomic with their mutation: an RPC that
        errors after its mutation but before `idempotency_record` (control
        lane) re-executes on retry — for server-minted ids
        (`create_instance`) that duplicates the instance. Fold the record
        into the mutating op's own capture (it is JC-28 commutative, so any
        lane may carry it). Found by the 2026-07-28 split inventory.
  - [x] Eval NotRaftLeader retries back off exponentially (5s base → 60s
        cap, 1s jitter, in-process attempt tracking that resets on
        completion or failover) instead of a flat 5s forever (2026-07-30).
  - [ ] `destroy_job` under a standing split half-applies from the status/
        scheduler side (cleanups land, control delete rejected) until a
        retry against the control leader completes it. Consider reordering
        control-first + orphan sweeps, like the submit composite.
- [x] Loading state back in from a file — `legion state backup` +
      `--restore-from` ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Cluster federation** —
      [plan](plans/multi-cluster-federation-plan.md) (PLANNED, with a
      [calls doc](calls/multi-cluster-federation-plan_calls.md)): named
      clusters in the CLI/UI, symmetric peering (no hub), request
      forwarding, federated SSO, multi-cluster jobs, standby managers
      (learners + zone auto-replacement), and an async DR standby cluster
      with manual promotion and generation fencing. Cross-cluster service
      discovery deferred to a follow-up plan (see plan §11).
- [ ] **Scheduler sharding (more raft groups for more throughput)**
      ([#25](https://github.com/mikenomitch/legion/pull/25)) —
      [proposal](plans/scheduler-sharding-strategies-proposal.md) (with a
      [calls doc](calls/scheduler-sharding-strategies-proposal_calls.md)):
      the option map for scaling eval/alloc throughput by adding raft
      groups. Headline recommendation (revision 3, 2026-10-12): pool-keyed *node-owned*
      scheduler shards where a shard is a raft group **plus its own state
      file** (`scheduler-<k>.sqlite`) — extra groups over the shared
      `store.sqlite` only overlap the quorum round-trip and the arithmetic
      says ~×1.1 post-batching, because capture and apply serialize on the
      process-global `CAPTURE_MUTEX`. Rung A co-locates every shard leader
      on the anchor (N writers + N pipelines on one box, composites/backup
      untouched); rung B spreads leadership; rung C is online resharding.
      Job-keyed (Omega-style) shards rejected; more write-class lanes
      refused as a scaling axis. Revision 3 adds that the October AWS
      campaigns show two regimes: small fleets look commit-bound (rung A's
      case), while at the largest fleets the leader is planning-bound with
      an idle commit path and idle followers, where M3 (plan shipping)
      is the cheaper lever, so M3 is now a peer of rung A, not a fallback.
      Gated on Phase 0: read the capture-vs-propose ratio and the
      eval-phase split from the October S3 archives (`collect.sh` now
      scrapes both raft series) and micro-bench N pipelines over one file
      vs N files. Names the cross-file consequences to audit (cross-lane
      `ON DELETE CASCADE`s including `allocation_health` →
      `allocation`, shard-resident `scheduler_event`, per-shard
      adaptive-concurrency limits, per-file schema checks, attach cap of
      8, `ID_STRIDE` widening).
- [x] Version handshake between manager and worker — protocol version plus
      build-skew reporting, upgrade order in [docs/upgrades.md](../docs/upgrades.md)
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Node configuration jobs** — like sysbatch, but also runs on join and can
      add dynamic node metadata (CNI setup, volumes, artifacts, Ansible-shaped
      work). Specified as phase 3 of the
      [host convergence plan](plans/host-convergence-plan.md) (`nodeConfig`
      with `onJoin`, `gateReadiness`, `publishAttrs`), which subsumes the
      sysbatch/system-job items below.
- [ ] Reconfigure a worker from the manager with CRUD commands
  - [ ] Existing nodes of a changed type need the update pushed to them, which
        implies nodes store a reference to their type
- [ ] Root-privilege split: run networking and volume setup as root while the
      task itself doesn't
- [ ] **Stream raft snapshots as files (audit R5).** Builds no longer block
      writes, but a snapshot is still held fully in memory on build, serve and
      install. Switch openraft's `SnapshotData` from `Cursor<Vec<u8>>` to
      `tokio::fs::File`. [calls #26](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **`Store::state_backup` still runs `VACUUM INTO` under the capture
      lock.** The same read-connection approach R5 used for snapshots would
      stop backups from stalling writes.
- [ ] **An older-schema manager halted on an entry can be sent a newer
      whole-database snapshot (audit R2).** It then refuses to start until it
      is upgraded. Loud, not graceful.
- [ ] **Streaming mode: the WAL checkpointer fights rustyriver.**
      `Store::spawn_wal_checkpointer` runs PASSIVE checkpoints in streaming
      mode, where rustyriver expects to own checkpointing. Pre-existing, found
      during audit R4.
- [ ] **Streaming fencing is time-based (audit R4).** A primary paused past
      its lease TTL can accept a write and ship one more cycle before it
      notices. Documented in
      [persistence-streaming.md](../docs/persistence-streaming.md).
- [ ] **No raft leadership transfer on manager shutdown (audit M3).** openraft
      0.9 has no API for it, so stopping a leader costs an election timeout
      per lane. Revisit on an openraft upgrade. [calls #16](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **The exec-ws and telemetry listeners only log a bind failure.** The
      gRPC listener now ends the process (audit M2). These two don't.
- [ ] **Object storage: native GCS and Azure.** Every store opens through
      `src/storage/objects/`; a backend is an `object_store` Cargo feature, a
      `Scheme` arm, a `Backend` variant, and an `ObjectCredential` variant.
      None was added (S3-compatible endpoints, GCS interop included, work).
- [ ] **A worker resolves its object-store principal on first use, not at
      boot.** Streaming persistence resolves and logs its principal when the
      manager starts; a worker's `contentStore` finds a broken environment
      only at its first publish or restore.
- [ ] **Decide whether a `contentStore` should refuse an ambiguous AWS
      environment** (one web-identity variable without the other, EKS Pod
      Identity) as streaming persistence does. Today it continues down the
      chain to the instance role with a warning, which is what it always did
      ([swappable seams calls #3](calls/swappable-seams-plan_calls.md)).
- [ ] Measure `file://` publish throughput through `LocalFileSystem`'s
      multipart upload against the old direct write
      (`content_publish::tests::publish_throughput`;
      [swappable seams calls #17](calls/swappable-seams-plan_calls.md)).

## Job types

- [ ] **Per-replica job outcomes.** The derived job status takes each task's
      outcome from its `task_status` row, the last exit any replica reported,
      so a `count = 3` batch task with one failed replica can read `complete`
      ([plan §4](plans/job-status-plan.md#4-goals-and-non-goals),
      [calls 4](calls/job-status-plan_calls.md#4-a-tasks-outcome-is-its-task_status-row-not-a-per-replica-count))
- [ ] **Job status-change events.** The job status is derived on read, so
      nothing emits `job.status_changed` when a job goes `running` →
      `complete`; an agent has to poll `GetJobInfo`
      ([plan](plans/job-status-plan.md))
- [ ] **Sysbatch** — run once on every node
- [ ] **System job** — N per X (1 per metal, 2 per region, …). Is this really a
      job type, or just a richer `count`? (`count: {num: 1, per: "id"}`
      approximates it today but does not converge newly joined nodes.)

## KV & secrets

Plan: [kv-encryption-plan.md](plans/kv-encryption-plan.md) · docs: [kv-encryption.md](../docs/kv-encryption.md)

- [ ] **Encrypt the rendered copies of KV values, or stop making them.**
      Encryption at rest (shipped, see Done) covers `kv_entry.value` only: the
      *rendered* results of `$key` references and `{{ kv "…" }}` templates in
      `job_config` / `task_config`, the worker's state file, and task
      directories stay plaintext (plan §6.9, calls doc #10). Needs its own
      plan: encrypt the derived columns under the same keyring, or move
      rendering to the worker (Nomad's `template` + Variables model) so the
      manager never materializes secrets.
- [ ] **Native KMS root-key providers** (AWS KMS, GCP KMS, Azure Key Vault,
      Vault Transit linked into the binary). Deferred on purpose — the `exec`
      provider plus the reference scripts under `plugins/keyring/` cover every
      KMS without a cloud SDK in the binary (calls doc #7). Revisit only if
      the shell-out contract proves too fragile in practice.
- [ ] **A `passphrase` / manual-unlock root-key provider** (Docker autolock,
      Vault Shamir): the manager starts with KV locked until `legion keyring
      unlock`. The provider trait already fits it; nobody has asked yet.
- [ ] **`legion kv list` fails as a whole when one listed value is under a
      locked data key.** Simple and loud, but a partial listing with a
      per-row "locked" marker might serve an incident better; needs
      `KvEntry` to carry an unavailability reason.
- [ ] **Missed KV deletes under `restart`/`rollout` policies are undetectable
      (audit M4).** The leader sweep can't tell a deleted key from its last
      rendered value. Closing it needs a replicated marker (a schema change).
      There's also a residual window between `handle_job_submit` and
      `request_job_restart` inside `reconcile_kv_changes`. [calls #15](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **`onChange = "none"` probably turns into a rollout.** A watched put
      with `none` stores a new job config but leaves the allocations on the
      old one. That is exactly the signature `reconcile_missing_rollouts`
      repairs, so its next pass likely rolls the job out anyway. Found while
      building `onChange = "update"`, which moves allocations synchronously to
      avoid it ([calls #3](calls/kv-cert-delivery-plan_calls.md)). Confirm
      with a test, then decide: migrate in place (and keep files unmarked), or
      document `none` as "on the next replacement".
- [ ] **Relative KV file paths on `exec` land in the worker's directory.**
      Start and live rewrites resolve `path` as given, so `tls/bundle.pem` is
      written relative to the worker's cwd, not the task directory. Identity
      files share the rule. Fix both together
      ([calls #10](calls/kv-cert-delivery-plan_calls.md)).

## Auth, TLS & security

Plan: [auth-acl-plan.md](plans/auth-acl-plan.md) · docs: [auth-and-acls.md](../docs/auth-and-acls.md)

- [ ] **Namespaces** — [plan](plans/namespaces-plan.md) (PLANNED) ·
      [calls](calls/namespaces-plan_calls.md). Today namespaces are vocabulary
      without values: the `namespaces` ACL selector globs correctly but every
      scope is stamped `"default"` (`src/structs/acl.rs:213`), so a policy
      naming any other namespace silently grants nothing.
  - [ ] Phase 0 — `Namespace` object + CRUD (gRPC/CLI/MCP), the `namespace`
        ACL resource, and validation that a `namespaces` selector names a
        namespace that exists (kills the silent-no-op policy footgun).
  - [ ] Phase 1 — jobs carry a namespace; `-n`/`LEGION_NAMESPACE`/`-A`,
        `namespace/name` references, namespace on every job-derived ACL scope,
        list filtering, `$LEGION_NAMESPACE` in task env. Names stay globally
        unique; resolution becomes namespace-scoped (the plan's one behavioral
        change, landed while everything is still in `default`).
  - [ ] Phase 2 — per-namespace name uniqueness, **and** `{namespace}` in the
        default workload-identity subject template in the same phase (without
        it, `prod/web` and `dev/web` mint identical JWT subjects — see
        [call 4](calls/namespaces-plan_calls.md)).
  - [ ] Phase 3 — namespaced service catalog + same-namespace `connect`
        resolution, and per-namespace event filtering (`list_events` /
        `stream_events` are `operator:read` all-or-nothing today,
        `src/manager/server.rs:3133`).
  - [ ] Phase 4 — namespaced volumes and templates; UI (namespace in the URL,
        header switcher, All-namespaces mode, Namespaces page, switcher
        suppressed for single-namespace identities).
  - [ ] Phase 5 — governance on the namespace object: quota,
        `allowedPools`, `allowedRuntimes`, default band. Enforcement point for
        quota is open (plan § 14).
- [ ] **KV ACLs on `GetArtifactCredential`.** Artifact auth (`auth.kv`) lets any
      job read any KV credential path today. The enforcement point exists and is
      marked — see the SECURITY BOUNDARY comment on `get_artifact_credential` in
      [src/manager/server.rs](../src/manager/server.rs), the RPC comment in
      `proto/legion.proto`, and the call-out in
      [docs/artifacts.md](../docs/artifacts.md). Accepted for now; close it.
- [ ] **Worker mTLS (deferred from auth phase 3).** Now planned as part of
      the worker-plane fix at the top of this file:
      [worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md).
- [ ] Dynamic auth-method dylib plugins (deferred from auth phase 2)
- [ ] Workload-identity API credentials (deferred from auth phase 4)
- [ ] Token-resolution cache (deferred from auth phase 4)
- [ ] **TLS is autoconfigured and auto-rotated.** Joining a cluster with TLS
      should be trivial.
  - [ ] **Plan this, don't just code it.** Should the "insecure client" really be
        a "public cert client" (Let's Encrypt by default, bring-your-own CA
        supported), leaving the "secure client" as the self-signed-CA path only
        workers hold? Or is TLS the wrong mechanism here entirely?
  - [ ] Fix the join-token and pre-register call destinations
  - [ ] Env var or flag for contacting the manager — the join-server port
        shouldn't need to be supplied, since it's in the JWT
- [ ] **Cert manager built in**, with public-CA routes, usable by workloads and
      not just the control plane
- [ ] **A join secret can still claim an existing node's name (audit S1).**
      Node tokens are now bound to their node, but a compromised worker still
      holds its join secret. Fix shape: per-node join tokens, or workers
      persisting their node token. Mitigation today: short join-token TTLs,
      revoked after the fleet joins. [calls #3](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **`GetArtifactCredential` is bound to the node but not to its
      allocations.** Any node can read any credential-shaped KV path. Its proto
      comment ("KV ACLs do not exist yet") is stale. Found during audit S1.
- [ ] **Idempotency keys don't cover the request body (audit S4).** The same
      key with different arguments replays the first reply. Documented in
      [agent-readiness.md](../docs/agent-readiness.md).
- [ ] **Exec tasks inherit the worker's whole environment.**
      `src/task_plugins/exec.rs` adds the allocation env to the worker's and
      never clears it, so anything in the worker's environment (a
      `LEGION_TOKEN`, cloud credentials, `RUST_LOG`) reaches every host-mode
      exec task. It is why the join token went to `--token-file` and not an
      env var. Decide on an allowlist (Nomad's exec driver passes a filtered
      set). [calls #1](calls/join-token-file-and-cli-timeouts_calls.md)
- [ ] **A restarted worker still needs its join token string.** It
      re-registers with its persisted node token, but reads the manager
      address, worker type, and CA pin from the join token at every start.
      Persist those beside `node_token` so `legion start worker` runs with no
      token after the first registration and the token file can be deleted.
      [calls #6](calls/join-token-file-and-cli-timeouts_calls.md)
- [ ] **`prune_expired_boot_tickets` is never called outside tests.**
- [ ] **The ACL credential pruner runs only with ACLs enabled and isn't gated
      to the raft leader.**
- [ ] **Switch a running cluster's CA provider without re-joining workers**
      (cross-sign during an overlap, as the builtin rotation does).
      [cluster-CA calls #13](calls/cluster-ca-provider-plan_calls.md)
- [ ] **CA agreement is checked against the control-lane leader's record
      only.** Two followers that each agree with the leader but not with each
      other go unnoticed; record and compare every manager's view
      ([cluster-CA plan §11](plans/cluster-ca-provider-plan.md)).
- [ ] Exercise `plugins/cluster_ca/vault-pki/legion-ca-vault` against a live
      Vault in CI (today it runs against a fake `vault` CLI).
- [ ] An upstream-authority mode (a locally held intermediate from Vault or
      AWS Private CA), or native Vault/AWS PCA providers, if `exec` signing
      latency shows at scale ([cluster-CA plan §11](plans/cluster-ca-provider-plan.md)).
- [ ] `legion keyring provider test` has no capability-catalog entry
      ([cluster-CA calls #14](calls/cluster-ca-provider-plan_calls.md)).

## Observability

docs: [observability.md](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [ ] A trace viewer in the UI (make it dev-only at first)
- [ ] **Count the console lines a server drops.** Server console logs go
      through a lossy non-blocking writer (Done, below), and a line dropped
      under backpressure leaves no trace. Expose `tracing-appender`'s
      dropped-line count as a metric, or log a summary line when lines were
      dropped ([calls #3](calls/sandbox-boot-throughput-plan_calls.md)).
- [x] Metrics endpoint and health probes — Prometheus `/metrics`, `/healthz`,
      `/readyz` ([docs/metrics.md](../docs/metrics.md), [#137](https://github.com/mikenomitch/legion/pull/137)).
- [x] Worker-side `/metrics` + OTLP metrics push (2026-07-30): opt-in
      worker Prometheus listener (`LEGION_WORKER_TELEMETRY_PORT`;
      `legion_worker_*` heartbeat/allocation families) and an OTLP/HTTP
      metrics push for both registries (`OTEL_*` env conventions,
      cumulative temporality, full histogram buckets) —
      [docs/metrics.md](../docs/metrics.md),
      [docs/observability.md](../docs/observability.md). Per-task runtime
      metrics on workers still open (below).
- [ ] Worker per-task runtime metrics (CPU/memory usage per allocation) —
      also wanted by autoscaler phase 2, which collects usage via
      heartbeats; build together.
- [x] Refresh [docs/observability.md](../docs/observability.md) — corrected; it
      had claimed OTLP export did not exist while [src/otel.rs](../src/otel.rs)
      ships it ([#137](https://github.com/mikenomitch/legion/pull/137))

## Templates, registry & packaging

The Pack/Helm equivalent. Job templates are in flight on
[#151](https://github.com/mikenomitch/legion/pull/151); the registry half is untouched.

- [ ] Write the plan + calls doc for job templates (#151 shipped without one)
- [ ] There is a default registry
- [ ] I can add a registry
- [ ] I can run with `@<registry>/<job>`
- [ ] I can inspect a job in the registry (reads its metadata)
- [ ] UI for filling in variables from a job definition's metadata
- [ ] Plugins can be fetched by URL or from the registry
- [ ] Automatic build & push step for container images — `legion build` /
      `legion prep`, plus a context file
- [ ] Buildpack runner
- [ ] Publish `legion-rs` to crates.io. The crate is renamed and packages
      cleanly (`cargo package`), but nothing is published yet, so the
      README's `cargo install legion-rs` fails until the first publish.
- [ ] Bump the yanked `validit` 0.2.5 and `wide` 1.6.0 pins in `Cargo.lock`.
      `cargo package` warns about both.
- [ ] Prebuilt release binaries and a Homebrew formula, so installing does
      not need Rust or `protoc`

## Ecosystem apps (the omakase experience)

Each of these should run inside the core binary *and* be splittable into its own
job, so day 1 is simple and day N is performant. Each needs its own plan.

- [ ] **Autoscaler** — [plan](plans/autoscaler-plan.md) (IN EXECUTION) ·
      [calls](calls/autoscaler-plan_calls.md)
  - [x] Phase 1 — `legion job scale` (2026-07-30,
        [docs/autoscaling.md](../docs/autoscaling.md)): `task_scale` count
        overrides outside the job version (control-lane; see calls #5),
        effective counts in every desired-count reader (scheduler plans,
        slot retirement, rollout group plans), the `job_scale` eval
        (retire surplus + reschedule shortfall), the ECS rollout rule with
        `pending_count` promotion on terminal rollouts (calls #7),
        `base_count` resubmit arbitration, the `scale` ACL resource,
        ScaleTask/GetJobScaleStatus RPCs, CLI (`legion job scale`), MCP
        tools, events, and the `scale_in` stop cause reported to the
        worker whose allocation was retired (added merging with Phase 1a
        of the preemption plan — calls #16). Deferred within phase 1: counts in
        `legion job info` (scale status is the view), an ACL-scenario
        test proving `scale:write` cannot RunJob (the endpoint matrix
        covers the deny side), and regenerating `ui/src/gen/legion_pb.ts`
        (no UI surface consumes the new RPCs yet; run the ui/README
        protoc command when the scaling panel is built).
  - [ ] Phase 2 — per-allocation CPU/memory usage collection (heartbeat →
        `allocation_health`), request-vs-usage in CLI/UI
  - [ ] Phase 3 — task autoscaler MVP: `scaling { min, max, target }` block,
        in-manager leader-gated evaluator, damping, scale-status surface
  - [ ] Phase 4 — full `checks` form, Prometheus source, `per-replica`
        strategy, activation-gated `min: 0`, `headroom`
  - [ ] Phase 5 — escape-to-job: `legion autoscaler agent`, control-plane
        identity policy, KV-lock leadership, `LEGION_MANAGER_ADDR` injection
  - [ ] Phase 6 — node autoscaling: placement-shortfall signal, node groups,
        exec-ABI providers, drain-based scale-in, `ForgetNode` RPC
  - [ ] Phase 7 — dylib `metrics`/`strategies` plugin kinds, `ui.link` Apps
        sidebar section
- [ ] Ingress proxy / load balancer (also listed under *Networking*)
- [ ] GitOps helper — deploy to git, job updates automatically
- [ ] Cert manager (also listed under *Auth, TLS & security*)
- [ ] Overlay network controller

## Plugin surface

docs: [plugin-system.md](../docs/plugin-system.md)

Shipped plugin categories: schedulers, task runtimes, rollout strategies,
artifacts, volumes, node checks. Still to build:

- [ ] Resource plugins
- [ ] Command & API-endpoint plugins
- [ ] Fingerprinter plugins (including networking fingerprinters)
- [ ] CNI / networking plugins
- [ ] Allocation-setup plugins (pre-fetch an image, snapshot, or data set)
- [ ] Task-group-setup plugins
- [ ] Logging plugins (log sinks exist, but not as loadable plugins)
- [ ] Eval hooks — react to cluster events from inside Legion, with the option to
      break out into a job (this is roughly how the autoscaler would work)
- [ ] Pluggable node statuses for degradation (disk pressure, network pressure, …)
- [ ] Pluggable configuration language?
- [ ] **A stable plugin ABI (audit M6).** Loads are now refused on a build
      fingerprint mismatch, but plugins still pass Rust trait objects across a
      dylib boundary, and rollout plugins run inside the manager. Options:
      `abi_stable`, a C ABI, or out-of-process plugins. [calls #8](calls/architecture-audit-2026-10-09_calls.md).
- [ ] **Plugin lockfiles are stale.** `plugins/tasks/noop/Cargo.lock` still
      names `legion` instead of `legion-rs` and lacks `ring`. The other plugin
      lockfiles probably do too.

## Agent surfaces

docs: [agent-readiness.md](../docs/agent-readiness.md)

The MCP server, self-documenting CLI, structured output, and documented gRPC
surface all shipped. What's left is the harder "can an agent actually operate
this" work:

- [ ] **`deduplicated` lies to the loser of a CONCURRENT idempotency-key
      race, on every mutating RPC except `RunJob`.** Two requests carrying
      the same key both miss `idempotency_lookup`, and `idempotency_record`
      (`INSERT OR IGNORE`) already returns which one won — but `stop_job`,
      `destroy_job`, `scale_task`, the `instance` verbs and the rest
      discard that boolean and hardcode `deduplicated: false`, so both
      callers are told they were first. `run_job` was fixed alongside the
      job-intake group commit (which widens the overlap window); the rest
      each need their own decision about what the loser should be handed
      back. Sequential retries — what the key is really for — are correct
      everywhere. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §17.
- [ ] An agent can easily write a plugin (scaffolding, schema, a worked example)
- [ ] An agent can easily configure a worker to talk to a manager
- [ ] An agent can search the registry and find jobs worth installing and running

## Embedding

- [ ] Legion can be called as a library, not just as a CLI

## UI

docs: [ui-architecture.md](../docs/ui-architecture.md)

Deferred from [ui-jump-hints-plan.md](plans/ui-jump-hints-plan.md):

- [ ] Jump hints past the tenth row — a 20- or 50-row page labels only its first
      ten, and the rest look unfinished until you know it is deliberate
- [ ] Jump hints on the Overview dashboard and on detail-page tables; today only
      the six index pages register rows
- [ ] Verify the jump-hint chords survive Safari and Firefox. The letters avoid
      Chromium's reserved set, but "the page wins `⌘`+digit" is asserted from
      the reserved-key list, not measured
      ([calls entry 1](calls/ui-jump-hints-plan_calls.md))

- [ ] **Suspend, resume, and oversubscription in the UI**
      ([plan](plans/suspend-resume-ui-plan.md),
      [calls](calls/suspend-resume-ui-plan_calls.md),
      [docs](../docs/ui-architecture.md#usage-pressure-and-snapshots)).
  - [x] UI-1: worker Usage card from `GetNodeUsage` (capacity with ratio,
        reserved and used bars, pressure status, per-allocation table,
        pressure order with step chips), workers-index memory column, usage
        and `memoryMax` on the job's instance and allocation tables and the
        task page, Memory pressure cards on job and worker pages, structured
        `node.pressure` and `snapshot.*` summaries on Events, fixtures at
        `/testing`
  - [x] UI-2: job Snapshots card with take (disk or memory), restore, and
        delete, ACL-gated and two-step, row expansion with the manifest, a
        "restoring from" badge on instances
  - [x] UI-3: `suspended` badge and state filter, checkpoint column (kind,
        local or published, size, node, age), Suspend (kind menu when both
        are possible), Resume, and Resume cold, two-step and ACL-gated, last
        suspend and restore durations, `last_stop` on the row, and an
        `instance` events category with summaries. Rows are keyed by id so
        an armed confirm survives polls
        ([calls 6](calls/suspend-resume-ui-plan_calls.md#6-instance-rows-are-keyed-by-id-and-cells-read-the-row-through-an-accessor))
  - [x] UI-4: idle countdown from `idle_since` and `idle_after_seconds`,
        `suspend_reason` on suspended rows, and the `suspend` step chip
  - [x] UI-5: Fork with count and spread (spread only when the manager
        allows it), fork lineage, snapshot names shown, set, and removed
  - [x] UI-6: durability column, publish on create, Warm, snapshot holders,
        and the worker page's "Content and locality" card
  - [x] Server changes the UI needs
        ([plan §7.3](plans/suspend-resume-ui-plan.md#73-server-changes-the-ui-needs)):
        `ListNodeUsage` (the workers index makes one call), `InstanceInfo.last_stop`
        plus `instance_id` in `node.pressure`, `GetNodeContent` and
        `SnapshotInfo.holders`, `InstanceInfo.checkpoint_support` per
        operation, `forked_from`, and CPU `cores` and `used_millicores` on
        node usage (`legion node usage` with no node, `legion node content`,
        and the `legion_node_usage_list` and `legion_node_content` MCP tools)
  - [ ] Show the `artifacts` budget on the worker page's "Content and
        locality" card. The `snapshots` bar ships, with the room left for a
        new checkpoint and whether the budget or the disk limits it, but
        GetNodeUsage carries only the `snapshots` class
        ([calls 8](calls/suspend-resume-ui-plan_calls.md#8-getnodecontent-reads-only-what-the-manager-already-holds))
  - [ ] `create --from-snapshot` from the UI needs an instance-create
        surface, which the UI does not have
        ([calls 15](calls/suspend-resume-ui-plan_calls.md#15-creating-instances-from-a-snapshot-stays-in-the-cli))
  - [ ] Linux visual check: `/nodes/<id>` against `legion node usage <id>`
        on a worker with enforcement on, and an idle countdown reaching zero
        within one poll of the idle sweep suspending the instance. macOS
        only exercises the not-reported shape, so the countdown is checked
        against fixtures at `/testing` only

- [ ] **Sessions in the UI, at any count**
      ([plan](plans/session-ui-plan.md),
      [calls](calls/session-ui-plan_calls.md),
      [docs](../docs/ui-architecture.md#sessions)).
  - [x] Sessions labelled on the job's Instances card (mode badge, envelope
        and node, promoted-from), Promote and End per row and on the
        instance page, a session summary (count, nodes, `complete`,
        `asOf`, busiest nodes), URL filters (`mode`, `node`, `envelope`,
        `task`), envelope cards on the allocation page, envelope badges on
        the job and worker pages, notes on session and envelope task pages,
        fixtures at `/testing`
  - [x] Bounded at scale: sessions read 500 at a time with First/Prev/Next,
        100 rows rendered at a time, no re-poll past 5,000 instances, job
        allocations 200 at a time
  - [x] Server paging and a session limit on `ListInstances`
        (`session_limit`, `session_page_token`; #36,
        [calls 1](calls/session-ui-plan_calls.md#1-the-job-page-reads-one-500-session-page-and-pages-with-prevnext-counts-come-from-the-reply))
  - [x] Session counts without the listing: `session_count` and
        `sessions_by_node` on every `ListInstances` reply
        (`session_limit = 0` for counts only), and per-node counts from
        `ListSessionNodes` (#36,
        [calls 2](calls/session-ui-plan_calls.md#2-per-task-and-per-envelope-counts-come-from-listsessionnodes-not-the-listing))
  - [ ] `ListInstances` filters for `node_id`, `envelope_alloc_id`, `mode`,
        and `task`, so the node page and the envelope card can show their
        own sessions without the job's listing
        ([calls 6](calls/session-ui-plan_calls.md#6-node-envelope-task-and-mode-filters-live-in-the-url))
  - [ ] The runtime (or an envelope flag) on `LedgerAllocation`, so the
        worker page stops recognizing envelopes by name
        ([calls 7](calls/session-ui-plan_calls.md#7-an-envelope-is-recognized-by-name-confirmed-by-the-spec-when-there-is-one))
  - [ ] Browser check against a live session cluster (Promote, End,
        paging, the envelope card, the edge-nodes card); checked by
        typecheck, build, `/testing`, and a 250,000-session paging run of
        the fixtures only

Deferred from [ui-holistic-polish-plan.md](plans/ui-holistic-polish-plan.md):

- [ ] Restart worker from the worker page. `RestartWorker` exists and the CLI
      has `legion node restart`; it restarts the worker *process*, which wants
      its own confirmation copy before it sits next to Cordon / Drain
- [ ] A polled page whose one RPC keeps failing while the manager is otherwise
      up shows its last good data with no local hint — `retainOnError` keeps
      the last value and only the connection banner reports outages. A
      per-card "stale since" marker would close the gap
      ([calls entry 3](calls/ui-holistic-polish-plan_calls.md))
- [ ] ⌘K recents are per browser (localStorage); per-identity recents would
      need a manager-side store
      ([calls entry 7](calls/ui-holistic-polish-plan_calls.md))
- [ ] The CI generated-client check trusts the runner's apt `protoc`; pin it
      (buf, or a release download) if a runner image bump ever produces a
      false drift ([calls entry 9](calls/ui-holistic-polish-plan_calls.md))
- [ ] A toast primitive, should a feature need feedback away from the control
      that caused it; every message so far is an inline `Note`

From [ui-task-and-template-redesign-plan.md](plans/ui-task-and-template-redesign-plan.md)
(shipped; see Done):

- [ ] A server-side action fan-out (an allocation list or selector on the
      action RPC, one audit record, bounded concurrency). The Actions tab runs
      on several instances as one browser session per allocation today
      ([calls entry 3](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] Per-check, per-instance health results. `allocation_health` keeps one
      healthy flag and status string per allocation, so the task Overview
      can only show declared checks plus each instance's overall verdict
- [ ] A restart counter per allocation. The task Overview counts
      `allocation.failed` events from the job's last 200 instead, a floor on
      a busy job ([calls entry 6](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] Queryable action/exec run history ("last run 3h ago by dana", re-run).
      `exec_audit` is a tracing target today, not state
- [ ] `PlanJobTemplate` — the template analogue of `legion job plan` — so the
      guided launch's Review can say how many allocations a change replaces,
      not just how the spec text changes
      ([calls entry 1](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] A structured outcome on `RunJobTemplateReply` (created / updated with
      rollout / unchanged); the Launch step reads it from the message text
      today ([calls entry 8](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] Template-only tokens are denied `GetJobInfo` for a job that does not
      exist yet (the request carries no template attribute), so their Review
      cannot tell "creates" from "updates" and cannot follow the rollout
      ([calls entry 10](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] `Store::get_submissions_for_config` has no `ORDER BY`; the UI now finds
      the spec by name, but any other "first JSON submission" consumer has the
      same latent bug ([calls entry 5](calls/ui-task-and-template-redesign-plan_calls.md))
- [ ] Template catalog and authoring views: usage counts, jobs behind the
      latest version, version-to-version source diff

Deferred from [operator-ui-gaps-plan.md](plans/operator-ui-gaps-plan.md):

- [ ] `LEGION_WORKER_PUBLIC_ADDRESS=auto`: detect the public address from
      cloud metadata (EC2 IMDSv2, GCP, Azure) instead of the operator wiring a
      `curl` into the start script
      ([calls 5](calls/operator-ui-gaps-plan_calls.md#5-the-public-address-is-a-per-node-env-var-not-worker-type-config))
- [ ] Manager reverse proxy so a browser can open a service on nodes with no
      reachable ports ([plan](plans/manager-reverse-proxy-plan.md))
- [ ] `GetAllocation` RPC (job-scoped) so the allocation page works for tokens
      without `node:read`; today it reads the whole ledger
      ([calls 4](calls/operator-ui-gaps-plan_calls.md#4-the-allocation-page-reads-the-ledger-instead-of-a-new-getallocation-rpc))
- [ ] `legion kv get --json` omits `GetKvReply.references`; only the text
      output prints `used by:`
- [ ] The instance event filter scans the bounded journal with `json_extract`;
      an indexed `scheduler_event.instance_id` column if the journal bound ever
      grows ([calls 3](calls/operator-ui-gaps-plan_calls.md#3-the-instance-event-filter-reads-metadata-not-a-new-column))

## Cross-cutting

- [ ] **AWS spot-check re-run (2026-10-12): open papercuts.** Found on the
      rig, not fixed on `spot-check/fixes-2026-10-12`
      ([findings](launch-spot-check-findings.md#re-run-2026-10-12)).
  - [ ] **Keep the mesh datapath alive across a worker restart.** Re-adopted
        tasks keep running, but the datapath lives in the worker, so their
        upstream connections drop and an app without reconnect logic fails
        until rescheduled. Documented now
        ([upgrades.md](../docs/upgrades.md#restarting-a-worker-in-place)).
        Running the listeners in a process that outlives the worker would
        remove the limitation.
  - [ ] **Stopping the macOS manager leaves its Apple containers running.**
        SIGTERM to `legion start manager --run-worker` exits without tearing
        down the containers it started.
  - [ ] **The CLI writes its tracing to stdout and logs into the working
        directory.** One-shot commands print `info`/`warn` events in the
        pretty ANSI format on stdout, where they mix with `--json` output,
        and every command creates `./logs` and `./traces` wherever it runs
        (`src/tracing_util.rs`, `init_tracing_with_options`). Found while
        fixing the double-printed command errors.
  - [ ] **`state query` runs only the first of several statements.** The
        read path prepares the SQL once, so `SELECT 1; SELECT 2` silently
        returns the first result and ignores the rest. Refuse a trailing
        statement instead (`src/storage/store.rs`, `query_state_store`).
  - [ ] **`job plan` errors where `job run` would preempt.** With the
        workers full of low-band microVMs, `job plan` for a high-band claim
        failed "no available nodes", while the run preempted and placed.
  - [ ] **Instance JSON shows `nodeId: null`** for an active instance with a
        current allocation.
        until rescheduled. Document it in `docs/upgrades.md`, or keep the
        datapath alive across a worker restart.
  - [ ] **One `~/.legion/token` for every cluster.** A second local manager
        logs "token already exists and was left alone" and the CLI then
        presents the other cluster's token ("unknown or revoked ACL
        token"). Key stored tokens by manager address.
  - [ ] **The CLI panics on a closed pipe.** `legion raft status | head`
        prints "failed printing to stdout: Broken pipe" and a panic.
  - [ ] **`job plan` of an unsubmitted job uses positional task ids.**
        `Scheduler::tasks_for_plan` numbers its tasks 1, 2, ..., and
        `place_task_allocations` looks up volume and restore pins by task
        config id, so a preview can pick up another job's pins for the task
        whose real id matches. Found while fixing the plan's preemption
        preview, not seen on the rig. Skip the pin lookups for configs with no
        stored id.
  - [ ] **No CLI lists a job's allocations.** `job info` shows none, and
        there is no `job status`, so finding a failing allocation means
        `legion state query`.
  - [ ] **Storage tests need a higher fd limit.** Under the default 1024 on a
        32-core box, about ten `storage::` tests fail "unable to open
        database file". They pass with `ulimit -n 65536`. Note it in
        `docs/testing-and-benchmarks.md` or lower the per-test fd use.
  - [ ] **Re-verify on the rig:** the microVM clean stop (volume writes
        survive `instance stop`) and image pulls after a leader failover.
        Both fixes are unit-tested only. Also two local managers on
        different ports, each with its own `~/.legion/tokens/` file, and
        `rig_install_cli_token`'s per-address files on a 3-manager rig.

- [ ] **Ship a Nickel contract library for the jobspec.** `legion job schema`
      emits JSON Schema for the *rendered* JSON; an importable
      `legion/jobspec.ncl` a spec could apply as a contract would move whole
      classes of error to evaluation time. Needs a generator from the same
      source as the JSON Schema or it drifts — deferred out of the
      [Nickel migration](plans/nickel-config-language-plan.md).
- [ ] **Decide whether `legion fmt` is worth the dependency.**
      `nickel-lang-core`'s `format` feature pulls in topiary + tree-sitter;
      today users format with the standalone `nickel` CLI. Deferred out of the
      [Nickel migration](plans/nickel-config-language-plan.md).
- [ ] **Audit every name decision in depth.** Important, and gets harder the
      longer it waits.
- [ ] Performance and scale: prove Legion scales better than K8s, and than Nomad
- [ ] **`DestroyJob` reports a precondition failure as an opaque
      `Internal`.** Destroying a running job fails, correctly, with
      `"Job must be stopped before it can be destroyed"` — but that
      reason stays in the manager's log. The client gets
      `{"code": "Internal", "message": "Error deleting job"}`, which
      names neither the cause nor the fix. Wrong on three counts: the
      code should be a precondition/failed-precondition rather than
      `Internal`, the envelope should carry a `hint` ("stop the job
      first, or pass --asap"), and an agent reading `Internal` has no
      way to tell a bug from a thing it should do differently — exactly
      what the agent-readiness rules in AGENTS.md exist to prevent.
      Found when a benchmark teardown silently left 835 jobs behind.
      Worth auditing the other mutating verbs for the same pattern.
- [ ] **`schedulingPolicy` in the rig contradicts the scenarios.**
      `benchmarks/aws/configs/legion/manager-a.ncl.tpl` sets
      `schedulingPolicy = "bin_pack"` while
      `benchmarks/scheduler/scenarios/web_fleet.json` declares
      `"scheduling_policy": "spread"`. The manager setting wins, so every
      number from 2026-08-04 is a bin_pack number. That matters for the
      conflict work specifically: bin_pack is the maximally correlated
      policy — every planner's argmax is the same fullest-node-that-still
      -fits — so it is the worst case for optimistic concurrency and the
      best case for the two-pass bind. Decide which policy the campaign
      is supposed to measure, make the two agree, and measure the
      conflict rate under BOTH before quoting either.
- [ ] **Consider a batch job-submission RPC.** `RunJob` is one job per call,
      so a client submitting N jobs pays N round trips and N control-lane
      raft commits. Measured on the AWS rig 2026-08-04: a client submitting
      serially manages ~40 jobs/sec, of which ~25ms per job is CLI startup
      (~2ms), Nickel evaluation (~13ms) and the commit (~10ms) — and Legion
      placed each job before the next one arrived, so the client, not the
      scheduler, set the rate. The benchmark driver worked around it with a
      thread pool, but bulk submission is a real user path (CI fan-out,
      a `legion job run` over a directory, an agent registering a fleet of
      jobs). A `RunJobs` that commits once for the batch would cut both the
      round trips and the commits. Wants a plan first: batching changes
      idempotency (one key per batch or per job?), partial-failure
      semantics, and ACL scope evaluation, none of which are obvious.
      **Still open, and now measurable.** The benchmark rig no longer needs
      it — `legion-loadgen` reaches ~1,100-1,500 jobs/sec against one local
      manager with unary `RunJob` — but that is also the first honest
      baseline a `RunJobs` would have to beat, and the per-job control-lane
      commit is still there underneath it. The loadgen's concurrency curve
      is the evidence: throughput peaks near 128 in flight and *falls* to
      607 jobs/sec at 1,024, which is what a serialized commit looks like
      from the client side.
- [ ] **`a_task_that_ignores_its_stop_signal_is_killed_at_the_deadline` is
      flaky under parallel load.** Passes alone and in most full runs, fails
      intermittently in `cargo test --all` (seen 2026-08-04). Its own comment
      claims "a zero grace window is already expired, so the very first reap
      takes it — no sleeping, no flakiness", but it spawns a real process and
      asserts a reap deadline, so a loaded machine can reap before the child
      is scheduled. Not caused by the plan-apply work — `src/worker/task_manager.rs`
      is untouched by it. Make the assertion independent of the child actually
      having started, or gate on an observed start.
- [ ] **`simulated_fleet_feeds_the_worker_metric_registry` asserts a
      process-global gauge.** `tests/worker_simulation.rs` checks
      `simulated_nodes >= 4` while the binary's other tests run fleets of
      their own, so another fleet shutting down can pull the gauge under 4.
      Failed once in a full local run (2026-10-12, PR #31, which does not
      touch the simulator) and passed three reruns. Its counters are
      already asserted as deltas; read the gauge from this fleet, or
      serialize the tests that run fleets.
- [ ] **`acked_write_survives_any_single_node_loss` is bimodally flaky on
      small hosts.** Seen 2026-08-07 on a 4-core shared VM, debug build:
      either the whole test completes in ~1s or a round's survivors never
      re-elect and it dies at the 150s `wait_for` — nothing in between,
      and the failing state persists for stretches of minutes before
      clearing. Shown NOT to be caused by the JSONB-affinity migration:
      interleaved same-window runs had base `3a62fcd` failing 3/4 while
      the migration branch passed 3/4 (and each side showed the opposite
      pattern in other windows). The other three tests in
      `tests/raft_durability.rs` did not flap. Suspect the tight election
      timeouts the harness itself warns about ("under CPU starvation the
      tight election timeouts flap leadership") plus this box's scheduling
      jitter; wants either wider election timeouts under
      `cfg(debug_assertions)` in the harness or a diagnosis of why a
      two-survivor cluster can stay leaderless for 150 s. Seen again
      2026-09-21 on the candidate-sampling branch, same shape (~1 s or the
      150 s wait, nothing between): interleaved single-test runs had base
      `cafbe80` at 2/3 passing and the branch at 1/4, with the base
      failure identical to the branch's, so still environmental. It was
      also the one failure the full suite produced that night.
- [ ] **`teardown.sh` silently aborts when run non-interactively, leaving the
      whole rig billing.** It prompts `read -r -p "Destroy the entire benchmark
      rig?"` unless `--yes` is passed. Under `nohup ... < /dev/null` (how any
      unattended teardown runs) `read` gets EOF and returns non-zero, and
      `set -e` kills the script *before* it prints "aborted" — so the log ends
      on a successful-looking S3 sync line and every instance stays up. Hit
      tonight: teardown appeared to succeed twice while the full rig kept
      running. Fix: detect a non-tty and either default to proceeding or fail
      loudly, never exit silently on a confirmation prompt. Audit the other
      rig scripts for the same `set -e` + `read` shape.
- [x] **Bake the operator box's toolchain into an AMI.** Standing the box up
      installed build-essential, protoc, clang, terraform, AWS CLI v2 and
      rustup in user-data on every launch: `cloud-init analyze blame` measured
      `modules-final/config-scripts_user` at **3245 seconds (54 minutes)** on
      one launch and ~14 minutes on another — same instance type, same region,
      same script, so it is `apt-get` throughput and nothing the rig controls.
      All of it paid before any Legion code compiles.
      `benchmarks/aws/scripts/bake-operator-ami.sh` pays it once into a tagged
      AMI (~20 min, ~$0.05/mo for the snapshot) and `bootstrap-operator-box.sh`
      launches from it, leaving only per-instance setup — the rig's SSH key
      pair — in user-data, so first boot is seconds. Both paths run the one
      `scripts/operator-toolchain.sh`, whose content hash is tagged onto the
      image: a toolchain change makes the bootstrap report the image stale and
      fall back to the slow path rather than launch a box missing a tool. No
      baked AMI means today's behavior exactly, with a log line saying so.
      See the README's "Bake the operator AMI first" and
      [calls #29](calls/aws-comparative-benchmark-plan_calls.md).
  - [ ] **Bake one against real AWS.** Written and exercised against a stubbed
        `aws` CLI only (every path: no AMI, current AMI, stale AMI, re-run,
        `--show`), because a campaign was billing at the time and an extra
        instance or image would have confused its teardown. Unverified: that
        console output survives the bake instance's stop (the version tag
        degrades to `unknown` if not — the power state, not the console, is
        the completion signal), and that the real bake lands inside the
        90-minute timeout.
- [ ] **Share one target dir across plugin builds.** `build.rs` gives every
      plugin its own `CARGO_TARGET_DIR` (`target/plugin-build/<kind>/<name>`)
      and builds them sequentially, and every plugin depends on the whole
      `legion` crate — so a `LEGION_BUILD_PLUGINS=1` build compiles the same
      ~470 dependency crates six times in release, ~2,950 crate-compilations
      and ~10.9 GB where ~600 and ~2 GB would do. It dominates the 16m48s
      clean build, and the AWS rig pays it on every `build-and-upload.sh`.
      The blocker is a name collision, not the target dir: `node_checks/example`
      and `volumes/example` are both crate `example`, so one shared dir would
      have them clobber each other's `libexample.dylib`, which is what the
      harvest step globs for. Rename the plugin crates unique, then share the
      dir — sequential builds mean no lock contention, and identical profile +
      dependency versions mean cargo reuses everything after the first plugin.
- [x] **Binary size: ship a `dist` profile.** `cargo build --profile dist` (LTO +
      `codegen-units = 1` + strip) takes the binary from 43.7 MB to 26.0 MB. See
      "Building a shipping binary" in [development.md](development.md).
- [ ] **Re-measure binary size on linux/amd64.** The 26 MB figure is macOS arm64;
      Go binaries run 7-9% larger on amd64 and Legion has no cross-compile
      target set up, so the shipping number is probably a bit higher.
- [ ] **Decide whether the UI ships inside the binary.** `ui/` is served from
      disk today, so the 26 MB is not comparable to Nomad (150 MB) or k3s
      (81.5 MB), both of which embed their web assets. Embedding it is the
      simpler operator story; it costs binary size on workers that never serve
      a UI.

---
- [ ] **`cargo clippy --all-targets` fails on existing code**
      (`daemon.rs` items-after-test-module, `tests/node_drain.rs`
      needless_borrow). The project's standard `cargo clippy --all` passes.
- [ ] **Flaky timing tests under load.** `worker::forwarder`
      (`sync_stops_a_listener_that_is_no_longer_desired`,
      `failover_skips_a_dead_endpoint_and_still_serves`) and
      `manager::daemon::tests::wait_for_next_eval_does_not_busy_loop_when_only_future_work_exists`
      each failed once in full parallel runs and pass on rerun.
- [ ] **Create the `proto-breaking-ok` label** that skips the new
      `proto-breaking` CI job (audit M5).

## Long term

- [ ] A **speed** scheduler that optimizes for fast placement. Needs a plan
      before any code — the mechanism is an open design question, not a settled
      one.
- [ ] A **VM** runtime beyond the microVM sandbox — libvirt? cloud-hypervisor?
      QEMU?
- [ ] **Windows support pass**
  - [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
        image consumption (see the follow-ups in
        [artifact-system-plan.md](plans/artifact-system-plan.md))

---

# Done

## Configuration language

- [x] **Nickel replaces Jsonnet as the configuration language** (2026-08-03,
      [plan](plans/nickel-config-language-plan.md) ·
      [calls](calls/nickel-config-language-plan_calls.md) ·
      [docs/configuration-language.md](../docs/configuration-language.md)).
      Every surface — job specs, manager config, worker configs, ACL policy
      files, job templates — is `.ncl`, evaluated through one module
      (`src/config_lang.rs`) with Nickel's positioned diagnostics. The forked
      `jsonnet-rs` git dependency and its C++ VM are gone. Jsonnet's ext-vars
      and top-level args collapse into a single `vars` record and four flags
      (`--var`, `--var-code`, `--kv-var`, `--kv-var-code`); job templates hide
      their `job` function with `| not_exported` and reject imports lexically
      before evaluation; `source_jsonnet` became `source_nickel` in the proto,
      the store, and the UI. `tests/config_jobs.rs` grew coverage for the
      manager configs, the worker configs, and the shipped job template, none
      of which were evaluated by any test before. **No compatibility path**:
      `.jsonnet` files are not recognized and stored template sources do not
      migrate (deliberate — see calls #4).
- [x] **`.json` accepted on four of the five config surfaces** (2026-08-03,
      [calls](calls/nickel-config-language-plan_calls.md) ·
      [docs](../docs/configuration-language.md#json-for-machine-emitted-config)).
      For machine-emitted config — generators already have a JSON serializer,
      and JSON is *not* a Nickel subset, so without this a generated object
      fails as `invalid contract expression`. Dispatch is by extension only
      (`config_lang::format_for`), no content sniffing. `--var` against a
      `.json` file is an error, not a silent no-op; Nickel's native
      `import "x.json"` is the documented way to mix the two. Job templates
      stay Nickel-only — a template's `job` is a function, which JSON cannot
      express — and `legion template create` rejects a `.json` file with that
      reason. Shipped example: `configs/jobs/machine-generated.json`.
- [x] **Local jobspecs are evaluated in place** (2026-08-03). `legion job
      run/plan/validate` used to copy a local spec into a temp directory,
      which stranded every relative import — `configs/jobs/demo-imports-2.ncl`
      did not run from the CLI. Pre-existing under Jsonnet, where the import
      callback `unwrap()`ed the failed read and panicked. Remote sources still
      stage, correctly: the fetcher brings the whole tree.

## Worker nodes

- [x] **Node liveness off the raft path, and a lost window that moves with the
      cadence.** Two plans, shipped together 2026-08-03:
      [liveness-off-the-raft-path-plan.md](plans/liveness-off-the-raft-path-plan.md)
      ([calls](calls/liveness-off-the-raft-path-plan_calls.md)) and
      [heartbeat-cadence-at-scale-plan.md](plans/heartbeat-cadence-at-scale-plan.md)
      ([calls](calls/heartbeat-cadence-at-scale-plan_calls.md)) ·
      [docs: node-liveness.md](../docs/node-liveness.md),
      [heartbeat-cadence-and-scale.md](../docs/heartbeat-cadence-and-scale.md)
  - [x] A steady-state heartbeat costs **no replicated commit and no fsync**:
        it is one write into the status-lane leader's in-memory
        `LivenessTable` (`src/storage/liveness.rs`). Only status transitions
        replicate. `legion_raft_pipeline_depth{lane="status"}` should sit at
        ~0 between display checkpoints.
  - [x] **The failover grace runs from liveness RESPONSIBILITY**, not process
        uptime — closes the "status-lane leadership change has no lost-node
        grace" item that used to live in Open. Acquisition seeds every active
        node at `now` and marks nothing lost for `liveness.failoverGrace`
        (60s), so an election on a manager that has been up for hours marks
        **zero** nodes lost where it previously marked the whole fleet.
        `legion_liveness_responsible_seconds` resets at every acquisition.
  - [x] **Per-node lost windows** (`node_health.lost_after_seconds`, additive
        migration, NULL = the old constant): a node is lost after
        `missedBeatsBeforeLost x its own interval`, so a 2s pool and a 60s
        pool coexist with 10s and 300s windows. `5 x 2s` reproduces the
        historical fixed 10s exactly. The worker reports its resolved cadence
        in `config_attrs.heartbeatSeconds`.
  - [x] **`heartbeatSeconds` clamp widened 30 -> 300**, gated on a hard
        startup check against `stateful.onLost.selfTerminateAfter` — closes
        the "unvalidated against each other" item that used to live in Open.
        A config that would make a healthy worker self-fence between beats
        fails manager startup, naming both numbers.
  - [x] **Mass-disruption circuit breaker**: >50% of the active fleet lost in
        one tick marks none and logs at `error`. Suppressed under 20 nodes.
  - [x] **Peer-gap suppression and the liveness digest**
        (`RaftTransport.ReportNodeLiveness`): a manager that does not lead the
        status lane sends ONE RPC per second carrying the ids it heard from,
        whatever the fleet size, and a quiet peer's nodes are not marked lost
        inside `liveness.peerDigestGrace`. Falls back to per-heartbeat
        forwarding against a manager that predates the RPC.
  - [x] **Freshness**: `legion_heartbeat_freshness_ratio` (the number to alert
        on) plus served-interval / lost-window / sustainable-nodes gauges and
        `legion_nodes_marked_lost_total`, a debounced runtime warning that
        computes the `heartbeatSeconds` to set, and `legion node cadence`
        (`--json`, MCP tool, capability entry, `GetHeartbeatCadence` + ACL row).
  - [x] **Adaptive cadence, opt-in** (`heartbeat.targetRatePerSecond`): fleet-
        size-derived interval on a Fibonacci-ish ladder with step-up-now /
        step-down-after-5m hysteresis, served on `HeartbeatReply
        .next_interval_seconds`, with widen-immediately / narrow-on-confirmation
        windows and a one-shot full-width re-phase in the worker sleep. The
        simulated fleet honors it through the same shared helpers.
  - [x] **Adaptive cadence on by default** (2026-10-09, phase 5): default
        `targetRatePerSecond` 2,500 (2s through 2,500 nodes, 21s at 20k), an
        explicit `heartbeat.adaptive = false` opt-out, the served interval as
        a floor over worker-type `heartbeatSeconds`, followers relaying the
        leader's interval on the liveness digest reply, and per-tick
        widen-only windows so late registrants and fast pools are covered.
        The AWS rig's `collect.sh` archives the served interval and lost
        window; `packing-density.sh` judges cadence against the served
        interval. [cadence plan §13](plans/heartbeat-cadence-at-scale-plan.md) ·
        [calls 2, 3, 5, 9, 11, 14](calls/heartbeat-cadence-at-scale-plan_calls.md) ·
        [docs](../docs/heartbeat-cadence-and-scale.md#adaptive-cadence).
  - [x] **A returning node is schedulable again.** Node loss is a cross-lane
        composite (status lane `lost`, control lane `unavailable`), and the
        scheduler gates on `availability` — so recovery undoes both halves. A
        worker that rides out a partition and resumes heartbeating recovers
        without restarting or re-registering. Guarded so a heartbeat can
        never un-cordon an operator-drained node.
  - [x] **Display checkpoint + `NodeMsg.heartbeat_source`**: one batched
        status-lane commit per 30s records each node's OBSERVED beat time, so
        every manager can show an approximate "last heartbeat" and say that it
        is approximate. Never read back as liveness. `"0s"` disables it.
  - [ ] The AWS scale runs for both plans are **not done** — see the open
        items in "Benchmarks and scale" above.
- [x] Add a worker node
- [x] CLI arg to get the address
- [x] Makes contact via RPC
- [x] Message passing between the two via RPC
- [x] Heartbeats; marked unhealthy on miss, watchable from the CLI
- [x] Basic fingerprinting system
- [x] Dynamic attributes that can be updated
- [x] Shows up in the manager's ledger of worker nodes
- [x] Can be restarted from the server

## Scheduling

docs: [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md), [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md)

- [x] **Gang scheduling, Phase 1** (2026-10-17,
      [plan](plans/gang-scheduling-plan.md) as revised ·
      [calls](calls/gang-scheduling-plan_calls.md) ·
      [docs](../docs/gang-scheduling.md)). Open follow-ups are under
      Scheduling & placement above.
  - [x] `gang = true | { min = N }` on a task, with submit validation
        (`count.per`, keyed/session stateful, a shared group, `min` bounds,
        multi-step rollouts, sequential slots) and a `task_config.gang`
        column.
  - [x] A gang short of its quorum makes its whole plan place nothing and
        blocks on the shipped blocked-eval queue; above the quorum the rest
        trickles in. Top-ups are all or nothing, and a backoff hold on one
        member holds the whole top-up. Gang tasks never use truncated index
        candidates.
  - [x] Preemption never takes a gang below its quorum.
  - [x] The rollout engine replaces a gang group in one step.
  - [x] `PlanJobReply.gang_reports` (a deferred gang is a successful dry
        run), `JobBlocked.gang`, `job plan` / `job info` / UI / MCP
        rendering, `legion_gang_blocked_jobs`.
- [x] **Elastic session envelopes, Phase 1.** `stateful.envelope.max` and
      `shrinkAfter`: each node grows its session envelope toward the
      ceiling as its sessions fill it and shrinks it after they drain, one
      scheduler-lane entry per resize on the envelope's own allocation row,
      taking only free room, with the node capping its own admission
      before a shrink. A fixed envelope stays the default
      ([plan](plans/elastic-session-envelopes-plan.md),
      [calls](calls/elastic-session-envelopes-plan_calls.md),
      [docs](../docs/session-mode.md#elastic-envelopes)).
- [x] **Submit, update, and scale-out evals settle with their plans**
      (2026-10-14, [plan](plans/multi-eval-plans-plan.md) Phase 2 ·
      [calls](calls/multi-eval-plans-plan_calls.md) ·
      [docs](../docs/scheduler-and-eval-loop.md)). The fused settle is
      armed in `job_submit` (first config), `job_update`, and `job_scale`
      (scale-out), so each costs one entry fewer
      (`tests/raft_job_eval_ops.rs`). A partial plan no longer settles its
      eval, which closes a lost-follow-up window `job_reschedule` had, and
      a settled eval is no longer written a second time by its batch's
      settle.
- [x] **The planner's node universe truncated to one slice at rig scale —
      root-caused and fixed** (2026-09-21,
      [findings](aws-phase5-findings.md#open-in-priority-order),
      [calls](calls/scheduling-index-plan_calls.md) #14,
      [docs](../docs/scheduling-indexes.md)). Not the read model: the
      scheduling index's candidate budget (64 for a task needing 4) was
      applied as a cut of the best-first verified list, i.e. one tied
      cell's sorted-id prefix, so every plan saw the same lowest-id nodes.
      The cut inside a tied cell is now a uniform sample of the cell
      (`truncate_within_boundary_cell`); `tests/index_truncation_spread.rs`
      pins the rig shape (4 slices x 150 identical nodes, 40 jobs x 4,
      spread): 104/56/0/0 per slice on the old code, 49/30/43/38 fixed.
      Candidate
      construction cost is unchanged within noise
      ([benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
- [x] **The read-model refresh takes the changeset delta, not a whole-node
      reload.** 2026-08-05, from the phase-4 fill-scaling finding
      ([findings](aws-phase4-findings.md),
      [calls](calls/eval-path-raft-cost-plan_calls.md) #2).
      `cache_refresh_allocation_scopes` reloaded every touched node's ENTIRE
      allocation list after each committed changeset, on every replica — ~440
      rows at 50,000 allocations and ~1,760 at 200,000 to learn about the ~44
      the changeset already named. `cache_apply_allocation_delta` now reads
      exactly the named rows by primary key and splices them in, so the cost
      follows the change instead of the fill. What it gives up is
      self-healing, bounded three ways: a per-node fallback to the full
      reload when the delta contradicts the cache, a whole-changeset fallback
      when a record cannot be classified, and a round-robin scrub that
      reloads one node in full per projected changeset, so drift is repaired
      within one sweep of the fleet. Two new `warn!` lines fire only when the
      cache and committed state disagree.
- [x] **Plan-apply conflict reduction: the epoch is a ranking hint, not a
      gate.** 2026-08-04, from the AWS phase-2 finding that **74% of plan
      applies were rejected and re-planned**
      ([findings](aws-phase2-findings.md),
      [plan](plans/plan-apply-conflict-reduction-plan.md),
      [calls](calls/plan-apply-conflict-reduction-plan_calls.md)).
      Three things shipped together:
    - **Two-pass bind** in `apply_plan_submission_on`. Pass 1 is the old
      walk byte-for-byte; pass 2 revisits only epoch-moved candidates and
      admits them on the in-transaction revalidation that already runs
      (node row, availability, constraints, free pool with this plan's
      binds debited, static ports, `distinct`). Rejections now name
      capacity or constraint instead of staleness. `target_instance`
      ratification keeps the hard gate. New
      `legion_scheduler_plan_bind_pass{pass}`.
    - **Candidate lists restored.** `decouple_plan_components` was
      collapsing every multi-placement plan to ONE candidate whenever two
      placements shared a candidate node — which for a multi-replica job
      into one pool is always, so late binding was structurally off for
      the workloads it was built for. Overlap edge deleted (the in-tx
      pool debit already covers it); `distinct` and co-placement edges
      kept. Fallback tail now samples from the top-W by score instead of
      a deterministic top-N that handed every concurrent planner the same
      list.
    - **A pre-existing partial-apply bug**, found on the way: a *rejected*
      plan committed the placements it had already bound — allocation
      rows no `allocation.placed` event announced, inside a submission
      marked `rejected`. Only evictions were savepointed; the savepoint
      now covers the whole plan body. Removing the overlap edge would
      have widened that window sharply.
      1482 tests pass, clippy and fmt clean, and the oversubscription and
      job-config-epoch fences pass **unmodified**.
- [x] **Priorities and eviction/preemption, with eviction toggled at the pool
      level.** Six named priority bands (`critical` / `high` / `elevated` /
      `normal` / `low` / `background`) driving both eval-queue order and
      preemption; victims chosen during planning and committed — deleted,
      requeued, journalled — in the same epoch-CAS transaction that inserts
      the preemptor's allocations. Strictly cross-band, minimum victim age,
      minimal victim sets, `disruption.minAvailable` as a hard floor on
      voluntary disruption only, `enforce`/`observe`/`off` modes and
      `schedulerSettings.preemption.disabledPools` for the pool toggle,
      `job plan` preview, `allocation.preempted` events with a
      victim↔preemptor link, and `legion_preemptions_total`.
      [plan](plans/preemption-and-eviction-plan.md) ·
      [calls](calls/preemption-and-eviction-plan_calls.md) ·
      [docs](../docs/preemption-and-eviction.md)
  - [x] Phase 1a — stop causes. Every allocation removal records *why*,
        delivered to the owning worker and carried in `allocation.removed`
        metadata. [docs](../docs/graceful-stop.md)
  - [x] Phase 1b — graceful termination. `stop { signal, killAfter }`,
        per-runtime stop ladders, `LEGION_STOP_NOTICE_PATH`,
        `allocation.stopping`, stops-before-starts reconcile ordering, and the
        worker-side terminating set + reaper the blocking-`stop()` hazard
        required. [docs](../docs/graceful-stop.md)
  - [x] Phase 1b's grace-vs-latency question, resolved by taking neither
        option: no preemption-specific grace (it would make the ladder
        cause-dependent) and no accounted-not-free start (it would let a
        container runtime run victim and preemptor on the same reserved
        memory). The 5m `killAfter` cap is the backstop; documented as a known
        limitation.
  - [x] Plugin ABI version symbol. `declare_task_plugin_abi!()` is now
        required of every task plugin, so a stale dylib fails at load with a
        message naming the rebuild instead of mismatching silently.
        [docs](../docs/task-runtime-plugins.md)
  - [x] Phase 2 — priority bands, governed by a `bands` selector on `job` ACL
        rules (not `operator:write`), normalized before enforcement and
        covering the template-render path, with explicit cases in
        `tests/acl_endpoints.rs`. The selector composes with `namespaces`, so
        per-namespace band caps need no second surface — but they are not
        *live* until namespaces carry real values
        ([namespaces-plan.md](plans/namespaces-plan.md)); today the selector
        delivers band caps scoped by job name and principal only.
        [docs](../docs/auth-and-acls.md)
  - [x] Phase 3 — plan-atomic preemption. [docs](../docs/preemption-and-eviction.md)
  - [x] Phase 4's UI item — preemption history on the job page, the band badge
        on the job header, and the `priority` column in `job list`.
  - [x] At least think through fair-share policies — find the 80/20 rather than
        building a full hierarchical scheduler. The 80/20 is priority bands
        plus per-namespace band caps via the `bands` ACL selector (the caps go
        live when [namespaces](plans/namespaces-plan.md) ship);
        fair-share/hierarchical queues stay an explicit non-goal (Kueue's
        policy-matrix issue history is the argument against).
- [x] Ledger command — see every node and what's on it from the CLI
- [x] A framework for scheduler plugins
- [x] A "random" scheduler
- [x] Constraints
- [x] A "bin-pack" scheduler
- [x] A "spread" scheduler (the inverse of bin-pack)
- [x] Affinities and anti-affinities
- [x] Task groups
- [x] Rescheduling, including health-check-triggered reschedule after restart
- [x] Job health checks reschedule a job — initial failures restart it, failure
      after N times forces a reschedule
- [x] Dynamic metadata: a node attribute change re-evaluates affected jobs
      (`UpdateNodeDynamic` enqueues evals)
- [x] Plan epochs are sampled at *snapshot* time, not after planning. The
      version vector used to fence a plan submission was read after
      `build_plan`, so a competing plan that applied in between was already
      counted in the epoch and the compare-and-set passed against stale
      capacity — an intermittent node oversubscription
      (`concurrent_plans_cannot_oversubscribe_a_node`). The scheduler now
      samples every candidate node's epoch before it reads
      `cached_allocations_by_node`, in one batched query
      (`Store::allocation_version_vector_for_candidates`), and selects the
      affected subset at submit time.
- [x] **Declarable scheduling indexes** — ad-hoc `partition_by` × `dims`
      indexes over node attrs and free resources, nearest-fit candidate
      generation, automatic per-task selection, shadow mode, `job plan`
      explain output, Prometheus series, and the default-on `fit` index
      (free cpu/memory/disk by pool) armed above `index_min_nodes` —
      [plan](plans/scheduling-index-plan.md),
      [docs/scheduling-indexes.md](../docs/scheduling-indexes.md).
      Deferred pieces are open items above (group-path candidates, disk
      fingerprinting, backlog).
- [x] AGENTS.md now requires before/after benchmark numbers on scheduler
      changes, and `benchmarks/micro/scheduler_bench.rs` grew a
      `candidate_construction` group that isolates candidate-set cost
      (indexes on vs off) across fleet sizes.
- [x] **Late-binding plan apply** (phases 0–3) — plans carry a ranked
      candidate list per placement (format-2 submissions) and the applier
      binds each placement to the first candidate whose node epoch is
      unchanged, so parallel submits racing for one node cost a fallback
      rank instead of a conflict retry; coupled placements (overlapping
      lists, `distinct` tasks, multi-task groups) keep exact all-or-nothing
      semantics. Conflict-exhausted evals now requeue with jittered backoff
      (`eval.requeued`) instead of failing terminally. Includes the
      `plan_candidates` setting (default 8; 0 = kill switch + format-1
      emission), `job plan` candidate preview, `candidateRank`/`fallbacks`
      event metadata, the rank histogram + `fallback` outcome metrics, and
      the `tests/plan_contention.rs` burst harness (also the raft-mode
      concurrent-planning regression) —
      [plan](plans/late-binding-plan-apply-plan.md),
      [docs/late-binding-plan-apply.md](../docs/late-binding-plan-apply.md).
      Phase 4 backlog and the capacity-exhaustion gap are open items above.
- [x] **Adaptive scheduler concurrency** (phases 1–3) — the manager
      measures its own plan-apply conflict rate and tunes how many evals it
      will PLAN at once between a floor and `evalThreads`, so `evalThreads`
      becomes a ceiling rather than a guess. AIMD on a smoothed conflict
      rate (halve above 20%, add one back after 2s below 5%, integer limit,
      dead band + refractory + asymmetric dwell, starts at the ceiling),
      with a **goodput guard** that reverts a decrease which did not improve
      placements/sec and says `SATURATED` — the answer there is nodes, not
      patience. The actuator is an admission gate on planning
      (`PlannerGate`: one permit per worker per batch, held across the
      batched loop's fused settle-and-claim), never a resized worker pool; a
      throttled worker narrows to `ClaimScope::NonPlanning` instead of
      blocking so `job stop` never queues behind a congestion storm. Ships
      `legion scheduler concurrency` + `GetSchedulerConcurrency` + the
      read-only MCP tool, six metrics, and the
      `scheduler.concurrency_adjusted` / `_saturated` events. **Default is
      `observe`**: it computes and exports the limit it would enforce and
      actuates nothing — and the plan's motivating "4 beats 16" measurement
      was retracted after the feature was built, so `observe` is also the
      only honest mode until a conflict-bound cell is measured on the
      current build — [plan](plans/adaptive-scheduler-concurrency-plan.md),
      [calls](calls/adaptive-scheduler-concurrency-plan_calls.md),
      [docs/adaptive-scheduler-concurrency.md](../docs/adaptive-scheduler-concurrency.md).
      Phase 0 (measurement) was skipped and phases 4–5 are open items above.
- [x] **The `nodes_considered` scheduler tests are deterministic**
      ([#29](https://github.com/mikenomitch/legion/pull/29)).
      `spread_limits_nodes_considered_by_count` and its bin_pack twin failed
      about once per 3,000 runs: their node load was the job's own
      allocations, which a Fresh plan drops, so every pick was uniform. The
      load now comes from a filler job, and the subset RNG is pinned per
      iteration through a test-only `Scheduler` seed.

## Task runtimes

docs: [task-runtime-plugins.md](../docs/task-runtime-plugins.md)

- [x] A framework for task-runtime plugins
- [x] **Sandbox sessions boot in about 200 ms, and a cluster boots them at
      the sum of its nodes' rate** (2026-10-14). Measured on AWS, four
      4-vCPU workers: one boot from 2.9 s to 0.23 s, a burst of 160 from
      1.6 to 69 guests a second, and 40 a second sustained with none
      refused. cloud-hypervisor disks are declared raw (every boot had
      panicked and rebooted) and its console is `hvc0`. A sandbox session
      is `running` once its guest takes exec. `stateful.placement` (`spread`
      | `fast`). The router asks the least full node when its view says
      full. Shared registry clients and one tag resolution per burst. A
      session's stop sends its guest request off the lock and writes no
      state file. `/proc` start times. `posix_spawn` for VMMs. Pooled launch
      threads. O(changes) session-index heartbeats. Stacked with the base
      root disk cache, the router fixes, and per-stage start timings, and
      the `legion-loadgen --mode sessions` rig in
      `benchmarks/aws/sandbox-boot/` , 
      [plan](plans/sandbox-boot-throughput-plan.md) ·
      [calls](calls/sandbox-boot-throughput-plan_calls.md) ·
      [docs](../docs/sandboxes-at-scale.md#measured-on-aws) ·
      [boot](../docs/task-runtime-sandbox.md#how-fast-a-sandbox-boots) ·
      [placement](../docs/session-mode.md#picking-a-node)
- [x] **Startup probing for health checks** (2026-10-13). A `tcp` or `http`
      check that has not passed since its task (re)started is also probed
      between `every` ticks (100 ms doubling to 1 s), and only a pass counts,
      so a task turns healthy within about a second of its port opening
      instead of a whole `every` (10 s by default) later. Failure semantics
      are unchanged, [plan](plans/task-start-critical-path-plan.md) ·
      [calls](calls/task-start-critical-path-plan_calls.md) ·
      [docs](../docs/jobs-tasks-and-scheduling.md#startup-probing)
- [x] **Gated namespaces exist before the listeners inside them** (2026-10-13).
      `heartbeat_tick` created a gated container's namespace after it tried
      to bind its proxy, resolver, and upstream listeners inside it, so the
      task started with no network and its next beat reported
      `net_capture_unavailable`. The namespaces are now created first , 
      [plan](plans/task-start-critical-path-plan.md) §3. Needs Linux to
      exercise. Compile-checked only.
- [x] **microVM sandboxes stop cleanly, so volume writes survive a stop**
      (P0 from the 2026-10-12 AWS spot check). The guest agent signals init
      with `stop.signal`, init flushes, forwards it, unmounts the volumes,
      and powers off, and the worker holds the volumes until the VMM exits.
      Hard stops, restarts, disk suspends, and memory captures flush the
      guest first. `stop { signal, killAfter }` now applies to sandboxes —
      [calls](calls/sandbox-clean-stop_calls.md) ·
      [docs](../docs/task-runtime-sandbox.md#stopping-a-sandbox)
- [x] Rename "task kind" → "task runtime"
      ([plan](plans/task-runtime-rename-plan.md), single-pass, no shim)
- [x] A **noop** runtime ([docs](../docs/task-runtime-noop.md))
- [x] **Wake on request, Phase 1**: `suspend.wake = { port, timeout }`,
      `legion start gateway` (host, `Legion-Instance`, `CONNECT`, and
      `POST /v1/wake/<id>` routing; held requests with one resume per burst;
      `503` + `Retry-After` past the timeout; optional TLS; running
      instances reachable through a manager outage), and `InstanceInfo.wake`
      and `endpoints` on `GetInstance`.
      [plan](plans/wake-on-request-plan.md) ·
      [docs](../docs/gateway.md)
- [x] **Gated sandboxes, Phases 1 to 5** of the
      [gated sandbox checkpoints plan](plans/gated-sandbox-checkpoints-plan.md)
      ([calls](calls/gated-sandbox-checkpoints-plan_calls.md)), built on a
      host without KVM and tested against fake VMMs, Unix sockets standing
      in for vsock, and a real network namespace for the guest's netlink.
      Phase 0's boot test and the KVM checks are open above.
  - [x] Phase 1: gated sandboxes take memory checkpoints (suspend,
        snapshots, forks, publish). Every restore moves the guest's vsock
        onto its own allocation's socket (Firecracker `vsock_override`, a
        rewritten cloud-hypervisor `config.json`), the checkpoint records
        its network shape, a gated-or-TAP mismatch is refused by name at
        restore, the agent drops dead relays, the restore waits briefly for
        the datapath, and Firecracker older than 1.16.1 is refused before
        capture. [docs](../docs/suspend-and-resume.md#gated-sandboxes)
  - [x] Phase 2 (all but the flip): `gated-default` deny on every gated
        runtime, a resolver for every gated sandbox, lower-case proxy vars
        and `NODE_USE_ENV_PROXY`, the static `legion-guest` agent (workspace
        crate `guest/`), submit refusals for gated plus TAP and for ingress
        ports on a gated sandbox (the latter lifted by Phase 5), `net policy
        show` resolving `auto`, and the
        shipped `configs/policies/agent-sandbox-egress.ncl`. The `auto` to
        `gated` flip is built and held (open above).
        [docs](../docs/network-handlers.md#default-deny)
  - [x] Phase 3: `connections` counts open gated egress, and `network`
        stamps bytes as they move, so a long quiet model call keeps a gated
        sandbox awake. [docs](../docs/suspend-and-resume.md#what-each-signal-sees-on-a-sandbox)
  - [x] Phase 4: Legion addresses TAP guests. Worker `sandbox.bridges`
        pools, a per-allocation lease, `ip=` at boot, netlink re-addressing
        and a fresh lease on every restore and fork, `LEGION_GUEST_ADDR`,
        `instance get`'s `address:` line, the `sandbox.bridges` node
        attribute and implicit constraint, and `config.network.tap` and
        `mac` removed (disk forks of TAP sandboxes allowed).
        [docs](../docs/task-runtime-sandbox.md#addressing)
  - [x] Phase 5: ingress into sandboxes. The ingress listener dials a gated
        guest through the VMM's base vsock socket (`CONNECT <port>` / `OK
        <n>`, `src/worker/netd/vsock.rs`), the guest agent listens on a
        vsock port per advertised port and relays to the workload's
        loopback, a TAP guest's ports are fronted at its leased address,
        the guest's ports ride the checkpoint so a restore dials the ports
        the restored workload holds, and submit refuses ports only on a
        loopback-only sandbox. G5.1 is open above.
        [docs](../docs/network-handlers.md#ingress-into-a-sandbox) ·
        [plan §6.10](plans/gated-sandbox-checkpoints-plan.md#610-ingress-into-a-gated-sandbox-phase-5)
  - [x] Found while building Phase 5, now fixed: a gated guest's
        `127.0.0.1:1080` is the agent's proxy relay, so submit refuses an
        advertised port the guest would bind on 1080, and an upstream
        `localPort` of 1080 or 53
        ([calls entry 38](calls/gated-sandbox-checkpoints-plan_calls.md#38-an-upstream-localport-of-1080-or-53-and-a-guest-port-of-1080-or-1100-are-refused-on-a-gated-sandbox)).
  - [x] Upstreams in gated sandboxes: the guest agent listens on each
        upstream's loopback port and VIP and relays over vsock to the
        node's own upstream listener (bound on
        `<alloc>/.sandbox/vsock.sock_<local port>`), so HTTP mode, policy,
        handlers, and credential injection apply unchanged; a restore
        retargets the agent's relays to the new allocation's ports. G5.2 is
        open above. [docs](../docs/network-handlers.md#sandboxes) ·
        [plan §6.10, "As built"](plans/gated-sandbox-checkpoints-plan.md#610-ingress-into-a-gated-sandbox-phase-5)
- [x] **Sandbox guest size follows `resources`** (`memoryMax`, else
      `memory`), so the scheduler's reservation and the VM's size can no
      longer drift apart
      ([plan Phase 2](plans/suspend-resume-and-oversubscription-plan.md#8-implementation-phases),
      [docs](../docs/task-runtime-sandbox.md#guest-size)). An explicit
      `memory_mib` still wins for one release, with a warning.
- [x] **`sandbox` adopts `resource_enforcement()`** as `WorkerCgroup`: the VMM
      joins the allocation's worker cgroup, `memory.max` = guest +
      `sandbox.vmmOverheadMiB` (default 64), and its usage is measured
      ([plan Phase 1](plans/suspend-resume-and-oversubscription-plan.md#8-implementation-phases),
      [docs](../docs/resource-enforcement.md#sandbox-tasks)). Chose
      `WorkerCgroup` over the `RuntimeApplied` shape this item first proposed:
      the VMM is a host process, so the cgroup ceiling is the honest limit and
      guest size stays the task config's job.
- [x] An **exec** runtime ([docs](../docs/task-runtime-exec.md)) — runs the given
      command and args, as a specific user, joining an existing cgroup (v2 only),
      with landlock
- [x] A **container** runtime ([docs](../docs/task-runtime-container.md)) with no
      Docker dependency: macOS uses Apple `container`; Linux uses a low-level OCI
      runtime (`runc` 1.2+ by default, or `crun` via `LEGION_OCI_RUNTIME`;
      youki, the original default, was dropped on 2026-10-12). Linux runtimes
      can't pull images, so the runtime builds an OCI bundle from the image's OCI layout
      ([oci_bundle.rs](../src/task_plugins/oci_bundle.rs)).
- [x] A **sandbox** runtime ([docs](../docs/task-runtime-sandbox.md)) — OCI images
      in microVMs for untrusted code and agent workspaces
- [x] **Sandbox snapshot, fork, and suspend footguns** (2026-10-11): a fixed
      TAP is refused at submit wherever two guests could share a node, forks
      no longer act as their origin in Legion's own token checks, and a cold
      resume is recorded on the instance as `cold_boot`
      ([calls 135-140](calls/suspend-resume-and-oversubscription-plan_calls.md#135-a-fixed-tap-is-refused-wherever-two-guests-could-share-a-node-not-removed),
      [sandbox networking](../docs/task-runtime-sandbox.md#networking),
      [forks and snapshots](../docs/workload-identity.md#forks-and-snapshots),
      [when a resume cannot restore](../docs/suspend-and-resume.md#when-a-resume-cannot-restore)).
- [x] **The `container` runtime's four core gaps, closed** (2026-07-31) —
      [plan](plans/container-runtime-exec-and-signals-plan.md) ·
      [calls](calls/container-runtime-exec-and-signals-plan_calls.md). Six of
      its seven phases; Phase 3 (graceful stop) was **superseded before merge**
      by the preemption plan's Phase 1b, which shipped the same ladder as
      `stop { signal, killAfter }` — see the Phase 3 entry below:
  - [x] Phase 1 — exec on Linux via `<runtime> exec` for runc/crun, so
        `legion exec`, actions, the UI terminal, and MCP exec work on the OS
        the workloads ship on ([docs](../docs/exec-and-actions.md))
  - [x] Phase 2 — `TaskPlugin::signal()` seam with a tracked-pid default, and
        a container override that signals the container *init*
        (`container kill` / `<runtime> kill`); `changeMode: signal` identities
        on container tasks are accepted
        ([docs](../docs/workload-identity.md))
  - [~] Phase 3 — graceful stop. **Superseded, not shipped as designed.** The
        branch built `stopSignal` / `stopGracePeriod`; preemption's Phase 1b
        landed on `main` first with the same feature as
        `stop { signal, killAfter }`, one ladder for every stop cause. The
        duplicate surface was dropped at merge rather than shipped alongside
        — two jobspec fields for one behavior is the incoherence this repo
        keeps out. What the branch *kept* is the runtime-side half Phase 1b
        did not cover: the Linux container teardown ladder (signal → bounded
        wait → `delete --force` instead of an unconditional SIGKILL)
        ([docs](../docs/graceful-stop.md),
        [calls 19](calls/container-runtime-exec-and-signals-plan_calls.md))
  - [x] Phase 4 — the `resource_enforcement()` interface (`Unsupported` /
        `WorkerCgroup` / `RuntimeApplied` + a worker-computed
        `ResourceAssignment`), Linux memory hard limits, `cpu` MHz →
        `cpu.weight`, OOM-kill annotation from `memory.events`, and the
        whole-`cores` / `cores`-xor-`cpu` validation rules
        ([docs](../docs/resource-enforcement.md))
  - [x] Phase 5 — exclusive `cores` via cpuset pinning: worker core allocator,
        `legion/share` + `legion/reserve` slices, exhaustion failure path,
        assignment persisted and rebuilt on worker restart
  - [x] Phase 6 — the `exec` runtime adopts the interface (`WorkerCgroup`), so
        one node-wide fence governs container *and* exec tasks, exec gains
        real memory limits, and teardown reaps double-forked descendants via
        `cgroup.kill`; an explicit `config.cgroup` still wins
        ([docs](../docs/task-runtime-exec.md))
  - [x] Phase 7 — the task dir is mounted at `/legion/task` with translated
        env, flipping KV file injection, identity file delivery, and file/tree
        artifacts to supported in containers — refresh included, because the
        directory is mounted rather than individual files
        ([docs](../docs/task-runtime-container.md#the-task-directory-legiontask))
  - Enforcement is gated on the pre-existing opt-in `resourceEnforcement`
    worker config rather than shipping ungated, and it *replaces* the exec
    runtime's own per-allocation cgroup path (which conflated `cpus` with
    `cores` and used a CFS quota) — see calls 8, 10, and 16.
  - The two-phase stop this branch also built (signal under the lock, reap on
    the supervisor's tick) arrived independently in preemption's Phase 1b and
    is `main`'s implementation now; the branch keeps only its resource-release
    step, ordered strictly behind the reap so a cgroup is never removed while
    it still holds a live process
    ([calls 19](calls/container-runtime-exec-and-signals-plan_calls.md)).
- [x] **macOS local-dev spot check fixes** (2026-10-12): Apple `container`
      task containers are named `legion-<node>-<alloc>` and labeled
      `legion.node`/`legion.allocation`, and stale cleanup (`container` and sandbox dev mode)
      removes only a container labeled as this node's; every runtime CLI call
      has a deadline and kills its process group past it; readiness probes
      run off the task-manager lock; the gated-egress warning follows the
      task's `os` constraint
      ([docs](../docs/task-runtime-container.md#runtime-behavior)).

## Artifacts

[plan](plans/artifact-system-plan.md) · [docs](../docs/artifacts.md)

- [x] Grab container images (`oci://` fetcher + ORAS mediaType)
- [x] Auth for fetching (KV credentials via `GetArtifactCredential` + cluster
      `artifacts.auth` matchers)
- [x] Grab files onto disk (https/file/git/s3 fetchers, task dirs, checksums, safe
      unpack, `${artifact.<name>}` references, per-artifact status in CLI and UI)
- [x] A plugin system for fetchers (`plugins/artifacts/<scheme>/`)
- [x] OCI layout → runtime bundle conversion so Legion-managed images work on
      Linux container workloads
- [x] **Content store budgets that fit the disk, and a disk-aware checkpoint
      room check.** Content-store follow-up to the
      [suspend plan](plans/suspend-resume-and-oversubscription-plan.md),
      [calls 131 to 134](calls/suspend-resume-and-oversubscription-plan_calls.md#131-published-checkpoints-are-unpinned-on-their-node-content-store-follow-up) ·
      [docs: the node content store](../docs/artifacts.md#the-node-content-store),
      [the checkpoint disk budget](../docs/suspend-and-resume.md#the-checkpoint-disk-budget)
  - [x] `content.budgets.*` takes a size or a share of the content
        filesystem (`"20%"`), sized at worker start. Defaults are 10%
        clamped to 1..20 GiB (artifacts) and 20% clamped to 2..200 GiB
        (snapshots). Bad percentages and shares over 100% fail config load.
  - [x] Workers report `disk_free_bytes`, `min_free_disk_bytes`, and
        `reclaimable_bytes` on `SnapshotBudget`. A node's checkpoint room is
        `min(budget - pinned, free + reclaimable - minFreeDisk)`, used by
        the placement filter (whose error counts each limit), `legion node
        usage`, and the worker page's content card. `scheduler_bench`
        `snapshot_budget` reports disk numbers too.
  - [ ] Rerun `scheduler_bench` `snapshot_budget` and
        `candidate_construction` on a quiet host. The local before/after
        (2026-10-11, load average 11 to 22) moved `unreported/nodes_5000`,
        which never reaches the changed filter, from 11.8 ms to 21.6 ms, so
        it measured the host, not the change: `half_full/nodes_5000` 14.8 ms
        before, 23.4 to 29.1 ms after, `half_full/nodes_1000` 4.0 ms before,
        2.7 to 3.6 ms after. The filter itself is one pass over reported
        nodes per task, as before, with a `min` added per node.
  - [x] Published checkpoints are unpinned on their node (see Phase 7b
        under Scheduling & placement).

## Rollouts

[docs](../docs/rollouts.md)

- [x] A new version of a job starts a deployment
- [x] A framework for deployment strategies
- [x] An "immediate" strategy that cycles everything out in place
- [x] A "rolling" strategy
- [x] Health-gated progression, manual and automatic
- [x] A UI for all of it

## KV & secrets

[plan](plans/kv-reactive-task-updates-plan.md) · [docs](../docs/kv-store.md)

- [x] A task can read a KV value
- [x] Updating a value restarts the task, when configured to
- [x] A value can be used in the job definition
- [x] Reactive updates: injection into env and files, watch/on_change/on_delete
      policy, dependency graph, debounce, per-runtime capability gating

[plan](plans/kv-cert-delivery-plan.md) · [calls](calls/kv-cert-delivery-plan_calls.md) ·
[kv docs](../docs/kv-store.md#live-file-updates) ·
[net docs](../docs/network-handlers.md#certificates-from-kv) ·
[guide](../docs/guides/tls-certificates.md)

- [x] **Live KV file updates** (2026-10-10): `onChange = "update"` on a KV
      `files` entry rewrites it under the running task (atomic, with its mode)
      and applies `changeMode` (`noop`/`signal`/`restart`, plus `splay`). The
      manager moves allocations to the new config in place, with no rollout.
      Start-time file writes became atomic too.
- [x] **KV-backed ingress certificates** (2026-10-10): `tls.certFrom = { kv }`
      on ingress chains. A worker-plane `GetIngressCertificates` RPC derives
      the path from each allocation's chain, validates the bundle, and sends
      it once per version. Nodes hot-swap with fail-static status. Naming a
      path needs `kv:read` on it.
- [x] Example renewer and consumer jobs: `configs/jobs/cert-renewer.ncl`
      (lego, DNS-01, KV as the only state) and `configs/jobs/tls-edge.ncl`.

[plan](plans/egress-credential-injection-plan.md) ·
[calls](calls/egress-credential-injection-plan_calls.md) ·
[docs](../docs/network-handlers.md#credential-injection)

- [x] **Credential injection on egress, Phase 1** (2026-10-12):
      `connect.upstreams[].credentials = [{ header, prefix?, from = { kv } }]`
      on external `https://` upstreams. The node's forwarder replaces the
      task's header with the KV value on every request (retries and `proxy`
      hand-offs included), so the key never enters the workload. No TLS
      interception. A worker-plane `GetUpstreamCredentials` RPC derives the
      path from the allocation's own declaration and sends each value once
      per version; values stay in worker memory. A missing value answers a
      local `503 credential-unavailable` without dialing; deleting the key
      revokes it. Naming a path needs `kv:read`. Gated sandboxes are
      covered through their vsock upstreams; TAP-wired and loopback-only
      sandboxes are not.
- [x] **Credential injection, Phase 2: the identity provider** (2026-10-12):
      `from = { identity = "<task identity>" }` injects a workload-identity
      token the manager mints in `GetUpstreamCredentials` (the token's expiry
      is its version; a new one once half the TTL is left). An identity named
      this way may skip `env`/`file`, and the worker then never sees it; the
      control-plane `legion` identity cannot be injected
      ([calls #6 to #8](calls/egress-credential-injection-plan_calls.md)).

[encryption plan](plans/kv-encryption-plan.md) · [docs](../docs/kv-encryption.md)

- [x] **KV values encrypted at rest, on by default** (2026-09-21): AES-256-GCM
      per value with the key path as AAD, under data keys wrapped by a root
      key that never enters the store; wrapped data keys replicate in the
      `keyring_key` table; a root key is minted at `<storePath>/keys/root.key`
      on first start; lazy migration keeps pre-upgrade plaintext rows readable
      until a rewrap; `state:read` now sees ciphertext, closing the KV path-ACL
      bypass
  - [x] Phase 1 — encrypt on write / decrypt on read, `file`/`env` providers,
        locked-not-dead mode with the exact next step in the error, the
        mixed-version initialization gate (every raft transport reply carries
        the answering manager's schema version), backup reply + CLI reminder,
        restore fail-fast, `legion keyring status` with a server-computed hint
  - [x] Phase 2 — automatic data-key rotation (`dataKeyRotationAge`, 2³¹
        encryption guard), background rewrap that never bumps versions or
        restarts jobs, retired-key GC, `root-key generate|rewrap`,
        `rewrap --plaintext`, `reset` break-glass, `legion_keyring_*` metrics,
        `keyring.*` events, the Manager-page card and the KV-page badge
  - [x] Phase 3 — `exec` root-key provider (JSON on stdin/stdout, never on a
        KV read or write path), `legion keyring provider test` conformance
        harness, reference helpers for AWS KMS and Vault Transit under
        `plugins/keyring/`
  - [x] Phase 4 — a joining manager receives the root key inside the raft join
        reply (sealed under a key derived from the cluster secret), so "join
        the cluster" stays one command

[checkpoint encryption plan](plans/checkpoint-encryption-plan.md) ·
[calls](calls/checkpoint-encryption-plan_calls.md) ·
[docs](../docs/suspend-and-resume.md#published-checkpoint-encryption)

- [x] **Published checkpoints are sealed** (2026-10-11): every part a worker
      publishes (`durability = "store"`, `snapshot create --publish`,
      `--spread` forks) is AES-256-GCM in 1 MiB STREAM chunks under a part
      key the manager derives from the keyring's `ck_…` checkpoint key and
      the part's plaintext digest. Keys ride the publish, warm, and restore
      work (never a pull RPC), are redacted from Debug, and never serialize.
      Objects live at `sealed/<keyed name>`, dedupe and stay idempotent, and
      open in place on fetch. Plaintext leftovers, tampering, wrong keys,
      and missing or locked keys each fail by name. Open follow-ups are under
      the suspend item in Task runtimes.
- [x] **Published checkpoints are chunked and deduplicated**: every part is
      cut into fixed-size chunks (`contentStore.chunkSize`, 1 MiB), each
      distinct non-zero chunk is sealed and uploaded once per job and dedup
      window (`contentStore.dedupWindow`, 24h), and a sealed index per part
      lists them. Zero chunks are never stored, a repeat checkpoint uploads
      only changed chunks, and sandboxes of one job share their image's
      chunks. Chunks seal and open 16 at a time and never land ciphertext
      on disk, which closes the encryption plan's parallel-sealing and
      streaming-decrypt follow-ups. Measured on a QEMU guest (512 MiB
      memory, 605 MiB root disk): 1,117 MiB per checkpoint before, 131 MiB
      after an active minute and 38 MiB after an idle one
      ([plan](plans/chunked-checkpoint-publish-plan.md) ·
      [calls](calls/chunked-checkpoint-publish-plan_calls.md) ·
      [docs](../docs/suspend-and-resume.md#how-published-parts-are-stored)).
      Open follow-ups are under the suspend item in Task runtimes.

## Service discovery (v1)

[plan](plans/service-discovery-final-plan.md) · [docs](../docs/service-discovery.md)

- [x] Declare a named service on a task
- [x] Declare health checks on that task
- [x] Query address and port from other services
- [x] Catalog as a projection (no registration RPC, no drift, only-healthy)
- [x] `legion service list | resolve | doctor`; works identically on Linux and macOS
- [x] Connect to another service by name, load balanced, without writing any
      client code: `connect.upstreams` -> `$LEGION_UPSTREAM_<SVC>`, a node-local
      loopback forwarder, and `round_robin`/`random`/`least_conn` (pluggable —
      one trait impl plus one registry row)
- [x] Declare which other jobs may reach my service (`service.allow`, matched
      against the consuming job id). Enforced on the **connect path only**: the
      manager filters the per-node endpoint feed, so a denied consumer's
      forwarder has nothing to dial. Not cryptographic identity — a task that
      already knows a backend's `address:port` can still dial it directly, and
      that hop is unauthenticated. mTLS below is what closes it.
- [x] `legion service resolve --one`, `service` entries in the capability
      catalog, and `legion_service_list`/`legion_service_resolve` MCP tools.
      Note `service list -j` / `resolve -j` now emit the standard `{ok, data}`
      envelope instead of a bare array (`jq '.[0]'` -> `jq '.data[0]'`, or use
      `--one`, which needs no `jq`).

## Datapath performance

[plan](plans/datapath-performance-plan.md) · [calls](calls/datapath-performance-plan_calls.md)
· docs: [service-discovery.md](../docs/service-discovery.md#balancing-strategies),
[network-handlers.md](../docs/network-handlers.md#connection-pooling),
[testing-and-benchmarks.md](../docs/testing-and-benchmarks.md)

- [x] `least_latency` balancing (P2C over a peak-EWMA latency estimate,
      weighted by load), now the default. L4 learns from connect time and
      time to first reply; HTTP mode from each response head.
- [x] HTTP-mode connection pooling per listener (service endpoints, external
      targets, mesh endpoints, the task behind ingress), with a safe retry
      for stale connections. HTTP mode now counts in-flight requests.
- [x] `splice(2)` for bulk L4 directions on Linux in the forwarder,
      ingress, the gated egress proxy, and the gateway (copy first, splice
      once a read fills the buffer), with a kernel probe and a copy fallback.
- [x] No per-connection copies of the endpoint list or in-flight map.
- [x] `CounterVec::inc_by` is one atomic add. It looped single increments,
      so `legion_net_bytes_total` cost every L4 flow one atomic per byte at
      close (about 0.1 GiB/s and 50 CPU-s/GiB through the forwarder in
      `net_bench`).
- [x] `benchmarks/micro/net_bench.rs` (network-handlers plan §7.12).

## Stateful workloads & volumes

[stateful plan](plans/stateful-workloads-and-instance-identity-plan.md) · [volume plan](plans/volume-plugin-system-plan.md) · [docs](../docs/volumes.md)

- [x] Instance identity: slot and keyed modes, storage handles, fencing
- [x] Volume plugin system: the `VolumePlugin` contract, exec ABI, built-in
      `local-dir`/`host`/`nfs`/`tmpfs`, a reference exec plugin, the conformance
      harness (`legion volume plugin test`), the volume table and reclaim,
      node-local pinning, `legion volume` CLI + MCP tools, UI, and the
      [Postgres](../docs/guides/postgres.md) / [Redis](../docs/guides/redis.md) guides
- [x] Block-mode volumes, consumed directly by the sandbox runtime
- [x] **Idle sessions sleep on their node** (session durability plan Phase
      5a): `suspend.durability = "local"` on session tasks, `keepFor`,
      `checkpointEvery`, `onWorkerStop`, suspend and resume routed by the
      id's node tag, eviction with an explicit `evicted` answer, and no raft
      entry for any of it ([plan](plans/session-durability-and-routing-plan.md)
      §6.6 · [docs](../docs/session-mode.md#idle-sessions) ·
      [calls](calls/session-durability-and-routing-plan_calls.md) #13, #14,
      #17-#21). Store durability for sessions is open above (Phase 5b).
- [x] **Edge boot: provisional allocations, gate levels, and boot tickets** —
      a node reaches a declared execution stage (`stage`/`boot`/`serve`) before
      the placement is durable, and an edge node authorizes a start itself from
      a short-lived manager-signed ticket. Ratification is the existing plan
      apply with the instance named, so the at-most-one-live-allocation
      invariant is unchanged and its suites run unmodified
      ([plan](plans/edge-boot-plan.md) · [docs](../docs/edge-boot.md) ·
      [calls](calls/edge-boot-plan_calls.md)). Phase 0's measurement and
      phase 5's backlog are open above.
- [x] **The ticket spend rides the ratification** (2026-10-12). `boot_ticket`
      moved to the scheduler lane, and a ratification spends its ticket,
      creates the keyed instance when the ticket names an id that never
      existed, and places it in one group-committed entry, so an edge start
      is one entry and a refused ratification leaves the ticket unspent
      ([sandbox-scaling calls](calls/sandbox-scaling-plan_calls.md) #31,
      [docs](../docs/edge-boot.md)).
- [x] **Ratifications stop refusing each other** (edge ratify throughput
      Phase 1, 2026-10-14): [plan](plans/edge-ratify-throughput-plan.md) ·
      [calls](calls/edge-ratify-throughput-plan_calls.md) ·
      [docs](../docs/edge-boot.md#ratification-under-load). The networked
      load test and the retry gap are open above.
  - [x] Ratification load harness, in-process through the real `RatifyClaim`
        handler with real tickets, local and single-voter raft
        (`tests/ratify_throughput_measure.rs`, before/after numbers in the
        plan §8 and [edge-boot.md](../docs/edge-boot.md#ratification-under-load)).
  - [x] Ratifications fill commit batches. They already shared the instance
        group commit (cap 64), not the plan coalescer, so
        `plan_apply_batch_max` never applied. What kept entries from
        filling was the job-config and node epoch fences, which refused all
        but one ratification of a job per entry as `internal`, and the
        handler blocking the async runtime, which held a burst to the
        worker-thread count. A ratification is now fenced by committed
        state inside its entry, and runs off the runtime.
  - [x] A start for a never-existed id decides the create inside the entry,
        so two nodes racing to create one sandbox leave one owner and a
        loser refused `instance_claimed`, not `internal`.
- [x] **Edge starts hold up on a three-manager raft cluster** (2026-10-14,
      [#45](https://github.com/mikenomitch/legion/pull/45)). `race.sh`
      started each of 60, 200, and 300 instance ids on three workers at
      once. Every id had exactly one ratified start, and no loser's process
      survived, including a run with the raft leader stopped 30 s in
      ([results](../benchmarks/aws/sandbox-boot/RESULTS.md#ratification-on-three-managers-2026-10-14) ·
      [docs](../docs/edge-boot.md#ratification-on-a-raft-cluster) ·
      [calls](calls/edge-ratify-throughput-plan_calls.md) #2, #3). It found
      and fixed:
  - [x] The dispatch carries `speculativeStart`
        (`TaskAllocation.task_speculative_start`). Before, every node's spec
        cache read every task as `off` and refused every boot ticket. This
        was on main, and in-process tests missed it because they fill the
        cache by hand ([docs](../docs/edge-boot.md#the-spec-cache)).
  - [x] A follower answers `RatifyClaim` with not-leader instead of an
        `internal` refusal, writing nothing, and the node retries the other
        managers it knows and keeps the one that ratified. Before, most edge
        starts on three managers were killed after boot.
  - [x] A just-ratified claim is kept from the stop sweep until a desired
        set lists it, or for 30 s (`RATIFIED_SETTLE`). Before, a desired set
        from a lagging follower stopped 378 confirmed claims on one worker.
  - [x] An edge start sets its own `LEGION_INSTANCE_ID` and drops the
        identity keys the cached task config carried from another
        allocation.
- [x] **Edge starts shed before boot when ratification is behind** (edge
      admission control Phase 1). Each edge node keeps an AIMD window over
      its unratified edge claims, capped by `edgeBoot.maxProvisionalPerNode`
      (moved to the worker type config and now enforced) and halved when
      verdicts come back later than `edgeBoot.overloadThreshold` of the
      lease. A start that finds it full waits `queueTimeout`, then is
      refused `OVERLOADED` (retryable) before admission, so nothing boots,
      nothing is held, and the ticket stays unspent. In-flight claims are
      never touched, and the window grows back on timely verdicts
      ([plan](plans/edge-admission-control-plan.md) ·
      [docs](../docs/edge-boot.md#overload) ·
      [calls](calls/edge-admission-control-plan_calls.md)). Phases 2 and 3
      are open above, under Scheduling & placement.
  - [x] `edgeBoot.maxProvisionalPerNode` is enforced on every edge node.
  - [x] A `ratifyTimeout` under a second is refused at submit. The lease
        travels in whole seconds, so `"500ms"` reached the node already
        expired and the next lease sweep killed the claim.
  - [x] Backpressure before lease expiry: the typed `OVERLOADED` refusal,
        `legion_worker_claim_window` and
        `legion_worker_claim_overload_refusals_total`, and the failure-mode
        section in edge-boot.md.
- [x] **Sandbox scaling Phases 0–3** (2026-10-12,
      [#27](https://github.com/mikenomitch/legion/pull/27)):
      [plan](plans/sandbox-scaling-plan.md) §14 ·
      [calls](calls/sandbox-scaling-plan_calls.md) ·
      [docs](../docs/sandboxes-at-scale.md). Open phases and gaps are above,
      under Task runtimes.
  - [x] Phase 0: `tests/raft_lifecycle_ops.rs` asserts raft entries per
        lifecycle stage on `single` and `lanes`; `legion-loadgen --mode
        instances` drives create → running → suspend → resume → destroy at
        an open-loop rate (simulated nodes send allocation health and fake
        checkpoints); row sizes at 10^5 and 10^6 and heartbeat bytes at
        1,000 and 10,000 allocations (`tests/instance_scale_measure.rs`).
        Baseline: 22 entries per lifecycle, ~25 lifecycles/s.
  - [x] Phase 0 found and fixed: a keyed instance left `pending` by a full
        cluster was never retried by the capacity pass
        (`tests/capacity_retry.rs`, calls #47).
  - [x] Phase 1: 8 entries per lifecycle. Allocation health is written only
        on change; eval claims are leader memory; a placing eval settles
        inside its plan; stop and destroy queue no eval; concurrent instance
        operations group-commit (`schedulerSettings.instance_group_commit`,
        default on); the docs fix for instance writes being on the scheduler
        lane (`docs/multi-raft.md`). The ≥ 5× rate target was missed (~2×),
        open above.
  - [x] Phase 2: `Manager.StreamWork` delivers placements to nodes one round
        trip after commit, with per-process epochs, per-node sequences,
        acks, replay, and digest-checked resync; a steady heartbeat carries
        a position and digest instead of the allocation set
        ([docs](../docs/architecture-managers-workers.md#delivery)).
        Replaces the "heartbeats rebuild the node's whole desired set every
        beat" item.
  - [x] Phase 3: unplaceable job evals end `blocked` under a key (pool
        class, pinned node, or `escaped`), one per job, woken only by
        matching capacity (registration, node change, readiness, freed
        allocations) with missed-unblock detection; `Job.blocked` and the
        `blocked` job status
        ([docs](../docs/scheduler-and-eval-loop.md#blocked-evals)). Replaces
        the "capacity exhaustion still fails evals terminally" item.
  - [x] A standalone AWS sandbox campaign
        (`benchmarks/aws/scripts/run-sandbox-campaign.sh`,
        `analysis/sandbox_report.py`), not yet run (open above).
- [x] **Sandbox scaling Phases 4–6** (2026-10-12,
      [#30](https://github.com/mikenomitch/legion/pull/30)):
      [plan](plans/sandbox-scaling-plan.md) §16 ·
      [calls](calls/sandbox-scaling-plan_calls.md) ·
      [docs](../docs/session-mode.md). Open gaps are above, under Task
      runtimes.
  - [x] Phase 4: server-minted keyed ids are `<task>-<26 base32>` (130
        bits) and destroy deletes their row; caller ids keep tombstones and
        may not take that shape; the upgrade marks every existing row named
        ([docs](../docs/stateful-workloads.md#id-rules)).
  - [x] Phase 5: `suspend.archive = "slim" | "manifestOnly"` moves a
        published checkpoint's record into the bucket in the publish
        report's entry; `instance start` reads it back; `instance list
        --include-idle` pages the bucket
        ([docs](../docs/suspend-and-resume.md#archiving-idle-instances)).
  - [x] Phase 6: `stateful.mode = "session"`: generated envelope tasks, the
        session router, node session reports and the per-manager session
        index, duplicate convergence, derived-id idempotency,
        `PromoteInstance` / `legion instance promote`, `durable`,
        `complete`, and `asOf` on the instance surfaces, the MCP tool and
        catalog entry. An unnamed session's start and stop write no raft
        entry; a promotion writes one
        ([docs](../docs/session-mode.md)).
- [x] **Suspend and resume no longer wait on heartbeats** (2026-10-13):
      [plan](plans/checkpoints-on-stream-plan.md) ·
      [calls](calls/checkpoints-on-stream-plan_calls.md) ·
      [docs](../docs/suspend-and-resume.md#how-it-works). Checkpoint
      requests are pushed on `StreamWork` when their row commits, and a
      worker reports a finished capture, restore, publish, or warm on an
      early heartbeat. The heartbeat stays the record. Replaces the
      "suspend and resume still wait on heartbeats" item (sandbox-scaling
      plan §14.6). A report path that skips the heartbeat is deferred until
      early beats show up as load (plan §8).

## Persistence

[plan](plans/persistence-plan.md) · [docs](../docs/persistence-streaming.md), [raft-operations.md](../docs/raft-operations.md)

- [x] Streaming SQLite replication, Litestream-style, with optional lease failover
- [x] **The manager's own identity for state replication** (2026-10-12). The
      WAL replica, the takeover restore and the lease share one credential
      path: `persistence.streaming.credentials` keys, or the platform chain
      (AWS env keys, web identity for EKS IRSA/SPIRE/Vault, ECS task role,
      instance metadata), resolved and logged at boot, with half-configured
      credentials and EKS Pod Identity refused. The lease's own keys were
      removed. Legion deliberately does not issue the manager's identity
      itself. Also fixed: the lease store sent no ETag preconditions, so the
      object-storage lease could never be acquired on real S3, R2 or MinIO
      ([plan](plans/manager-workload-identity-plan.md) ·
      [calls](calls/manager-workload-identity-plan_calls.md) ·
      [docs](../docs/persistence-streaming.md#credentials)).
      Deferred: `--restore-from` from the replica, introspection, Pod
      Identity (open under Persistence & cluster operations)
- [x] Raft control plane: leader election, failover controls, membership,
      observability, CLI and UI operations
- [x] Snapshot install no longer loses the writes that follow it (2026-09-29):
      the WAL checkpointer's connection is now closed and reopened with every
      other connection when `install_state_db` swaps the state file. Left open
      on the replaced inode it checkpointed the new file's WAL into the old
      one and truncated it, which reverted a fresh joiner to the seed image
      (`tests/raft_durability.rs` caught it once the KV encryption merge
      shifted the timing)
- [x] `LEGION_DATA_DIR` moves every `.legion/*` default (store, raft, worker
      state, identity tokens) for one process, so a second cluster can run
      from the same checkout. The store default became the data dir itself,
      with the old `.legion/store.sqlite/` layout still opened
      ([plan](plans/data-dir-env-plan.md) ·
      [docs](../docs/data-directory.md))
- [x] The read model follows SQLite in every mode (2026-10-10). A shadow
      check found 95 unit tests where `local_disk` left the cache
      disagreeing with SQLite: registration and mark-lost cached values a
      later op would commit, `complete_allocation` never updated the cached
      status, and `destroy_job` leaked the destroyed config's task configs.
      With the check on in integration tests it also found that a full
      read-model rebuild (restart, snapshot install) dropped every cached
      task's `speculative_start`.
      `local_disk` mutations now project the read model from a session
      changeset like raft replicas do. Every manager also rebuilds after an
      outside commit (`PRAGMA data_version`) and scrubs 128 nodes every 5s,
      counted in `legion_read_model_external_writes_total` and
      `legion_read_model_drift_repaired_total`
      ([plan](plans/read-model-coherence-plan.md) ·
      [docs](../docs/state-store.md#read-model-coherence))

## Workload identity

[plan](plans/workload-identity-oidc-plan.md) · [docs](../docs/workload-identity.md)

- [x] Manager as an OIDC issuer: keys, discovery, JWKS, rotation
- [x] Token issuance RPC, job-spec API, worker delivery, change mode
- [x] Node identity tokens and control-plane identity (phase 7)
- [x] The manager presents a platform-issued identity for its own storage
      access and never signs one for itself; there is deliberately no
      `managerIdentities` block
      ([plan §6](plans/manager-workload-identity-plan.md#6-why-the-manager-does-not-use-legions-own-idp) ·
      [docs](../docs/workload-identity.md#the-managers-own-identity-and-why-legion-does-not-issue-it))
- [x] Identity epochs: a fork or snapshot capture retires the instance's
      earlier tokens in Legion's own verifiers and re-issues the running
      instance at once
      ([calls 137-139](calls/suspend-resume-and-oversubscription-plan_calls.md#137-fork-and-snapshot-captures-retire-the-origins-earlier-tokens-with-a-per-instance-identity-epoch),
      [docs](../docs/workload-identity.md#forks-and-snapshots)). Offline
      verifiers (AWS, GCP, Vault) cannot see the epoch, and login-minted ACL
      tokens are still open (Task runtimes, Phase 6 follow-ups).
- [x] A raft follower with `workloadIdentity` on no longer crashes at
      startup proposing `insert_signing_key`. It starts keyless (issuance
      `UNAVAILABLE`, JWKS 503) and adopts the leader's key within 5 seconds.
      A keyless node that wins leadership generates one, and a new leader
      rotates from the store's key set. Followers also stop logging ERRORs
      for leader-only `ensure_mesh_ca` and `prune_expired_credentials`.
      Found on the AWS spot-check rig, 2026-10-12
      ([docs](../docs/workload-identity.md)).

## Auth & ACLs

[plan](plans/auth-acl-plan.md) · [docs](../docs/auth-and-acls.md)

- [x] Phase 1 — the ACL model: resources, actions, scopes, default-deny, one-line
      enforcement, and the `tests/acl_endpoints.rs` coverage gate
- [x] Phase 2 — interactive SSO and richer admin
- [x] Phase 3 — worker-plane hardening (join tokens from the authenticated
      `CreateJoinToken` RPC; the insecure `:4748` bootstrap listener is gone)
- [x] Phase 4 — operations at scale
- [x] Idempotency keys are scoped to operation and caller, and pruned after
      24 hours (audit S4, [calls #6](calls/architecture-audit-2026-10-09_calls.md),
      [docs](../docs/agent-readiness.md)).
- [x] Worker-plane identity binding: join tokens pin worker type and pool,
      node tokens re-register only their own node, and node ids on worker RPCs
      are bound to the caller (audit S1). Browser origin allow-list (S2),
      private keys redacted from state queries (S3), cluster-wide
      `RotateMeshCa` (S5), stream re-auth (S7). See the
      [audit](architecture-audit-2026-10-09.md) and
      [auth-and-acls.md](../docs/auth-and-acls.md).
- [x] The join token can stay off the worker's command line:
      `--token-file` on `legion join` and `legion start worker`, a warning
      when the token arrives on argv or the file is readable by others, and
      the spot-check rig's unit reads a `0600` file (AWS spot check P1).
      [calls](calls/join-token-file-and-cli-timeouts_calls.md) ·
      [docs](../docs/architecture-managers-workers.md#running-a-worker-under-a-service-manager)

## Exec & actions

[plan](plans/exec-and-actions-plan.md) · [docs](../docs/exec-and-actions.md)

- [x] Worker exec core: `ExecSupport`/`exec()` on the runtime trait, PTY, session
      limits
- [x] Manager proxy, ACLs, audit events, and `legion exec`
- [x] Browser path: WebSocket bridge and an xterm.js terminal in the UI
- [x] Actions: spec schema, `ListJobActions`, server-side render and param
      validation, the `action` ACL resource, `legion action list|run`
- [x] Actions UI, MCP tools, and docs
- [x] The exec WebSocket follows `server.tls`: with TLS on, it serves `wss://`
      with the gRPC listener's certificate and key, so the UI terminal and
      actions work against a TLS manager (they failed the handshake against a
      plaintext listener before). Found while planning the manager reverse
      proxy. [docs](../docs/exec-and-actions.md#how-it-works) ·
      [UI docs](../docs/ui-architecture.md#exec--actions-interactive-terminal)

## Observability

[docs](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [x] Tracing in the scheduler
- [x] Benchmarks in a dedicated directory
      ([docs](../docs/testing-and-benchmarks.md))
- [x] Task log capture, tailing, rotation, and external shipping (including OTLP)
- [x] OTLP trace export for Legion's own spans ([src/otel.rs](../src/otel.rs))
- [x] **Server console logs never block** (2026-10-14,
      [#45](https://github.com/mikenomitch/legion/pull/45)). Managers and
      workers write the console through a lossy non-blocking writer, so a
      slow journald drops lines instead of stalling every thread that logs.
      Found on a c5.metal: a 1,200-start burst blocked heartbeats for about
      57 s, the worker was marked lost, and the burst fell to 8 starts/s.
      After, 293/s and no lost node
      ([docs](../docs/observability.md#logging-and-tracing-setup) ·
      [calls](calls/sandbox-boot-throughput-plan_calls.md) #3 ·
      [results](../benchmarks/aws/sandbox-boot/RESULTS.md#console-logging-under-a-burst-2026-10-14)).
      Dropped lines are not counted yet (open item under Observability).
- [x] Event stream: scheduler events as history and live tail (`ListEvents`,
      `StreamEvents`, CLI, UI)
- [x] **Task start stage timings** (2026-10-13) , 
      `legion_task_start_stage_seconds{runtime,stage}` on the worker
      (`staging`, `volumes`, `prepare`, `start`/`restore`, `register`,
      `total`, `health`, and the sandbox's `network`, `image`, `root_disk`,
      `vmm_config`, `vmm_spawn`), plus one `Task allocation started` log line
      per start with the breakdown, [plan](plans/task-start-critical-path-plan.md) ·
      [calls](calls/task-start-critical-path-plan_calls.md) ·
      [docs](../docs/metrics.md). Not yet run on a KVM node.
- [x] Simulated fleets feed the `legion_worker_*` families (2026-08-02) —
      the sim fleet starts the shared worker telemetry listener (via
      `daemon::start_worker_telemetry`, so the real and simulated paths
      cannot drift on how telemetry is turned on) and reports into the
      process-global worker registry: the existing heartbeat/allocation
      families aggregated over the fleet, plus
      `legion_worker_simulated_nodes`, `..._nodes_target`, and
      `legion_worker_simulated_registrations_total{outcome}`. Per-node
      series deliberately omitted — cardinality at the ~1,250 nodes/host
      the AWS campaign packs, and the manager already holds the per-node
      view. The AWS rig consumes them: `start-sim-fleet.sh` sets
      `LEGION_WORKER_TELEMETRY_PORT=9101` and publishes file_sd targets
      (retracted by `stop-sim-fleet.sh`), Prometheus grows a
      `legion-sim-workers` job, and the campaign board grows a sim-fleet
      row. Like the rest of the rig that plumbing is unexercised until a
      real campaign runs. [docs/metrics.md](../docs/metrics.md) § Worker,
      [docs/worker-simulation.md](../docs/worker-simulation.md) § Metrics,
      [plan](plans/worker-simulation-plan.md),
      [calls](calls/worker-simulation-plan_calls.md).

## Job types & dependencies

[docs](../docs/jobs-tasks-and-scheduling.md)

- [x] Short-lived jobs — the `batch` lifecycle
- [x] Cron jobs — `schedule` on a batch task, taking a cron expression or
      `{ every: <duration> }`
- [x] Task groups co-locate on one node
- [x] DAGs: `depends_on` with `start`/`complete`/`success` conditions, topological
      sort, cycle rejection, cross-group dependencies (so DAGs span machines) and
      group-bound sidecars
- [x] Job status derived from the workload: `GetJobInfo`/`ListJobs` report
      `running`, `pending`, `failed`, `scheduled`, `complete`, `suspended`,
      `idle`, or `stopped` instead of the stored flag, which is now `active`
      (was `done`, which read as "finished" beside a running service)
      ([plan](plans/job-status-plan.md) ·
      [docs](../docs/jobs-tasks-and-scheduling.md#job-status) ·
      calls: [job-status-plan_calls.md](calls/job-status-plan_calls.md))

## Agent surfaces

[docs](../docs/agent-readiness.md)

- [x] A self-documenting CLI for agents (`legion docs`, `legion capabilities`)
- [x] Structured output everywhere (`--json` + the standard envelope)
- [x] gRPC endpoints documented in `proto/legion.proto` as the source of truth
- [x] An MCP server (`legion mcp serve`), with mutating tools gated behind
      `--allow-write`
- [x] Idempotency keys on mutating RPCs; validate/plan/preview paths
- [x] The CLI never hangs on a silent manager: `LEGION_REQUEST_TIMEOUT`
      (30s) on every call but streams and whole-store operations,
      `LEGION_CONNECT_TIMEOUT` (5s), and both failures as retryable envelope
      errors (`DeadlineExceeded`, `Unavailable`) (AWS spot check P2).
      [calls](calls/join-token-file-and-cli-timeouts_calls.md) ·
      [docs](../docs/agent-readiness.md#connecting)

## UI

[docs](../docs/ui-architecture.md)

- [x] The Jobs index and the job page show the same derived job status;
      the page's client-side `running` workaround is gone
      ([suspend-resume-ui calls 9](calls/suspend-resume-ui-plan_calls.md#9-a-done-job-with-live-work-reads-running-derived-in-the-ui),
      [job-status plan](plans/job-status-plan.md))
- [x] Keyboard navigation: ⌘K command palette, `j`/`k`/`o` index selection, `u`
      to go up a level
- [x] Jump hints — hold ⌘ (Ctrl) to label index rows with digits and sidebar
      items with letters, then ⌘1 / ⌘⇧B to jump
      ([plan](plans/ui-jump-hints-plan.md),
      [calls](calls/ui-jump-hints-plan_calls.md)). Rows past the tenth, the
      Overview dashboard, and detail-page tables are open items above.
- [x] Job mtime — the Jobs index "Updated" column showed `created_at`, because
      jobs had no `updated_at` anywhere in the system. Real column, stamped on
      spec submissions and status transitions but not on allocation churn;
      `ListJobs` sorts by it by default and `job list` renders both timestamps
      readably ([plan](plans/job-updated-at-plan.md),
      [calls](calls/job-updated-at-plan_calls.md),
      [docs](../docs/jobs-tasks-and-scheduling.md#job-timestamps-created_at-and-updated_at)).
- [x] Holistic polish (2026-09-02): every index and detail page polls (one
      `usePoll` helper, visibility-paused, last data kept across a transient
      failure); **Stop job**, **Cordon / Drain / Uncordon**, and a **Scale**
      panel in the UI; a lost-contact banner with retry; `?` shortcut sheet,
      recents and actions in ⌘K, per-page tab titles, a favicon, a real
      not-found page; Jobs status chips backed by new `ListJobs.status` +
      `status_counts` fields (also `legion job list --status` and the MCP
      tool); the Workers index shows liveness as the status with health and a
      "Heartbeat" column; route-level code splitting (initial bundle
      1.6 MB → 0.58 MB); a `ui-build` CI job with a generated-client drift
      check ([plan](plans/ui-holistic-polish-plan.md),
      [calls](calls/ui-holistic-polish-plan_calls.md),
      [docs](../docs/ui-architecture.md)). Closed the job-updated-at
      deferrals (created/updated on the job header, per-page last-column
      label) and the stale "gen has drifted" item — the client was already
      current, and CI now keeps it so.
- [x] Task page tabs (Overview · Logs · Exec & actions · Spec, in the URL as
      `?tab=` / `?alloc=`): health tiles, one Instances table replacing the
      three lists of the same allocations, declared checks, recent events;
      one exec panel behind a target picker instead of one per allocation
      ([plan](plans/ui-task-and-template-redesign-plan.md),
      [calls](calls/ui-task-and-template-redesign-plan_calls.md),
      [docs](../docs/ui-architecture.md#task-page)). The deferrals are open
      items above.
- [x] Actions tab on the task page (replaces "Exec & actions"): the task's
      actions as a list with **Open a shell** last; the chosen one as a typed
      form (segmented / chips for enums, bounded ints, patterned and masked
      strings, switches), the argv it renders to, run on one / all running /
      unhealthy instances, and an exit code and output per instance, with the
      equivalent `legion action run` line. `?tab=actions&action=<name>|shell`;
      `?tab=exec` still works
      ([plan §3](plans/ui-task-and-template-redesign-plan.md#3-actions-tab-phase-3),
      [calls](calls/ui-task-and-template-redesign-plan_calls.md),
      [docs](../docs/exec-and-actions.md)).
- [x] Guided template launch: Variables → Review (creates vs updates, changed
      values, rendered-spec diff against the running job) → Launch (rollout
      progress, Copy as CLI); source and versions on their own tab
      ([plan](plans/ui-task-and-template-redesign-plan.md),
      [calls](calls/ui-task-and-template-redesign-plan_calls.md),
      [docs](../docs/ui-architecture.md#job-templates)).
- [x] Template-launched jobs listed no tasks on the job and task pages: the
      UI took the first JSON submission as the spec, which for a template run
      can be `template.variables.json`. It now prefers `job.rendered.json`
      ([calls entry 5](calls/ui-task-and-template-redesign-plan_calls.md)).
- [x] Operator UI gaps (2026-10-12, [#20](https://github.com/mikenomitch/legion/pull/20)):
      tasks and per-task allocations no longer come up empty
      (`GetJobSpecReply.rendered_jobspec`, `LedgerAllocation.task_name`);
      instance page (`/jobs/:job/instances/:id`) and allocation page
      (`/allocations/:id`) with event timelines from new `instance_id` /
      `allocation_id` event filters (`legion events --instance/--allocation`,
      MCP `legion_events`); the allocation page embeds the task page's
      Actions panel for that one allocation; KV values hidden until revealed (`ListKv` returns no values unless
      `include_values`, `GetKv.metadata_only`, `GetKvReply.references` for
      "Used by"); `LEGION_WORKER_PUBLIC_ADDRESS` →
      `ServiceEndpoint.public_address` for working endpoint links, private
      addresses marked; service-graph boxes link to their pages; a rollout
      whose target is no longer the latest config cancels itself instead of
      retiring the newer allocations (the stuck `kv-demo` / undone scale);
      the manager page renders its config instead of JSON, and the KV
      encryption card moved to the KV page
      ([plan](plans/operator-ui-gaps-plan.md),
      [calls](calls/operator-ui-gaps-plan_calls.md),
      docs: [event-stream](../docs/event-stream.md),
      [kv-store](../docs/kv-store.md),
      [service-discovery](../docs/service-discovery.md#public-addresses),
      [rollouts](../docs/rollouts.md#a-newer-version-supersedes-a-running-rollout),
      [ui-architecture](../docs/ui-architecture.md)).

## AWS rig: mitigations in place for the node-deregistration and plugin-build gaps

Both gaps are still open (see "No way to remove a node record" and "Share one
target dir across plugin builds" above); this records what `benchmarks/aws/`
now does about them so nobody re-derives it at 02:00.

- **Node records.** The rig no longer pretends the node table is a fleet
  count. `scripts/env.sh` gained `node_census` / `assert_fleet`: an exact
  assertion — right row count, every row `active` in `node_health` and
  `available` in `node`, every id from this fleet's `sim-<slice>-` prefixes —
  replacing the `registered >= NODES` gate in both `run-step.sh` and
  `start-sim-fleet.sh`, and printing the observed census
  (`total=4000 active=1000 lost=3000 …`) when it refuses.
  `scripts/reset-control-plane.sh` (stop the fleet, then reinstall, which
  wipes) is the only reliable cleanup, and `run-campaign.sh` runs it before
  every campaign, which also closes the `packing-density.sh` ->
  `run-campaign.sh` boundary in `phase1.sh`. Node-name prefixes remain
  reused, deliberately, for the reason stated above.
- **Fleet churn.** A cell now avoids a fleet restart when the running fleet
  already matches (system, scenario, node count) — a saturation sweep's
  points share one fleet for exactly this reason — waits for the eval
  backlog to drain before measuring, and archives the depth it started from
  in the cell's `pre-cell.json` and `pre_cell_eval_backlog` in the fairness
  record, so a contaminated cell is identifiable from the archive alone.
- **Plugin builds.** `build-and-upload.sh` now fingerprints `plugins/` plus
  the rustc version, and when the fingerprint matches the bundle already in
  S3 it builds with `LEGION_SKIP_PLUGIN_BUILD=1` and reuses that bundle
  (`--force-plugins` overrides). That removes the repeated ~10.9 GB /
  6x-470-crate rebuild from every campaign, but it is a cache, not a fix:
  the first build on a fresh box and any change under `plugins/` still pays
  in full, so the shared-target-dir item above stands.

## Pre-launch spot check (2026-10-11)

[findings](launch-spot-check-findings.md) ·
[plan](plans/launch-spot-check-fixes-plan.md) ·
[calls](calls/launch-spot-check-fixes-plan_calls.md) · fixed in
[#17](https://github.com/mikenomitch/legion/pull/17) (open). Per-finding
outcomes are in the plan's §8; what it deferred is open above under
"Follow-ups from the 2026-10-11 spot-check fixes".

- [x] **Workers on other hosts** (P0-1): Phase 1 of the worker plane:
      reachable bind and advertise, mTLS with per-RPC caller classes, CSR
      issuance ([architecture-managers-workers.md](../docs/architecture-managers-workers.md),
      [auth-and-acls.md](../docs/auth-and-acls.md)).
- [x] **Task lifecycle** (P0-2, P0-3, P1-1, P1-4, P1-9, P2-7, P2-8): batch
      tasks finish and restart policies reach the worker; health checks start
      for staged tasks; resource enforcement turns on; gated starts create
      their namespace; failure reasons carry the task's stderr; the plugin
      dir defaults next to the binary; exec sessions get the task env
      ([task-supervision.md](../docs/task-supervision.md),
      [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md)).
- [x] **Scheduler** (P0-4, P1-2, P1-3, P1-8): DAG dependencies wait for
      reported status; static ports are planned, plans place what fits and
      freed capacity retries the rest; reschedules fall back to the failed
      node; job status is derived from live state
      ([scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md),
      [rollouts.md](../docs/rollouts.md)).
- [x] **Manager availability** (P0-5, P0-6, P0-8, P1-5 in part, P2-5, P2-10,
      `transfer-leader`): forwarded heartbeats in `single`, worker failover
      across managers, an availability reconcile, re-registration with the
      node's own token, follower write forwarding (also fixes
      `WorkerRegister` on a follower), remote-election leadership moves, a
      quiet restart for raft members
      ([raft-operations.md](../docs/raft-operations.md),
      [node-liveness.md](../docs/node-liveness.md)).
- [x] **Calls that dial a manager by address work after a failover**
      (found on the AWS run, 2026-10-12). Only the join token's manager had a
      recorded CA pin, so after a leader failover every artifact pull that
      needed a KV credential failed with `no cluster CA pin for the manager
      at ...`. Trust is now shared per cluster: every manager address the
      cluster reports maps to the token's verifier, and the JWKS is fetched
      from the active manager (calls 53,
      [architecture-managers-workers.md](../docs/architecture-managers-workers.md#managers-a-worker-can-reach)).
- [x] **Linux containers** (P0-7, P1-6, P2-6, P2-9, P2-11): Docker's default
      capabilities less `NET_RAW` with `security.capabilities` bounded by
      `container.allowCaps`, ownership kept on extraction, reopenable stdio,
      `ulimits`, the macOS 200 MiB floor, a per-blob image cache, a Linux
      install guide with the guest kernel source and AWS nested
      virtualization ([task-runtime-container.md](../docs/task-runtime-container.md),
      [installation.md](../docs/installation.md)). `redis.ncl` runs through
      `sh -c` with TCP checks and the docs use `--param`.
- [x] **CLI and diagnostics** (P1-7, P1-9, P2-1 to P2-4, P2-6, P2-7):
      allocation ids survive lane snapshots, removed allocations' logs stay
      readable, the quickstart works outside a checkout, precondition errors
      keep their cause, `kv get` misses exit non-zero, `--json` on every
      command and camelCase everywhere ([logging.md](../docs/logging.md),
      [agent-readiness.md](../docs/agent-readiness.md)).
- [x] **The Managers page lists every raft member** (P2-12, found on the
      second AWS pass): the page read the config table, which only the
      control-lane leader writes, so a three-voter cluster showed one
      manager. New `ListManagers` RPC (`legion raft members`, MCP
      `legion_manager_list`) reads the replicated membership and probes each
      member live, so any manager, leader or follower, lists them all with
      role, address, reachability, term, and applied index. Config rows are
      now keyed by the raft node id instead of the shared `0.0.0.0:4747`
      listener address ([raft-operations.md](../docs/raft-operations.md#members),
      [ui-architecture.md](../docs/ui-architecture.md);
      [calls](calls/managers-page-raft-members_calls.md)).
- [x] **Small fixes from the earlier read-through**: `tls-edge.ncl` listens
      on its advertised port on Linux, and `internal_docs/development.md`
      points workers at :4747.
- [x] **Memory resumes on cloud-hypervisor always booted cold** (found on
      the AWS sandbox run, 2026-10-12): the node attribute read `51.2` from
      the `--version` banner and the checkpoint `51.2.0` from `vmm.ping`, and
      compat compared strings. VMM versions are now recorded and compared as
      `MAJOR.MINOR.PATCH`, and a local checkpoint its node refuses settles
      within a sweep instead of waiting out `resumeTimeout`
      ([suspend-and-resume.md](../docs/suspend-and-resume.md#where-a-restore-can-run)).

## AWS spot-check re-run fixes (2026-10-12)

Bug fixes, no plan. Uncommitted at the time of writing.

- [x] **A job-level volume claim killed the leader's raft core.** The
      work-stream projection looked up `allocation.job_id`, a column that
      does not exist, inside the state machine's apply once any worker
      streamed. openraft treats an apply error as fatal, so the leader's
      `RaftCore` stopped and the cluster went leaderless until a restart.
      Fixed the lookup (through `job_config`), and made every post-commit
      projection infallible: a failure now rebuilds the read model and
      resyncs every work stream instead of failing the apply. Snapshot
      installs rebuild the same state and now resync work streams too
      ([architecture-managers-workers.md](../docs/architecture-managers-workers.md)).
      Volume UPDATEs (mount reports) now reach the work stream as well.
- [x] **A task that fails on every start was re-placed every 30s forever.**
      The manager's reschedule delay now doubles per consecutive failure,
      capped at 30m
      ([jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md#manager-side-move-logic)).
- [x] **The reschedule backoff was invisible, and other evals ignored it.**
      On the rig a keyed instance sat `rescheduling` with nothing saying
      why, while the capacity pass re-placed it about once a minute despite
      `delay_secs: 1800`. The failure's removal op now records a replicated
      reschedule hold (`reschedule_hold`, also the move history, so every
      manager backs off the same), the planner leaves held replacements
      unplaced in every `Reschedule` plan, and the replacement is a
      replicated delayed eval instead of a timer in one manager's memory.
      Operators get an `allocation.reschedule_deferred` event per deferral
      and `allocation.reschedule_exhausted` when the budget runs out,
      a `rescheduling:` detail on `instance get/list`, the waiting
      replacements on `job info` (`InstanceInfo.reschedule_backoff`,
      `Job.reschedule_backoffs`), and `instance start` lifts one keyed
      instance's hold (`--force-reschedule` lifts the job's)
      ([jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md#manager-side-move-logic),
      [event-stream.md](../docs/event-stream.md),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#55-the-reschedule-backoff-is-a-replicated-hold-the-planner-honors-not-a-timer)).
      [calls](calls/launch-spot-check-fixes-plan_calls.md#54-the-reschedule-backoff-is-a-replicated-hold-the-planner-honors-not-a-timer)).
- [x] **A failed slot instance could never go back to its node (rig
      P0, P1-3's last case).** `fullstack-db-0` (local-dir, `size = "2G"`)
      failed on sc-wkr-1 and every rollout tick said "healthy but has no
      room" on a node 12% reserved: the disk-fit check charged the volume's
      `size` again on the node that already holds it, and that node's free
      disk read under 2 GiB. An instance returning to its volume's node is
      no longer charged its storage. The pinned-node message also replaces
      the generic "no available nodes … preemption did not run" when the
      pinned node is not a candidate at all, and reads the scheduler's own
      view of the node. `instance start` now retries a `rescheduling`
      slot instance too. Covered for the reschedule eval (after the
      backoff, other node free or full), `--force-reschedule`, `instance
      start`, and rollouts (failure before or during the rollout)
      ([volumes.md](../docs/volumes.md),
      [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md#manager-side-move-logic),
      [calls](calls/launch-spot-check-fixes-plan_calls.md) 57 to 59).
- [x] **Two seeded saved queries** (`agent:recent-failed-evals`,
      `agent:pending-evals`) named eval columns that do not exist (`kind`,
      `node_name`). Fixed, with a test that runs every seeded query.
- [x] **One allocation was launched twice, and the two launches deleted
      each other's rootfs.** The heartbeat's reconcile and the work stream
      (or a push) each decide to launch under their own lock, with identity
      delivery awaited in between. A second dispatch of an allocation still
      staging artifacts tore the staging down and restaged it, the first
      staging thread launched anyway, and the cancelled launch's teardown
      removed `allocs/<id>` under the live one. Seen as `python:3.12-alpine`
      failing with "No such file or directory while canonicalizing
      .../zoneinfo/Asia/Hong_Kong". A duplicate of the same payload while
      staging is now ignored, and a staging thread starts its allocation
      only while its generation is still the current one.
- [x] **Layer extraction followed host symlinks and refused cross-layer
      replacements.** It now resolves paths inside the rootfs (an image's
      `lib -> /usr/lib` works, and a whiteout under an absolute symlink no
      longer deletes the host file it names), lets a hard link replace a
      lower layer's file, and lets a file replace a directory
      ([task-runtime-container.md](../docs/task-runtime-container.md)).
- [x] **One `~/.legion/token` served every cluster.** A second local manager
      left the file alone and the CLI presented the other cluster's token.
      Stored tokens are now keyed by manager address
      (`~/.legion/tokens/<host>_<port>`) for `legion login`, `logout`, the
      CLI lookup and the manager's first-start write, which replaces a stale
      file for its own address. An `Unauthenticated` hint names the file and
      address the CLI used
      ([auth-and-acls.md](../docs/auth-and-acls.md#where-the-initial-token-goes),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#51-stored-cli-tokens-are-keyed-by-manager-address-and-a-first-start-replaces-the-file-for-its-address)).
- [x] **The CLI panicked on a closed pipe.** One-shot commands restore the
      default SIGPIPE disposition and end with status 141 like `cat`
      (`tests/cli_closed_pipe.rs`,
      [calls](calls/launch-spot-check-fixes-plan_calls.md#58-a-one-shot-cli-command-dies-by-sigpipe-status-141-on-a-closed-pipe)).
- [x] **`state query` user errors were `Internal`, with an ANSI tracing
      line.** SQL that does not compile or names a missing column or table
      is `InvalidArgument` with SQLite's message. CLI handlers no longer
      carry `#[instrument(err)]`, which printed every command error a
      second time on stdout
      ([codebase-conventions.md](../docs/codebase-conventions.md#tracing-and-diagnostics),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#59-cli-handlers-drop-instrumenterr-across-srccmd-not-only-on-state-query)).
- [x] **Server errors named proto fields.** `net policy test`, session task
      selection, `job scale` and `state query` now name the CLI flag
      (`--task`, `--group`, `--allow-write`) with the API field in
      parentheses.
- [x] **Node attrs were JSON strings in the MCP tools.** `legion_node_list`,
      `legion_node_claims` and `legion_node_drain` return `configAttrs`,
      `fingerprintAttrs`, `dynamicAttrs` and `healthChecks` as objects, like
      `legion node list --json`
      ([calls](calls/launch-spot-check-fixes-plan_calls.md#62-mcp-decodes-node-attributes-in-the-tool-layer-the-proto-keeps-json-text)).
- [x] **A worker restart's effect on mesh connections was undocumented.**
      [upgrades.md](../docs/upgrades.md#restarting-a-worker-in-place) and
      [architecture-managers-workers.md](../docs/architecture-managers-workers.md#running-a-worker-under-a-service-manager)
      now say re-adopted tasks keep running but lose in-flight datapath
      connections, so apps should reconnect. Keeping the datapath alive
      stays open.
- [x] **The macOS spot-check config said "no ACLs".** The comment and
      RUNBOOK section 10 now say where the token is.
- [ ] **A count=1 job got repeated second placements after a KV env-change
      restart** (kv-demo: 1273, 1274 failing on sc-wkr-2 every ~90s while
      1272 served on sc-wkr-1). Not root-caused from the code. A reschedule
      plan counts only allocations of its own `job_config_id`, so check
      whether the failing allocations ran a different config than 1272 and
      whether `job_config_unwanted_reason` let a superseded config through.
- [ ] **Layer extraction applies directory modes as it goes.** A layer
      whose directory is read-only (`0555`) and has entries after it fails
      for a worker that is not root. Root bypasses the mode, so the Linux
      worker is unaffected. Deferring directory modes to the end of a layer,
      as `tar::Archive::unpack` does, would fix it.
- [x] **`job plan` errored where `job run` would preempt.** The victims were
      younger than `victimExemptAge`, so the plan failed "no available
      nodes", and the submit blocked until unrelated churn woke it. The plan
      now shows the victims with how long the submit waits
      (`PlannedPreemption.eligible_in_seconds`), and the submit's eval
      retries when they age past the guard instead of waiting for a capacity
      event
      ([preemption-and-eviction.md](../docs/preemption-and-eviction.md#previewing-before-you-displace-anyone),
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#what-blocks),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#64-job-plan-previews-a-preemption-that-only-waits-on-victimexemptage-and-the-submit-retries-on-the-clock)).
- [x] **Instance JSON showed `nodeId: null`** for every active row-backed
      instance. `nodeId` is now the current allocation's node, and
      `instance list` has a NODE column
      ([stateful-workloads.md](../docs/stateful-workloads.md),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#66-an-instances-node-is-read-from-its-current-allocation-not-stored-on-the-row)).
- [x] **Stopping the macOS manager left its Apple containers running.** The
      in-process worker of `--run-worker` on macOS now stops every task on
      SIGTERM or SIGINT, and the next start runs them again. On every
      platform the in-process worker no longer exits the process under the
      manager's drain
      ([task-runtime-container.md](../docs/task-runtime-container.md#stopping-a-single-process-cluster-macos),
      [calls](calls/launch-spot-check-fixes-plan_calls.md#65-the-macos-in-process-worker-stops-its-tasks-on-a-clean-shutdown-and-the-next-start-runs-them-again)).
      *Unit-tested; not yet re-run on a Mac.*

## Architecture audit (2026-10-09)

[audit](architecture-audit-2026-10-09.md) · [calls](calls/architecture-audit-2026-10-09_calls.md)

- [x] Worker: an empty desired set stops the last allocation (W1), re-adoption
      checks process start time (W2), launches run outside the task-manager
      lock (W3, partial), stopping allocations survive a restart (W4), the hard
      kill hits the process group (W5), and a boot fence covers restarts (W6).
- [x] Raft and store: a new leader catches up before capturing (R1), schema
      mismatches are refused instead of silently skipped (R2), members fetch
      the KV root key (R3), streaming standbys are fenced and restore on
      takeover (R4), snapshot builds don't block writes (R5, partial).
- [x] Scheduler: rollbacks survive failover (SC1), the claim sweep stops
      deleting unrelated evals (SC2), failed reconciles retry (SC3, SC6), a
      capacity pass runs on registration and uncordon (SC4, partial), and
      stale plans place nothing (SC5).
- [x] Manager lifecycle: bounded template evaluation (M1), exit on gRPC
      failure (M2), graceful SIGTERM (M3), a KV reconciliation sweep (M4), a
      proto breaking-change check in CI (M5), plugin build fingerprints (M6,
      partial).

## Sandbox spot check (2026-10-12): pressure, preemption, volumes

Found on the live AWS rig (3 raft managers, 2 workers). Bug fixes, so no plan.

- [x] **`node.pressure` events were never journaled.** A follower leads no
      lane in `single` topology, so standalone events it emitted while
      handling a heartbeat were dropped. A manager that leads no lane now
      forwards them (`ForwardableLaneWrite::EmitEvent`;
      [event-stream.md](../docs/event-stream.md),
      [calls](calls/durability-and-ops-execution-plan_calls.md) entry 15).
- [x] **A bursting hog was re-placed and evicted every 1-2 minutes.**
      `evict-bursting` now spends the task's `on_unhealthy` move budget
      (`move_delay`, `move_count`), like a failed allocation; `evict-reserved`
      still replaces at once
      ([resource-enforcement.md](../docs/resource-enforcement.md#memory-pressure)).
- [x] **Preemption took a `normal` victim while `low` work would do.** The
      `victimExemptAge` guard no longer pushes a placement up the band ladder
      ([preemption-and-eviction.md](../docs/preemption-and-eviction.md#which-of-them-are-taken)).
- [x] **`legion volume list` showed destroyed tombstones by default.** Hidden
      now; `--all` (`include_destroyed`) or `--state destroyed` shows them
      ([volumes.md](../docs/volumes.md#lifecycle)).
- [ ] **Pressure evictions still loop forever by default.** `move_count` is
      unlimited, so the move delay only slows the loop; the exponential move
      backoff (`move_backoff` in `src/manager/server.rs`) widens it once it
      lands. The scheduler also has no memory of pressure, so the replacement
      can land on the node that evicted it.
- [ ] **The preemption floor counts only loaded nodes.** `load_candidates`
      in `src/manager/scheduler.rs` builds `task_running` from the candidate
      nodes' rows, not the fleet, so `disruption.minAvailable` can bind
      tighter than documented when a task runs on nodes outside the pass.

## Swappable seams (2026-10-14)

[plan](plans/swappable-seams-plan.md) · [calls](calls/swappable-seams-plan_calls.md) · docs: [codebase-conventions.md](../docs/codebase-conventions.md#datapath-conventions), [network-handlers.md](../docs/network-handlers.md)

- [x] **One attachment type for the datapath.** `netd::attach::Attachment`
      (host, gated namespace, gated sandbox over vsock, TAP guest) is decided
      once per allocation and carried by every listener spec; "fails closed"
      comes from it, not from which socket path was set.
- [x] **One listener and one dialer.** `netd::listen` (`ListenAddr`,
      `spawn_accept_loop`, `dial_tcp`) replaces four copies of accept-loop
      plumbing and five hard-coded dials.
- [x] **A typed worker ↔ datapath boundary.** `netd::Netd` with
      `NetdInput`, `NetdWants`, and `NetdReport`; spec building, status,
      namespace capture, persistence and restore moved out of `daemon.rs`.
      The process split itself stays open (Networking, above).
- [x] **DNS on `hickory-proto`**, with truncated answers refetched over TCP
      for TCP clients and `SERVFAIL` instead of a silent `1.1.1.1` fallback
      when `resolv.conf` names no nameserver.
- [x] **The egress proxy's HTTP on hyper.** Every request on a connection is
      judged; a request pipelined behind an allowed one used to be spliced
      through unchecked.
- [x] **Mesh upstreams work.** The worker dropped the endpoint feed's
      `mesh_tls` flag, so connect upstreams dialed mesh-TLS endpoints in
      plaintext and were refused at the handshake (since mesh M7).
- [x] **One object-storage layer.** `src/storage/objects/` parses every
      store URL, resolves every principal through one chain, and opens every
      store as `Arc<dyn ObjectStore>`; the `contentStore` publisher, session
      manifests, the `s3://` fetcher, the manager's archive, and streaming
      persistence use it instead of three S3 builders. It fixed three bugs
      on the way: restore reads signed in a different region than the
      publish, the archive matched a manifest to a sibling prefix's
      credential, and a KV credential could be signed with the node's own
      `AWS_SESSION_TOKEN` ([calls](calls/swappable-seams-plan_calls.md)
      #3, #6–#8, #10–#12, #15–#17).
- [x] **Pluggable cluster CA providers.** `clusterCa.provider` = `builtin`
      (the default) | `file` (an operator intermediate on disk) | `exec` (an
      external signer, with a Vault PKI helper in `plugins/cluster_ca/`);
      Legion-chosen names verified on every non-builtin certificate; manager
      certificates from CSRs; heartbeat bundle pins; a leader-recorded
      agreement check; `legion net ca provider test`.
      [plan](plans/cluster-ca-provider-plan.md) ·
      [calls](calls/cluster-ca-provider-plan_calls.md) ·
      [docs](../docs/cluster-ca.md)
- [x] **VIP blocks follow the node's OS.** Placement asked the manager's own
      OS, so a macOS manager gave its Linux nodes no VIP blocks.
