# Lifecycle track report (saved by coordinator from the agent's hand-back)

All seven findings fixed with regression tests. Tests: targeted lib tests (worker::task_manager, task_supervisor, task_health, util, worker::edge, manager::dispatcher, task_plugins::exec, worker::resources, plugin_runtime) 160 passed / 0 failed / 2 ignored (root + cgroup v2); tests/artifact_staging_integration.rs 12/12 incl. new e2e.

Manager-visible batch semantics: success -> TerminatedAllocation{success:true} once -> Store::complete_allocation(id,true): allocation "complete", task status "success", allocation.terminated event; failure -> worker restarts within budget (onFailure), then failed_allocations -> row removed, allocation.failed, reschedule after move_delay; when move budget spent, job failed and task status "failed". complete_allocation(id,false) effectively dead (worker never sends success:false).

P0-2: TaskAllocation.task_lifecycle=26, task_restart=27 filled in dispatcher build_task_allocation_with; task_from_allocation parses. TaskRestartConfig.policy now Option (omitted = lifecycle default) -- behavior change: batch task with restart block w/o policy gets onFailure not always.
P0-3: staging threads (spawn_staging_thread, spawn_provisional_staging_thread, edge provisional-launch) capture+enter runtime handle; spawn_health_monitor refuses w/o runtime with a status message; edge::apply_verdict now starts health monitor after ratification (was dropped).
P1-1: NodeResourceGovernor::from_settings; task manager starts disabled; daemon calls configure_resource_enforcement before restore/start; log from governor. Not verified on real cgroup v2 host.
P1-4: root cause: PostWork-pushed allocations never ran apply_net_capture (heartbeat-only). task_plugins::ensure_gated_namespace creates when missing; netns::create serialized by lock; record_failure releases failed gated alloc's netns.
P1-9a: TaskExit.stderr_tail (last 3 non-empty stderr lines, <=400 chars, current run) in failure reasons/events; health checks stand aside while process down; health failure reason appends recent stderr.
P2-7a: plugin root: <exe>/plugins, <exe>/../lib/legion/plugins, build checkout only if exists; validate_manifest no longer logs at ERROR.
P2-8: exec sessions layer allocation_env like start does.

Files: proto/legion.proto, src/manager/dispatcher.rs, src/structs/job.rs, src/worker/{task_manager,task_supervisor,task_health,edge,resources}.rs, src/worker/daemon.rs (surgical), src/worker/netd/netns.rs, src/task_plugins/{mod,exec,container}.rs, src/util.rs, src/plugin_runtime.rs, tests/artifact_staging_integration.rs; docs: jobs-tasks-and-scheduling, task-supervision, exec-and-actions, plugin-system, network-handlers, resource-enforcement.

Deferred / found (TODO candidates):
1. Pushed gated task's datapath listeners bound only at next heartbeat (loopback-only until then). Documented in network-handlers.md.
2. legion exec into a gated exec task runs in host netns (ExecOptions has no namespace field).
3. macOS: image pull happens inside the task process; explicit `container image pull` in prepare would give first-class failure.
4. Up to one supervisor tick (1s) window where a health check failure can still count after process death.
5. stderr tail copies task output into events (bounded, failure-only) -- secret exposure risk documented.
6. Terminal reports not persisted: worker restart between batch exit and the reporting heartbeat -> exit unknown -> treated as failure -> completed batch task re-runs.
7. TerminatedAllocation.success=false never sent; route never-policy failures through it or drop the field.
8. cgroup enforcement not verified on real hardware.
