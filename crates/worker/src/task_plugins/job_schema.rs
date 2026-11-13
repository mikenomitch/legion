//! The jobspec's JSON Schema (`legion job schema`, `GetJobSchema`, the
//! `legion_job_schema` MCP tool): a documented top-level envelope plus the
//! live, per-runtime task config schemas derived from the built-in plugins.
//! A manager adds the keys installed apps declare at their extension points
//! (`crate::structs::extensions::add_keys_to_schema`).

use super::builtin_task_plugin_schemas;
use serde_json::{json, Value};

/// Assemble the jobspec JSON Schema: a documented top-level envelope plus the
/// live, per-runtime task config schemas derived from the built-in plugins.
pub fn core_jobspec_schema() -> Value {
    let mut task_runtimes = serde_json::Map::new();
    for plugin in builtin_task_plugin_schemas() {
        task_runtimes.insert(plugin.name, plugin.schema);
    }

    let mut schema = jobspec_schema_envelope();
    // `autoscale`: generated from the type the parser uses, with its
    // definitions moved beside the task's.
    let mut autoscale = crate::autoscaler::AutoscaleSpec::json_schema_value();
    if let Some(definitions) = autoscale
        .as_object_mut()
        .and_then(|object| object.remove("definitions"))
    {
        if let (Some(defs), Value::Object(definitions)) =
            (schema["$defs"].as_object_mut(), definitions)
        {
            for (name, definition) in definitions {
                defs.insert(name, definition);
            }
        }
    }
    if let Some(object) = autoscale.as_object_mut() {
        object.remove("$schema");
        object.remove("title");
        object.insert(
            "description".into(),
            json!(
                "Task autoscaling (docs/autoscaling.md): `true` for the effective defaults \
                   (min = count, max = 4 x count, CPU at 70% of the reservation), or a record \
                   overriding some of them. Not on keyed, `count.per`, or batch tasks; at most \
                   one task per group."
            ),
        );
    }
    let text = serde_json::to_string(&autoscale)
        .unwrap_or_default()
        .replace("#/definitions/", "#/$defs/");
    schema["$defs"]["task"]["properties"]["autoscale"] =
        serde_json::from_str(&text).unwrap_or(Value::Null);
    // Built apart: inline, the envelope's `json!` exceeds the macro
    // recursion limit.
    schema["$defs"]["task"]["properties"]["gang"] = gang_schema();
    if let Some(gang_block) = schema["$defs"]["task"]["properties"]["gang"]["oneOf"]
        .as_array_mut()
        .and_then(|forms| forms.get_mut(1))
    {
        gang_block["properties"]["topology"] = gang_topology_schema();
    }
    schema["taskRuntimes"] = Value::Object(task_runtimes);
    schema
}

/// A task's `gang` block (docs/gang-scheduling.md).
fn gang_schema() -> Value {
    json!({
        "description": "All-or-nothing placement for this task's replicas. `true`: every replica places in one plan or none does, and a gang that does not fit places nothing (nor do the job's other tasks in that pass), holds no capacity, and waits blocked until a node that could help has room. `{ min: N }` places once N replicas fit together and adds the rest as room appears; `placementTimeout` bounds the wait, `readyTimeout` bounds how long a placed member may take to come up, `onMemberLoss` says what a lost member does to the rest and `onLocalRestart` whether a member's in-place restart is a loss; `topology` keeps every member inside one domain of a node attribute (one rack, else one zone). Not combinable with count.per or keyed stateful tasks; the task must have its group to itself, and that group's rollout must replace it in one step. See docs/gang-scheduling.md.",
        "oneOf": [
            { "type": "boolean" },
            {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "min": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Quorum: replicas that must place together, 1..=count. Default: the whole count."
                    },
                    "placementTimeout": {
                        "type": ["string", "integer"],
                        "description": "How long the gang may wait for its quorum before its job fails (any members already running stop), as a duration (`\"30m\"`) or seconds. Counted from the first time the gang came up short. At least 1s. Default: unset, wait indefinitely."
                    },
                    "readyTimeout": {
                        "type": ["string", "integer"],
                        "description": "How long each placed member may take to report healthy once (running, and passing its checks if it has any) before it counts as a lost member and `onMemberLoss` applies, as a duration or seconds. Spends the move budget like a failure. At least 1s. Default: unset, no bound."
                    },
                    "onMemberLoss": {
                        "type": "string",
                        "enum": ["restart", "restartInPlace", "replace", "fail"],
                        "description": "What a lost member does to the rest when it leaves fewer than the quorum running. `restart` (default): stop the survivors and place the whole gang again once their stop grace has passed. `restartInPlace`: keep the survivors on their nodes, restart their processes there under a new LEGION_GANG_EPOCH, and place the missing members together once they fit. `replace`: keep the survivors and place the missing members together once they fit. `fail`: fail the job and stop every member. A loss that leaves the quorum running is replaced alone. Restarts after a failure spend on_unhealthy's move budget; once it is spent the job fails."
                    },
                    "onLocalRestart": {
                        "type": "string",
                        "enum": ["loss", "ignore"],
                        "description": "A member its worker restarts in place under the task's restart policy. `loss` (default): it counts as a lost member and `onMemberLoss` applies. `ignore`: it rejoins on its own and the rest keep running."
                    }
                }
            }
        ]
    })
}

/// `gang.topology` (docs/gang-scheduling.md, "Topology").
fn gang_topology_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Place every member within one value (domain) of a node attribute, such as one rack or one NVLink domain. Set `key` for one level or `keys` for several, innermost first (one rack, else one zone). Nodes without an attribute are in no domain of that level. The planner tries whole domains, best fit first, skipping any that cannot hold the quorum even by preempting, at most sixteen per plan; `job plan` reports the level and domain chosen or the closest one. See docs/gang-scheduling.md, \"Topology\".",
        "properties": {
            "key": {
                "type": "string",
                "minLength": 1,
                "description": "The node attribute name, looked up like count.per and distinct (config, then fingerprint, then dynamic attributes). One level; exclusive with `keys`."
            },
            "keys": {
                "type": "array",
                "minItems": 1,
                "maxItems": 4,
                "uniqueItems": true,
                "items": { "type": "string", "minLength": 1 },
                "description": "Several levels, innermost first: `[\"rack\", \"zone\"]` places in one rack when one fits, else in one zone. Exclusive with `key`."
            },
            "mode": {
                "type": "string",
                "enum": ["required", "preferred"],
                "description": "`required` (default): every member in one domain of some level, or the gang waits holding nothing; a top-up stays in its domain. `preferred`: one domain when one fits, else the fewest outermost domains that hold the quorum, else any eligible nodes."
            }
        }
    })
}

fn jobspec_schema_envelope() -> Value {
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Legion Jobspec",
        "description": "A Legion job specification (the JSON a Nickel jobspec renders to). \
            A job has a name and one or more tasks (use `task` for a single task or `tasks` for many). \
            Each task's `config` object is validated against the schema for its `runtime` under `taskRuntimes`.",
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string", "description": "Unique job name." },
            "task": { "$ref": "#/$defs/task", "description": "A single task (shorthand for tasks: [task])." },
            "tasks": { "type": "array", "items": { "$ref": "#/$defs/task" }, "description": "List of tasks." },
            "version": { "type": "integer", "description": "Optional jobspec version." },
            "priority": {
                "type": "string",
                "enum": ["critical", "high", "elevated", "normal", "low", "background"],
                "default": "normal",
                "description": "Priority band, most important first. Orders the eval queue AND governs preemption: an allocation is only ever displaced by a job in a STRICTLY higher band, never a peer. Choosing a band below `normal` is the consent to be displaced. Claiming a band may require an ACL `bands` grant. See docs/preemption-and-eviction.md."
            },
            "rollout": { "$ref": "#/$defs/rollout", "description": "Default rollout strategy for the job." },
            "rolloutByGroup": {
                "type": "object",
                "additionalProperties": { "$ref": "#/$defs/rollout" },
                "description": "Per-group rollout overrides keyed by group name."
            }
        },
        "$defs": {
            "task": {
                "type": "object",
                "required": ["runtime"],
                "properties": {
                    "name": { "type": "string", "description": "Task name (unique within the job)." },
                    "runtime": { "type": "string", "description": "Task runtime plugin that runs this task (e.g. exec, container, noop)." },
                    "group": { "type": "string", "description": "Task group; tasks in a group are placed together." },
                    "lifecycle": { "type": "string", "enum": ["service", "batch", "sidecar"], "description": "Task lifecycle." },
                    "count": {
                        "description": "Number of instances, or {num, per} for spread placement.",
                        "oneOf": [
                            { "type": "integer", "minimum": 0 },
                            { "type": "object", "required": ["num", "per"], "properties": {
                                "num": { "type": "integer" },
                                "per": { "type": "string" }
                            }}
                        ]
                    },
                    "config": { "type": "object", "description": "Runtime-specific config; validated against taskRuntimes[runtime]." },
                    "dependsOn": {
                        "type": "array",
                        "items": { "type": "object", "required": ["task"], "properties": {
                            "task": { "type": "string" },
                            "condition": { "type": "string", "enum": ["running", "healthy", "complete", "success"] }
                        }},
                        "description": "Task dependencies that gate start."
                    },
                    "schedule": { "description": "Batch-only schedule (cron string or {every: duration})." },
                    "stop": {
                        "type": "object",
                        "description": "How the platform terminates this task, for every cause (job stop, rollout replacement, drain, reschedule, scale-in, preemption). See docs/graceful-stop.md.",
                        "properties": {
                            "signal": {
                                "type": "string",
                                "enum": ["SIGTERM", "SIGINT", "SIGQUIT", "SIGHUP", "SIGUSR1", "SIGUSR2"],
                                "default": "SIGTERM",
                                "description": "Notice signal, sent to the task's process group. SIGKILL is deliberately not offered: it is the end of the ladder, not a notice."
                            },
                            "killAfter": {
                                "type": "string",
                                "default": "30s",
                                "description": "Grace between the signal and SIGKILL, e.g. \"30s\". Capped at 5m — grace may slow a drain, never wedge one."
                            }
                        }
                    },
                    "children": {
                        "type": "object",
                        "description": "Lets a unit of this task own child sessions of sibling session tasks of the same job (docs/child-sessions.md): a child is started under its owner (`legion instance create <job> --task <child> --owner <id>`, or `legion session start <child> --owner self` inside the task), ends when its owner is stopped, destroyed, or exits, is charged to its own task's envelope, and is listed under its owner. Any task except a child task may declare it (depth 1 only).",
                        "required": ["tasks"],
                        "properties": {
                            "tasks": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "Session tasks (stateful.mode = \"session\") or keyed tasks (stateful.mode = \"keyed\") of this job whose units this task's units may own. A session task may not name a keyed task."
                            },
                            "max": {
                                "type": "integer",
                                "minimum": 1,
                                "default": 1,
                                "description": "The most children one owner holds at once. A start past it is refused with RESOURCE_EXHAUSTED."
                            },
                            "suspendWithOwner": {
                                "type": "boolean",
                                "default": false,
                                "description": "Whether an owner's children suspend when it does (those whose task has a suspend block). By default each child follows its own task's idle policy."
                            },
                            "orphanAfter": {
                                "type": "string",
                                "default": "10m",
                                "description": "Session owners only: how long a child on another node outlives an owner nobody can reach, e.g. \"10m\". At least 1m."
                            }
                        }
                    },
                    "run": {
                        "type": "object",
                        "description": "Opts a session task (stateful.mode = \"session\") on the exec, container or sandbox runtime into one-shot runs: `legion session run` starts a fresh single-use session, stages the caller's files under /legion/run/, executes `command` once, returns its output, and ends the session. See docs/session-runs.md.",
                        "required": ["command", "wall"],
                        "properties": {
                            "command": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "The argv a run executes. Name staged files under /legion/run/ (rewritten to the session's own run directory on exec)."
                            },
                            "wall": {
                                "type": "string",
                                "description": "Deadline of one run's command, e.g. \"30s\" or \"5m\". Required; at most 24h. A run past it is stopped and answered with stopped.reason = \"wall\"."
                            },
                            "output": {
                                "type": "string",
                                "default": "1MiB",
                                "description": "Most stdout plus stderr returned, e.g. \"256KiB\". Maximum 1MiB; output beyond it is dropped and the reply says truncated."
                            },
                            "allowCommand": {
                                "type": "boolean",
                                "default": false,
                                "description": "Whether a caller may replace command (--command). The caller also needs exec:write on the job."
                            }
                        }
                    },
                    "disruption": {
                        "type": "object",
                        "description": "Floor on how many of this task's allocations VOLUNTARY disruption (preemption) may leave running. Does not bind drain, node loss or job stop. See docs/preemption-and-eviction.md.",
                        "properties": {
                            "minAvailable": {
                                "description": "Absolute count (2) or percent of count (\"50%\", rounded up). Default 0.",
                                "oneOf": [
                                    { "type": "integer", "minimum": 0 },
                                    { "type": "string" }
                                ]
                            }
                        }
                    },
                    "gang": {},
                    "indexHint": {
                        "type": "string",
                        "minLength": 1,
                        "description": "The scheduling index this task's candidates should come from: the name of an index in the manager's schedulerSettings.indexes, or \"scan\" for the full scan. Advisory: a hint naming no index, or one the task cannot use (a partition key it does not bind with an `=` constraint), falls back to automatic selection, and `job plan` says why in the task's index reason. Default: automatic. See docs/scheduling-indexes.md."
                    },
                    "speculativeStart": {
                        "type": "object",
                        "description": "How far a node may get on a start for this task BEFORE the placement is durable. Absent = off, today's behavior. A task declaring network ports, connect upstreams, or storage whose driver cannot fence a concurrent attach is downgraded to `stage` (reported by `job validate`). See docs/edge-boot.md.",
                        "properties": {
                            "mode": {
                                "type": "string",
                                "enum": ["off", "stage", "boot", "serve"],
                                "description": "off (default) = no speculation. stage = fetch artifacts and build the root disk, do not start. boot = also boot, with the network held shut. serve = also open the network; the only mode where a workload that loses its race may already have had externally visible side effects."
                            },
                            "ratifyTimeout": {
                                "type": "string",
                                "description": "Lease: unratified work is killed at expiry, unconditionally. Default 5s, capped at 2m."
                            },
                            "onRefused": {
                                "type": "string",
                                "enum": ["kill", "stop"],
                                "description": "What to do when ratification is refused. kill (default) is a hard destroy; stop runs the normal graceful-stop ladder."
                            }
                        }
                    }
                }
            },
            "rollout": {
                "type": "object",
                "properties": {
                    "strategy": { "type": "string", "description": "Rollout strategy name (e.g. default)." },
                    "maxParallel": { "type": "integer", "description": "Max allocations updated in parallel." },
                    "autoPromote": { "type": "boolean", "description": "Advance automatically between steps." }
                }
            }
        },
        "taskRuntimes": {}
    })
}
