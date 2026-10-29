//! The jobspec's JSON Schema (`legion job schema`, `GetJobSchema`, the
//! `legion_job_schema` MCP tool): a documented top-level envelope plus the
//! live, per-runtime task config schemas derived from the built-in plugins.
//! A manager adds the keys installed apps declare at their extension points
//! (`crate::structs::extensions::add_keys_to_schema`).

use crate::task_plugins::builtin_task_plugin_schemas;
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
    schema["taskRuntimes"] = Value::Object(task_runtimes);
    schema
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
                    "gang": {
                        "description": "All-or-nothing placement for this task's replicas. `true`: every replica places in one plan or none does, and a gang that does not fit places nothing (nor do the job's other tasks in that pass), holds no capacity, and waits blocked until a node that could help has room. `{ min: N }` places once N replicas fit together and adds the rest as room appears; `placementTimeout` bounds the wait and `onMemberLoss` says what a lost member does to the rest. Not combinable with count.per or keyed stateful tasks; the task must have its group to itself, and that group's rollout must replace it in one step. See docs/gang-scheduling.md.",
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
                                    "onMemberLoss": {
                                        "type": "string",
                                        "enum": ["restart", "replace", "fail"],
                                        "description": "What a lost member does to the rest when it leaves fewer than the quorum running. `restart` (default): stop the survivors and place the whole gang again once their stop grace has passed. `replace`: keep the survivors and place the missing members together once they fit. `fail`: fail the job and stop every member. A loss that leaves the quorum running is replaced alone. Restarts after a failure spend on_unhealthy's move budget; once it is spent the job fails."
                                    }
                                }
                            }
                        ]
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
