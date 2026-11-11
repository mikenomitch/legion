//! Where a run of an isolate task happens (brief item 9, plan §6.12): the
//! tier decision, the node's memo of code that needed a VM, and the
//! mapping of a host's `InvokeReply` to the `RunSessionResponse` a caller
//! gets.

use crate::structs::isolate_host::{InvokeReply, RunOutcome};
use crate::structs::{RunOutputFile, RunSessionResponse, RunStop};
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub const TIER_ISOLATE: &str = "isolate";
pub const TIER_VM: &str = "vm";
pub const TIER_AUTO: &str = "auto";

/// What the decision needs to know, gathered before anything runs.
#[derive(Debug, Clone, Default)]
pub struct TierInputs<'a> {
    /// `RunSessionRequest.tier` as sent.
    pub requested: &'a str,
    /// The task has a VM task (`config.vm` resolves to one).
    pub has_vm: bool,
    /// A runtime disable covers `isolate` for the job.
    pub runtime_disabled: bool,
    /// The pre-flight scan's missing imports.
    pub missing_imports: &'a [String],
    /// The image's name, for the reason text.
    pub image: Option<&'a str>,
    /// How long ago the memo says this code needed a VM, when it does.
    pub memo_age: Option<Duration>,
}

/// The decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tier {
    Isolate { reason: String },
    Vm { reason: String },
}

/// Why a run cannot be placed at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierRefusal {
    /// An unknown `tier` value.
    Invalid(String),
    /// `tier = "vm"` on a task without `config.vm`.
    NoVm,
    /// The isolate runtime is disabled for the job and the run cannot go to
    /// a VM.
    RuntimeDisabled,
}

/// Decide the tier. `auto` (or empty) goes to the isolate unless the
/// runtime is disabled, the scan found a missing import, or the memo hit,
/// and the task has somewhere else to go.
pub fn decide(inputs: &TierInputs<'_>) -> Result<Tier, TierRefusal> {
    match inputs.requested.trim() {
        TIER_ISOLATE => {
            if inputs.runtime_disabled {
                return Err(TierRefusal::RuntimeDisabled);
            }
            Ok(Tier::Isolate {
                reason: "requested".into(),
            })
        }
        TIER_VM => {
            if !inputs.has_vm {
                return Err(TierRefusal::NoVm);
            }
            Ok(Tier::Vm {
                reason: "requested".into(),
            })
        }
        "" | TIER_AUTO => {
            if inputs.runtime_disabled {
                return if inputs.has_vm {
                    Ok(Tier::Vm {
                        reason: "auto: the isolate runtime is disabled".into(),
                    })
                } else {
                    Err(TierRefusal::RuntimeDisabled)
                };
            }
            if inputs.has_vm {
                if let Some(first) = inputs.missing_imports.first() {
                    return Ok(Tier::Vm {
                        reason: format!(
                            "auto: import {first} is not in image {}",
                            inputs.image.unwrap_or("module")
                        ),
                    });
                }
                if let Some(age) = inputs.memo_age {
                    return Ok(Tier::Vm {
                        reason: format!("auto: this code needed a VM {} ago", short_age(age)),
                    });
                }
            }
            Ok(Tier::Isolate {
                reason: "auto: no imports outside the image".into(),
            })
        }
        other => Err(TierRefusal::Invalid(format!(
            "tier {other:?} is not one of \"auto\", \"isolate\", \"vm\""
        ))),
    }
}

/// `12m`, `40s`, `2h`: the largest whole unit.
pub fn short_age(age: Duration) -> String {
    let secs = age.as_secs();
    if secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// The node's memory of code that needed a VM: keyed by a hash of the job,
/// the task, and the entry source, remembered for `config.vm.memoFor`, at
/// most [`VmMemo::CAPACITY`] entries (least recently recorded dropped).
#[derive(Debug, Default)]
pub struct VmMemo {
    entries: HashMap<[u8; 32], Instant>,
}

impl VmMemo {
    pub const CAPACITY: usize = 4096;

    /// The memo key for a run.
    pub fn key(job_id: &str, task_name: &str, source: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(job_id.as_bytes());
        hasher.update([0]);
        hasher.update(task_name.as_bytes());
        hasher.update([0]);
        hasher.update(source);
        hasher.finalize().into()
    }

    /// Remember that the code needed a VM, now.
    pub fn record(&mut self, key: [u8; 32], now: Instant) {
        if self.entries.len() >= Self::CAPACITY && !self.entries.contains_key(&key) {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, at)| **at)
                .map(|(key, _)| *key)
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, now);
    }

    /// How long ago the code needed a VM, when that is within `memo_for`.
    pub fn lookup(&mut self, key: &[u8; 32], memo_for: Duration, now: Instant) -> Option<Duration> {
        if memo_for.is_zero() {
            return None;
        }
        let at = *self.entries.get(key)?;
        let age = now.saturating_duration_since(at);
        if age > memo_for {
            self.entries.remove(key);
            return None;
        }
        Some(age)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The hint a stopped isolate run carries (plan §7.2).
pub fn stop_hint(reason: &str, limit: &str, has_vm: bool) -> String {
    let vm = if has_vm { ", or rerun with --tier vm" } else { "" };
    match (reason, limit) {
        ("limits", "cpu") => format!("raise config.limits.cpu{vm}"),
        ("limits", "wall") => format!("raise config.limits.wall{vm}"),
        ("limits", "memory") => format!("raise resources.memoryMax{vm}"),
        ("limits", "output") => "raise config.limits.output, or write large results to a file \
                                 the caller gets"
            .into(),
        ("limits", _) => format!("raise the task's config.limits{vm}"),
        ("unsupported", _) if has_vm => "rerun with --tier vm (this code's next auto run on this \
                                         node goes to a VM on its own), or pass --fallback vm \
                                         to rerun it once automatically"
            .into(),
        ("unsupported", _) => "this code needs something isolates do not offer; give the task \
                               a VM tier with config.vm"
            .into(),
        ("trap", _) => "the code crashed the isolate; check its stderr".into(),
        ("ended", _) => "its session isolate was stopped or the node drained; retry".into(),
        _ => String::new(),
    }
}

/// Whether a stop says the code belongs in a VM.
pub fn wants_vm(reason: &str) -> bool {
    matches!(reason, "unsupported" | "limits")
}

/// Map a host's reply to the caller's.
pub fn map_invoke_reply(
    reply: InvokeReply,
    node_id: &str,
    task_name: &str,
    run_id: &str,
    has_vm: bool,
    tier_reason: &str,
    get: &[String],
) -> RunSessionResponse {
    let run = match reply.outcome {
        Some(crate::structs::isolate_host::invoke_reply::Outcome::Run(run)) => run,
        _ => RunOutcome::default(),
    };
    let stopped = reply.stopped.map(|stop| RunStop {
        hint: stop_hint(&stop.reason, &stop.limit, has_vm),
        reason: stop.reason,
        limit: stop.limit,
        detail: stop.detail,
    });
    let next_tier = match &stopped {
        Some(stop) if has_vm && wants_vm(&stop.reason) => TIER_VM.to_string(),
        _ => String::new(),
    };
    let files = run
        .files
        .into_iter()
        .enumerate()
        .map(|(index, file)| RunOutputFile {
            path: get.get(index).cloned().unwrap_or(file.path),
            content: file.content,
            truncated: file.truncated,
            missing: file.missing,
        })
        .collect();
    RunSessionResponse {
        node_id: node_id.to_string(),
        session_id: run_id.to_string(),
        task_name: task_name.to_string(),
        exit: if stopped.is_some() { None } else { run.exit },
        stdout: reply.stdout,
        stderr: reply.stderr,
        truncated: reply.truncated,
        result: run.result,
        files,
        wall_ms: rounded_ms(reply.wall_micros),
        cpu_ms: rounded_ms(reply.cpu_micros),
        stopped,
        run_id: run_id.to_string(),
        tier: TIER_ISOLATE.into(),
        tier_reason: tier_reason.to_string(),
        next_tier,
        cpu_micros: reply.cpu_micros,
        wall_micros: reply.wall_micros,
        isolate_start: reply.start,
        ..Default::default()
    }
}

fn rounded_ms(micros: u64) -> u64 {
    (micros + 500) / 1000
}

#[cfg(test)]
mod tier_test {
    use super::*;
    use crate::structs::isolate_host::{invoke_reply, File, Stopped};

    fn inputs<'a>(requested: &'a str, missing: &'a [String]) -> TierInputs<'a> {
        TierInputs {
            requested,
            has_vm: true,
            runtime_disabled: false,
            missing_imports: missing,
            image: Some("python"),
            memo_age: None,
        }
    }

    #[test]
    fn decision_table() {
        let none: Vec<String> = Vec::new();
        let torch = vec!["torch".to_string()];
        let isolate = |reason: &str| Ok(Tier::Isolate { reason: reason.into() });
        let vm = |reason: &str| Ok(Tier::Vm { reason: reason.into() });

        assert_eq!(decide(&inputs("", &none)), isolate("auto: no imports outside the image"));
        assert_eq!(decide(&inputs("auto", &none)), isolate("auto: no imports outside the image"));
        assert_eq!(decide(&inputs("isolate", &torch)), isolate("requested"));
        assert_eq!(decide(&inputs("vm", &none)), vm("requested"));
        assert_eq!(
            decide(&inputs("auto", &torch)),
            vm("auto: import torch is not in image python")
        );
        // No VM task: a missing import goes to the isolate and fails there.
        let mut no_vm = inputs("auto", &torch);
        no_vm.has_vm = false;
        assert_eq!(decide(&no_vm), isolate("auto: no imports outside the image"));
        no_vm.requested = "vm";
        assert_eq!(decide(&no_vm), Err(TierRefusal::NoVm));
        // The memo.
        let mut memo = inputs("auto", &none);
        memo.memo_age = Some(Duration::from_secs(12 * 60 + 5));
        assert_eq!(decide(&memo), vm("auto: this code needed a VM 12m ago"));
        memo.has_vm = false;
        assert_eq!(decide(&memo), isolate("auto: no imports outside the image"));
        // A disabled runtime.
        let mut disabled = inputs("auto", &none);
        disabled.runtime_disabled = true;
        assert_eq!(decide(&disabled), vm("auto: the isolate runtime is disabled"));
        disabled.has_vm = false;
        assert_eq!(decide(&disabled), Err(TierRefusal::RuntimeDisabled));
        disabled.requested = "isolate";
        assert_eq!(decide(&disabled), Err(TierRefusal::RuntimeDisabled));
        // Garbage.
        assert!(matches!(
            decide(&inputs("gpu", &none)),
            Err(TierRefusal::Invalid(message)) if message.contains("\"gpu\"")
        ));
    }

    #[test]
    fn memo_expires_and_is_bounded() {
        let mut memo = VmMemo::default();
        let now = Instant::now();
        let key = VmMemo::key("job", "run", b"import torch");
        assert_eq!(memo.lookup(&key, Duration::from_secs(3600), now), None);
        memo.record(key, now);
        assert_eq!(
            memo.lookup(&key, Duration::from_secs(3600), now + Duration::from_secs(60)),
            Some(Duration::from_secs(60))
        );
        assert_eq!(memo.lookup(&key, Duration::ZERO, now), None);
        assert_eq!(
            memo.lookup(&key, Duration::from_secs(30), now + Duration::from_secs(60)),
            None
        );
        assert_eq!(memo.len(), 0, "an expired entry is dropped");
        // Keys differ by job, task, and source.
        assert_ne!(VmMemo::key("a", "b", b"x"), VmMemo::key("a", "bx", b""));
        for n in 0..(VmMemo::CAPACITY + 10) {
            memo.record(
                VmMemo::key("j", "t", n.to_string().as_bytes()),
                now + Duration::from_millis(n as u64),
            );
        }
        assert_eq!(memo.len(), VmMemo::CAPACITY);
        assert_eq!(
            memo.lookup(&VmMemo::key("j", "t", b"0"), Duration::from_secs(3600), now),
            None,
            "the oldest entry went first"
        );
    }

    #[test]
    fn short_ages() {
        assert_eq!(short_age(Duration::from_secs(5)), "5s");
        assert_eq!(short_age(Duration::from_secs(720)), "12m");
        assert_eq!(short_age(Duration::from_secs(7300)), "2h");
    }

    #[test]
    fn reply_mapping_for_a_finished_run() {
        let reply = InvokeReply {
            outcome: Some(invoke_reply::Outcome::Run(RunOutcome {
                exit: Some(0),
                result: "{\"n\":1}".into(),
                files: vec![File {
                    path: "out.txt".into(),
                    content: b"hi".to_vec(),
                    ..Default::default()
                }],
            })),
            stdout: b"42\n".to_vec(),
            cpu_micros: 3_140,
            wall_micros: 4_499,
            start: "fresh".into(),
            ..Default::default()
        };
        let mapped = map_invoke_reply(
            reply,
            "w-1",
            "run",
            "inv-1",
            true,
            "auto: no imports outside the image",
            &["/legion/run/out.txt".to_string()],
        );
        assert_eq!(mapped.exit, Some(0));
        assert_eq!(mapped.stdout, b"42\n");
        assert_eq!(mapped.cpu_ms, 3);
        assert_eq!(mapped.wall_ms, 4);
        assert_eq!(mapped.cpu_micros, 3_140);
        assert_eq!(mapped.files[0].path, "/legion/run/out.txt");
        assert_eq!(mapped.tier, "isolate");
        assert_eq!(mapped.next_tier, "");
        assert_eq!(mapped.isolate_start, "fresh");
        assert_eq!(mapped.session_id, "inv-1");
        assert!(mapped.stopped.is_none());
    }

    #[test]
    fn reply_mapping_for_stopped_runs() {
        let stopped = |reason: &str, limit: &str| InvokeReply {
            outcome: Some(invoke_reply::Outcome::Run(RunOutcome {
                exit: Some(3),
                ..Default::default()
            })),
            stopped: Some(Stopped {
                reason: reason.into(),
                limit: limit.into(),
                detail: "exceeded its 100ms CPU limit".into(),
            }),
            ..Default::default()
        };
        let mapped = map_invoke_reply(stopped("limits", "cpu"), "w", "run", "i", true, "", &[]);
        assert_eq!(mapped.exit, None, "a stopped run has no exit");
        let stop = mapped.stopped.unwrap();
        assert_eq!(stop.limit, "cpu");
        assert!(stop.hint.contains("config.limits.cpu") && stop.hint.contains("--tier vm"));
        assert_eq!(mapped.next_tier, "vm");

        let mapped = map_invoke_reply(stopped("limits", "cpu"), "w", "run", "i", false, "", &[]);
        assert_eq!(mapped.next_tier, "", "no VM task, no next tier");
        assert!(!mapped.stopped.unwrap().hint.contains("--tier vm"));

        let mapped = map_invoke_reply(stopped("unsupported", ""), "w", "run", "i", true, "", &[]);
        assert_eq!(mapped.next_tier, "vm");
        assert!(mapped.stopped.unwrap().hint.contains("--fallback vm"));

        let mapped = map_invoke_reply(stopped("trap", ""), "w", "run", "i", true, "", &[]);
        assert_eq!(mapped.next_tier, "", "a crash is not a VM's job");
    }
}
