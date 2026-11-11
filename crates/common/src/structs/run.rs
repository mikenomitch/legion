//! Session runs: the `run` block of a session task, and the pieces of a
//! run that the manager, the worker, and the CLI all agree on
//! (docs/session-runs.md).
//!
//! A run is one call that starts a fresh, single-use session of a task,
//! stages the caller's files under the run directory, executes the task's
//! `run.command` once, collects its output, ends the session, and answers.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Where a run's files live inside a unit that has its own root filesystem
/// (`container`, `sandbox`). A jobspec names this path in `run.command`
/// for every runtime; on `exec`, whose sessions share the host's root, the
/// worker rewrites it to the session's own run directory
/// ([`rewrite_run_dir`]).
pub const RUN_DIR: &str = "/legion/run";

/// The file a run's `input` is staged as, relative to the run directory.
pub const RUN_INPUT_FILE: &str = "input.json";

/// The file a command writes its JSON result to, relative to the run
/// directory.
pub const RUN_RESULT_FILE: &str = "result.json";

/// The largest `run.output`, and the default. A run's reply is one gRPC
/// message, so the streams, the result, and the returned files together
/// have to fit one (4 MiB by default).
pub const MAX_RUN_OUTPUT: u64 = 1024 * 1024;

/// The most a request may stage (files plus input), for the same reason.
pub const MAX_RUN_STAGED: u64 = 3 * 1024 * 1024;

/// The longest `run.wall`.
pub const MAX_RUN_WALL: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a node keeps a finished run's reply for idempotent retries.
pub const RUN_REPLY_TTL: Duration = Duration::from_secs(10 * 60);

/// The runtimes that take runs.
pub const RUN_RUNTIMES: [&str; 3] = ["exec", "container", "sandbox"];

/// `run` on a session task: opts the task into `legion session run`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RunConfig {
    /// What a run executes, as an argv. Elements may name files under
    /// `/legion/run/`.
    #[serde(default)]
    pub command: Vec<String>,
    /// The deadline of one run's command, e.g. `"5m"`. Required: a webhook
    /// and a build differ by three orders of magnitude.
    #[serde(default)]
    pub wall: String,
    /// The most stdout plus stderr a run returns, e.g. `"256KiB"`. Default
    /// and maximum `1MiB`; output beyond it is dropped and the reply says
    /// `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Whether a caller may replace `command` (`--command`). Default
    /// false; a caller also needs `exec:write` on the job.
    #[serde(default)]
    pub allow_command: bool,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

crate::structs::spec_fields::spec_block!(RunConfig, ["command", "wall", "output", "allowCommand"]);

/// [`RunConfig`] with its limits parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRun {
    pub command: Vec<String>,
    pub wall: Duration,
    /// As written, for messages.
    pub wall_text: String,
    pub output: u64,
    pub allow_command: bool,
}

impl RunConfig {
    /// Parse the limits; the error names the field.
    pub fn resolve(&self) -> Result<ResolvedRun, String> {
        if self.command.is_empty() || self.command[0].trim().is_empty() {
            return Err("run.command is required: the argv a run executes".into());
        }
        if self.wall.trim().is_empty() {
            return Err(
                "run.wall is required: the deadline of one run, e.g. \"30s\" or \"5m\"".into(),
            );
        }
        let wall = humantime::parse_duration(self.wall.trim())
            .map_err(|err| format!("run.wall {:?} is not a duration: {err}", self.wall))?;
        if wall.is_zero() {
            return Err("run.wall must be greater than zero".into());
        }
        if wall > MAX_RUN_WALL {
            return Err(format!(
                "run.wall {} exceeds the {} maximum; a longer job starts a session and uses \
                 `legion exec`",
                self.wall.trim(),
                humantime::format_duration(MAX_RUN_WALL)
            ));
        }
        let output = match self.output.as_deref() {
            None => MAX_RUN_OUTPUT,
            Some(raw) => {
                let bytes = crate::structs::config::parse_byte_size(raw)
                    .map_err(|err| format!("run.output {raw:?} is not a size: {err}"))?;
                if bytes == 0 {
                    return Err("run.output must be greater than zero".into());
                }
                if bytes > MAX_RUN_OUTPUT {
                    return Err(format!(
                        "run.output {raw} exceeds the 1MiB maximum: a run's reply is one \
                         message; write larger results to a volume or the object store"
                    ));
                }
                bytes
            }
        };
        Ok(ResolvedRun {
            command: self.command.clone(),
            wall,
            wall_text: self.wall.trim().to_string(),
            output,
            allow_command: self.allow_command,
        })
    }
}

/// Submit-time check of a task's `run` block.
pub fn validate_run(task: &crate::structs::TaskConfig) -> Result<(), String> {
    let Some(run) = task.run.as_ref() else {
        return Ok(());
    };
    let prefix = format!("task {}", task.name);
    if !crate::structs::session::is_session_task(task) {
        return Err(format!(
            "{prefix}: run needs a session task (stateful.mode = \"session\"); a run is a \
             fresh single-use session"
        ));
    }
    if !RUN_RUNTIMES.contains(&task.runtime.as_str()) {
        return Err(format!(
            "{prefix}: run is supported on the {} runtimes, not {:?}",
            RUN_RUNTIMES.join(", "),
            task.runtime
        ));
    }
    run.resolve()
        .map(|_| ())
        .map_err(|err| format!("{prefix}: {err}"))
}

/// The error a run on a task without a `run` block answers.
pub fn no_run_block(job_id: &str, task_name: &str) -> String {
    format!(
        "task {task_name} of job {job_id} does not take runs; add a run block to it \
         (run = {{ command = [...], wall = \"30s\" }}) or start a session and use `legion exec`"
    )
}

/// The error `--command` answers on a task that does not allow it.
pub fn command_not_allowed(task_name: &str) -> String {
    format!("task {task_name} does not allow --command (run.allowCommand = false)")
}

/// Check a caller's file name (a `--file` entry or a `--get` path) and
/// return it relative to the run directory. Names are relative, or absolute
/// under `/legion/run/`; nothing outside the run directory is staged or
/// read, so a run cannot be used to read or overwrite the rest of the unit
/// (or, on `exec`, the host).
pub fn run_relative_path(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let relative = match trimmed.strip_prefix(RUN_DIR) {
        Some(rest) if rest.is_empty() || rest.starts_with('/') => rest.trim_start_matches('/'),
        _ => trimmed,
    };
    if relative.is_empty() {
        return Err(format!("{raw:?} names no file"));
    }
    if relative.starts_with('/') {
        return Err(format!(
            "{raw:?} is outside the run directory; name files relative to it or under {RUN_DIR}/"
        ));
    }
    if relative.len() > 512 {
        return Err(format!("{raw:?} is longer than 512 bytes"));
    }
    for part in relative.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(format!(
                "{raw:?} must be a plain relative path (no empty, `.` or `..` components)"
            ));
        }
        if part.chars().any(|c| c.is_control()) {
            return Err(format!("{raw:?} contains a control character"));
        }
    }
    Ok(relative.to_string())
}

/// Rewrite `/legion/run` at the start of each argv element to `run_dir`,
/// for runtimes whose sessions do not have a root filesystem of their own.
pub fn rewrite_run_dir(command: &[String], run_dir: &str) -> Vec<String> {
    if run_dir == RUN_DIR {
        return command.to_vec();
    }
    command
        .iter()
        .map(|arg| match arg.strip_prefix(RUN_DIR) {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => format!("{run_dir}{rest}"),
            _ => arg.clone(),
        })
        .collect()
}

/// Keep at most `budget` more bytes of `chunk` in `into`; true when some
/// were dropped.
pub fn append_capped(into: &mut Vec<u8>, chunk: &[u8], budget: &mut u64) -> bool {
    let take = (*budget).min(chunk.len() as u64) as usize;
    into.extend_from_slice(&chunk[..take]);
    *budget -= take as u64;
    take < chunk.len()
}

/// The id of a run with an idempotency key: stable per job, task and key.
pub fn derived_run_id(job_id: &str, task_name: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for part in [job_id, task_name, key] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(12).map(|b| format!("{b:02x}")).collect();
    format!("run-{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(wall: &str, output: Option<&str>) -> RunConfig {
        RunConfig {
            command: vec!["python3".into(), "/legion/run/main.py".into()],
            wall: wall.into(),
            output: output.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn a_run_block_resolves_its_limits_test() {
        let resolved = run("5m", Some("256KiB")).resolve().unwrap();
        assert_eq!(resolved.wall, Duration::from_secs(300));
        assert_eq!(resolved.output, 256 * 1024);
        assert!(!resolved.allow_command);
        assert_eq!(run("1s", None).resolve().unwrap().output, MAX_RUN_OUTPUT);
    }

    #[test]
    fn wall_is_required_and_bounded_test() {
        assert!(run("", None).resolve().unwrap_err().contains("run.wall"));
        assert!(run("soon", None)
            .resolve()
            .unwrap_err()
            .contains("not a duration"));
        assert!(run("0s", None)
            .resolve()
            .unwrap_err()
            .contains("greater than zero"));
        assert!(run("48h", None).resolve().unwrap_err().contains("maximum"));
    }

    #[test]
    fn output_is_capped_at_one_message_test() {
        assert!(run("1s", Some("2MiB"))
            .resolve()
            .unwrap_err()
            .contains("1MiB maximum"));
        assert!(run("1s", Some("lots"))
            .resolve()
            .unwrap_err()
            .contains("not a size"));
    }

    #[test]
    fn a_command_is_required_test() {
        let empty = RunConfig {
            wall: "1s".into(),
            ..Default::default()
        };
        assert!(empty.resolve().unwrap_err().contains("run.command"));
    }

    /// The first task of a job parsed as a jobspec is, which is what turns
    /// `stateful.mode = "session"` into the stored form validation sees.
    fn parsed_task(task: serde_json::Value) -> crate::structs::TaskConfig {
        let config: crate::structs::JobConfig =
            serde_json::from_value(serde_json::json!({ "name": "code-runner", "task": task }))
                .unwrap();
        config.tasks[0].clone()
    }

    #[test]
    fn run_needs_a_session_task_on_a_supported_runtime_test() {
        let session = serde_json::json!({
            "mode": "session",
            "envelope": { "resources": { "cpu": 400, "memory": 256 } },
        });
        let run = serde_json::json!({ "command": ["true"], "wall": "5s" });
        let plain = parsed_task(serde_json::json!({
            "name": "machine",
            "runtime": "exec",
            "config": { "command": ["sleep", "600"] },
            "run": run,
        }));
        assert!(validate_run(&plain)
            .unwrap_err()
            .contains("needs a session task"));
        let mut task = parsed_task(serde_json::json!({
            "name": "machine",
            "runtime": "noop",
            "resources": { "cpu": 100, "memory": 64 },
            "stateful": session,
            "run": run,
        }));
        assert!(validate_run(&task).unwrap_err().contains("not \"noop\""));
        task.runtime = "exec".into();
        validate_run(&task).unwrap();
        task.run.as_mut().unwrap().wall.clear();
        assert!(validate_run(&task)
            .unwrap_err()
            .contains("task machine: run.wall"));
        task.run = None;
        validate_run(&task).unwrap();
    }

    #[test]
    fn file_names_stay_inside_the_run_directory_test() {
        assert_eq!(run_relative_path("main.py").unwrap(), "main.py");
        assert_eq!(
            run_relative_path("/legion/run/out/a.csv").unwrap(),
            "out/a.csv"
        );
        for bad in [
            "",
            "/etc/passwd",
            "../x",
            "a/../b",
            "a//b",
            "./a",
            "/legion/run/",
        ] {
            assert!(run_relative_path(bad).is_err(), "{bad:?} must be refused");
        }
        // A sibling of the run directory is not inside it.
        assert!(run_relative_path("/legion/runner/x").is_err());
    }

    #[test]
    fn the_run_directory_is_rewritten_only_as_a_path_prefix_test() {
        let command = vec![
            "python3".to_string(),
            "/legion/run/main.py".to_string(),
            "/legion/running".to_string(),
            "/legion/run".to_string(),
        ];
        assert_eq!(
            rewrite_run_dir(&command, "/var/task/run"),
            vec![
                "python3",
                "/var/task/run/main.py",
                "/legion/running",
                "/var/task/run"
            ]
        );
        assert_eq!(rewrite_run_dir(&command, RUN_DIR), command);
    }

    #[test]
    fn output_is_truncated_at_the_budget_test() {
        let mut kept = Vec::new();
        let mut budget = 5u64;
        assert!(!append_capped(&mut kept, b"abc", &mut budget));
        assert!(append_capped(&mut kept, b"defg", &mut budget));
        assert!(append_capped(&mut kept, b"h", &mut budget));
        assert_eq!(kept, b"abcde");
    }

    #[test]
    fn a_run_id_is_stable_per_job_task_and_key_test() {
        let id = derived_run_id("job", "task", "k1");
        assert_eq!(id, derived_run_id("job", "task", "k1"));
        assert_ne!(id, derived_run_id("job", "task", "k2"));
        assert_ne!(id, derived_run_id("job2", "task", "k1"));
        // Length-prefixed parts: shifting a boundary changes the id.
        assert_ne!(
            derived_run_id("ab", "c", "k"),
            derived_run_id("a", "bc", "k")
        );
    }
}
