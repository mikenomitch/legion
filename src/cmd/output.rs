//! Agent-friendly CLI output helpers.
//!
//! Legion's CLI is a primary surface for both humans and autonomous agents.
//! Humans want compact tables; agents want a single, stable, machine-parseable
//! shape for every command — including failures. This module centralizes both.
//!
//! ## Envelope
//!
//! In JSON mode every command emits exactly one of two shapes on stdout:
//!
//! ```json
//! { "ok": true,  "data": <command-specific value> }
//! { "ok": false, "error": { "code", "message", "hint", "retryable" } }
//! ```
//!
//! Agents can therefore branch on `ok` once and never need to scrape prose.
//! Errors carry a stable `code` (mirroring the gRPC status code), an optional
//! `hint`, and a `retryable` boolean so an agent knows whether a retry is
//! sensible without parsing the message string.

use crate::prelude::*;
use serde::Serialize;
use serde_json::json;
use tonic::{Code, Status};

/// Output format selected by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-oriented tables and prose.
    Human,
    /// Machine-oriented JSON envelope (`--json`).
    Json,
}

impl OutputFormat {
    /// Derive the format from parsed CLI matches (`--json`/`-j`).
    pub fn from_matches(matches: &ArgMatches) -> Self {
        if matches.get_flag("json") {
            OutputFormat::Json
        } else {
            OutputFormat::Human
        }
    }

    /// True when JSON output was requested.
    pub fn is_json(self) -> bool {
        matches!(self, OutputFormat::Json)
    }
}

/// Structured, machine-readable error. Mirrors the fields an agent needs to
/// decide what to do next.
#[derive(Debug, Clone, Serialize)]
pub struct CliError {
    /// Stable error code, mirroring the gRPC status code name
    /// (for example `NotFound`, `Unavailable`, `InvalidArgument`).
    pub code: String,
    /// Human-readable description of what went wrong.
    pub message: String,
    /// Optional actionable hint for resolving the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Whether retrying the same request may succeed.
    pub retryable: bool,
}

impl CliError {
    /// Build a `CliError` from a gRPC [`Status`], attaching a hint and a
    /// retryable classification derived from the status code.
    pub fn from_status(action: &str, status: &Status) -> Self {
        let code = status.code();
        let (message_text, server_hint) = crate::cmd::helpers::split_hint(status.message().trim());
        let message = if message_text.is_empty() {
            action.to_string()
        } else {
            format!("{action}: {message_text}")
        };

        CliError {
            code: code_name(code).to_string(),
            message,
            hint: server_hint
                .map(str::to_string)
                .or_else(|| hint_for(code, message_text).map(str::to_string)),
            retryable: is_retryable(code),
        }
    }

    /// Build a `CliError` for a locally-detected (non-gRPC) failure, such as a
    /// validation or argument error.
    pub fn local(code: &str, message: impl Into<String>, hint: Option<&str>) -> Self {
        CliError {
            code: code.to_string(),
            message: message.into(),
            hint: hint.map(str::to_string),
            retryable: false,
        }
    }
}

/// Returns whether a retry of the same request may plausibly succeed.
pub fn is_retryable(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::ResourceExhausted
    )
}

/// Stable string name for a gRPC status code (used as the error `code`).
pub fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "Ok",
        Code::Cancelled => "Cancelled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

/// The generic next step for a failure with this code. A server that knows a
/// failure-specific fix puts it in the message as a `Hint:` line, which wins
/// over this (see [`crate::cmd::helpers::split_hint`]). `message` lets one
/// code carry two meanings apart: an `Unavailable` from the transport (the
/// manager itself is unreachable) versus one the manager returned (something
/// behind it — a worker, the raft leader — is).
pub fn hint_for(code: Code, message: &str) -> Option<&'static str> {
    let lower = message.to_ascii_lowercase();
    match code {
        Code::Unavailable
            if [
                "transport error",
                "tcp connect",
                "connection refused",
                "dns error",
                "error trying to connect",
            ]
            .iter()
            .any(|needle| lower.contains(needle)) =>
        {
            Some("manager unavailable; check LEGION_ADDR or start the manager")
        }
        Code::Unavailable => Some(
            "the manager answered, but something it depends on (a worker node, the raft \
             leader) is unreachable; retry shortly",
        ),
        Code::NotFound => {
            Some("resource not found; check the name or id (list commands show what exists)")
        }
        Code::AlreadyExists => Some("resource already exists"),
        Code::InvalidArgument => Some("request was invalid; check the arguments"),
        Code::FailedPrecondition if lower.contains("not the raft leader") => Some(
            "writes go to the raft leader; point LEGION_ADDR at the leader shown above and retry",
        ),
        Code::FailedPrecondition => Some(
            "the request is valid, but the cluster's current state does not allow it; the \
             message says what has to change first",
        ),
        Code::PermissionDenied => {
            Some("the token lacks a policy for this; `legion acl whoami` shows what it has")
        }
        Code::Unauthenticated => Some("no valid ACL token; set LEGION_TOKEN or run `legion login`"),
        Code::DeadlineExceeded => Some("request timed out; retry or raise the timeout"),
        Code::Aborted => Some("the request raced a concurrent change; retry it"),
        Code::ResourceExhausted => Some("a limit was reached; retry later or raise the limit"),
        Code::Unimplemented => {
            Some("the manager does not implement this operation; is it running an older Legion?")
        }
        Code::Internal => Some(
            "unexpected server error; the message carries its cause and the manager's log \
             has the details",
        ),
        _ => None,
    }
}

/// Print a success envelope (`{"ok":true,"data":...}`) as pretty JSON.
pub fn print_success<T: Serialize>(data: &T) {
    let envelope = json!({ "ok": true, "data": data });
    match serde_json::to_string_pretty(&envelope) {
        Ok(text) => println!("{text}"),
        Err(err) => println!(
            "{}",
            json!({ "ok": false, "error": {
                "code": "Internal",
                "message": format!("failed to serialize output: {err}"),
                "retryable": false,
            }})
        ),
    }
}

/// Print a failure envelope (`{"ok":false,"error":...}`) as pretty JSON.
pub fn print_failure(error: &CliError) {
    let envelope = json!({ "ok": false, "error": error });
    // Best-effort: a CliError always serializes.
    let text = serde_json::to_string_pretty(&envelope)
        .unwrap_or_else(|_| "{\"ok\":false,\"error\":{\"code\":\"Internal\",\"message\":\"serialization failed\",\"retryable\":false}}".to_string());
    println!("{text}");
}

/// Resolve a gRPC result for a CLI command.
///
/// On success the inner value is returned. On failure: in JSON mode a
/// structured error envelope is printed and the process exits non-zero (so an
/// agent always sees the standard shape and a failing exit code); in human
/// mode a friendly [`anyhow::Error`] is returned for normal propagation.
pub fn resolve<T>(format: OutputFormat, action: &str, result: Result<T, Status>) -> R<T> {
    match result {
        Ok(value) => Ok(value),
        Err(status) => {
            if format.is_json() {
                print_failure(&CliError::from_status(action, &status));
                std::process::exit(1);
            }
            Err(crate::cmd::helpers::friendly_status(action, status))
        }
    }
}

/// Report a locally-detected failure in the active format.
///
/// In JSON mode prints the error envelope and exits non-zero; in human mode
/// returns an [`anyhow::Error`] carrying the message.
pub fn fail_local(format: OutputFormat, error: CliError) -> anyhow::Error {
    if format.is_json() {
        print_failure(&error);
        std::process::exit(1);
    }
    match error.hint {
        Some(hint) => anyhow!("{} ({hint}) [code={}]", error.message, error.code),
        None => anyhow!("{} [code={}]", error.message, error.code),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_hint_replaces_the_generic_one() {
        let status = Status::failed_precondition(
            "task \"workspace\" asks for memory checkpoints, but sandbox memory checkpoints \
             don't support gated networking yet\nHint: use --disk",
        );
        let err = CliError::from_status("create snapshot", &status);
        assert_eq!(err.hint.as_deref(), Some("use --disk"));
        assert!(!err.message.contains("Hint:"), "{}", err.message);
        assert!(
            err.message.ends_with("gated networking yet"),
            "{}",
            err.message
        );
    }

    #[test]
    fn classifies_retryable_codes() {
        assert!(is_retryable(Code::Unavailable));
        assert!(is_retryable(Code::DeadlineExceeded));
        assert!(!is_retryable(Code::NotFound));
        assert!(!is_retryable(Code::InvalidArgument));
    }

    #[test]
    fn builds_structured_error_from_status() {
        let status = Status::not_found("job demo missing");
        let err = CliError::from_status("failed to fetch job", &status);
        assert_eq!(err.code, "NotFound");
        assert!(err.message.contains("failed to fetch job"));
        assert!(err.message.contains("job demo missing"));
        assert!(!err.retryable);
        assert!(err.hint.is_some());
    }

    #[test]
    fn unavailable_is_retryable_with_hint() {
        let status = Status::unavailable("connection refused");
        let err = CliError::from_status("failed to run job", &status);
        assert_eq!(err.code, "Unavailable");
        assert!(err.retryable);
        assert_eq!(
            err.hint.as_deref(),
            Some("manager unavailable; check LEGION_ADDR or start the manager")
        );
    }

    #[test]
    fn a_failed_precondition_says_what_kind_of_problem_it_is() {
        // P2-2: `job destroy` on a running job. The envelope carries the
        // manager's own reason and a hint that the state must change first.
        let status = Status::failed_precondition(
            "Error deleting job: Job web must be stopped before it can be destroyed (it is \
             running); run `legion job stop web` first",
        );
        let err = CliError::from_status("failed to destroy job", &status);
        assert_eq!(err.code, "FailedPrecondition");
        assert!(err.message.contains("must be stopped"), "{}", err.message);
        assert!(!err.retryable);
        assert!(err
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("what has to change first")));
    }

    #[test]
    fn unavailable_tells_an_unreachable_manager_from_an_unreachable_worker() {
        let transport = Status::unavailable("tcp connect error: Connection refused (os error 111)");
        assert_eq!(
            CliError::from_status("x", &transport).hint.as_deref(),
            Some("manager unavailable; check LEGION_ADDR or start the manager")
        );
        let worker = Status::unavailable("cannot reach node w-1 at https://10.0.0.7:4848");
        let hint = CliError::from_status("x", &worker).hint.unwrap();
        assert!(hint.contains("manager answered"), "{hint}");
    }

    #[test]
    fn every_error_code_but_ok_and_unknown_carries_a_hint() {
        for code in [
            Code::Cancelled,
            Code::InvalidArgument,
            Code::DeadlineExceeded,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Aborted,
            Code::Unimplemented,
            Code::Internal,
            Code::Unavailable,
            Code::Unauthenticated,
        ] {
            if code == Code::Cancelled {
                continue;
            }
            assert!(hint_for(code, "").is_some(), "{code:?} has no hint");
        }
    }

    #[test]
    fn human_errors_name_the_code_like_the_envelope() {
        let err = crate::cmd::helpers::friendly_status(
            "failed to destroy job",
            Status::failed_precondition("Job web must be stopped before it can be destroyed"),
        );
        let text = err.to_string();
        assert!(text.ends_with("[code=FailedPrecondition]"), "{text}");
        assert!(text.contains("must be stopped"), "{text}");
    }
}
