//! `dlp` — buffers a request body up to `maxBytes` and rejects it when a
//! pattern matches. Built in, in-process, HTTP paths only.

use super::Handler;
use crate::structs::hops::{handler_entry, DlpConfig, HandlerEntry, HopConfig};
use crate::structs::netpolicy::parse_bytes;

/// Bodies buffered for `dlp` when the handler names no `maxBytes`.
pub const DEFAULT_DLP_MAX_BYTES: u64 = 1024 * 1024;

pub(super) struct Dlp {
    source: String,
    max_bytes: u64,
    patterns: Vec<regex::Regex>,
    fails_open: bool,
}

impl Dlp {
    /// Patterns were validated at submit, so one that fails to compile here
    /// (a body written by a newer manager, say) is skipped rather than
    /// failing the listener.
    pub(super) fn new(source: String, config: DlpConfig, hop: HopConfig) -> Self {
        Dlp {
            source,
            max_bytes: config
                .max_bytes
                .as_ref()
                .and_then(|m| parse_bytes(m).ok())
                .unwrap_or(DEFAULT_DLP_MAX_BYTES),
            patterns: config
                .patterns
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect(),
            fails_open: hop.fails_open(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for Dlp {
    fn entry(&self) -> &'static HandlerEntry {
        handler_entry("dlp").expect("registered")
    }

    fn source(&self) -> &str {
        &self.source
    }

    fn body_cap(&self) -> Option<u64> {
        Some(self.max_bytes)
    }

    fn fails_open(&self) -> bool {
        self.fails_open
    }

    fn on_body(&self, body: &[u8]) -> Option<String> {
        if body.len() as u64 > self.max_bytes {
            return (!self.fails_open).then(|| "body larger than dlp maxBytes".to_string());
        }
        let text = String::from_utf8_lossy(body);
        self.patterns
            .iter()
            .find(|p| p.is_match(&text))
            .map(|pattern| format!("body matches dlp pattern {}", pattern.as_str()))
    }
}
