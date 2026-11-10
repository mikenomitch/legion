//! The syntax of a `connect.templates[].source` template: tokenizing, and
//! the admission-time checks that need nothing but the text. Rendering one
//! against an allocation's endpoints is the worker's
//! (`worker::templates`); validating a jobspec only needs to know the
//! template parses and which upstreams it names.

use crate::prelude::*;

/// Upstream keys a template refers to (`service "x"`, `endpoints "x"`), for
/// admission-time validation.
pub fn referenced_upstreams(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in tokens(source) {
        if let Token::Action(action) = token {
            let words = split_words(&action);
            match words.first().map(String::as_str) {
                Some("service") | Some("endpoints") => {
                    if let Some(key) = words.get(1) {
                        out.push(key.clone());
                    }
                }
                Some("range") if words.get(1).map(String::as_str) == Some("endpoints") => {
                    if let Some(key) = words.get(2) {
                        out.push(key.clone());
                    }
                }
                _ => {}
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Every action a template uses, so validation can reject unknown ones
/// with the line in the message.
pub fn validate_source(source: &str) -> R<()> {
    let mut depth = 0usize;
    for token in tokens(source) {
        let Token::Action(action) = token else {
            continue;
        };
        let words = split_words(&action);
        match words.first().map(String::as_str) {
            Some("service") => {
                if words.len() < 2 || words.len() > 3 {
                    bail!("`{{{{ {action} }}}}`: service takes an upstream key and an optional part (host|port)");
                }
                if let Some(part) = words.get(2) {
                    if part != "host" && part != "port" {
                        bail!("`{{{{ {action} }}}}`: the part must be host or port, got {part:?}");
                    }
                }
            }
            Some("env") => {
                if words.len() != 2 {
                    bail!("`{{{{ {action} }}}}`: env takes exactly one name");
                }
            }
            Some("range") => {
                if words.get(1).map(String::as_str) != Some("endpoints") || words.len() != 3 {
                    bail!("`{{{{ {action} }}}}`: only `range endpoints \"<upstream>\"` is supported");
                }
                depth += 1;
            }
            Some("end") => {
                if depth == 0 {
                    bail!("`{{{{ end }}}}` without a matching `{{{{ range ... }}}}`");
                }
                depth -= 1;
            }
            Some(".address") | Some(".port") | Some(".node") | Some(".tags") | Some(".allocation") => {
                if depth == 0 {
                    bail!("`{{{{ {action} }}}}` is only valid inside `{{{{ range endpoints ... }}}}`");
                }
            }
            Some(other) => bail!(
                "`{{{{ {action} }}}}`: unknown template action {other:?}; use service, endpoints (in a range), env"
            ),
            None => bail!("empty `{{{{ }}}}` action"),
        }
    }
    if depth != 0 {
        bail!("`{{{{ range ... }}}}` without a matching `{{{{ end }}}}`");
    }
    Ok(())
}

/// One piece of a template: literal text, or the inside of a `{{ }}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Text(String),
    Action(String),
}

/// Split a template into text and `{{ action }}` tokens. An unclosed `{{`
/// is text.
pub fn tokens(source: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        if open > 0 {
            out.push(Token::Text(rest[..open].to_string()));
        }
        let after = &rest[open + 2..];
        match after.find("}}") {
            Some(close) => {
                out.push(Token::Action(after[..close].trim().to_string()));
                rest = &after[close + 2..];
            }
            None => {
                out.push(Token::Text(rest[open..].to_string()));
                rest = "";
                break;
            }
        }
    }
    if !rest.is_empty() {
        out.push(Token::Text(rest.to_string()));
    }
    out
}

/// Split an action into words, honouring double quotes.
pub fn split_words(action: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for ch in action.chars() {
        match ch {
            '"' => quoted = !quoted,
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}
