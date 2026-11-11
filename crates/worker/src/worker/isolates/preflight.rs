//! The pre-flight import scan a `tier = "auto"` run takes before anything
//! runs (isolate-runtime plan §6.12, brief item 9): read the imports of the
//! run's entry source and compare their top-level module names with what
//! the task's image ships.
//!
//! It is a heuristic, written to be cheap and safe on hostile input: a
//! single forward pass over at most [`MAX_SCAN_BYTES`] with no regular
//! expressions and no backtracking. Comments and string literals never
//! produce imports; relative imports (`from . import x`, `"./x.js"`) are
//! the run's own code and never count as missing. What it cannot see (an
//! import built at run time from a variable) is found by the isolate
//! itself, which stops the run as `unsupported` and sets `next_tier`.

use std::collections::BTreeSet;

/// The most source the scan reads. A longer entry is scanned up to here.
pub const MAX_SCAN_BYTES: usize = 256 * 1024;

/// The source language of an entry, by image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Python,
    JavaScript,
}

impl Language {
    /// The language an image runs (`python`, `js`); `None` for a module.
    pub fn of_image(image: &str) -> Option<Self> {
        match crate::structs::isolate::image_name(image) {
            "python" => Some(Language::Python),
            "js" => Some(Language::JavaScript),
            _ => None,
        }
    }
}

/// What the scan found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scan {
    /// Top-level module names the source imports, in first-seen order.
    pub imports: Vec<String>,
    /// Those the image lacks and the run does not bring itself.
    pub missing: Vec<String>,
}

/// Scan `source` and compare against `available` (the image manifest's
/// module list) and the run's own file names (`helper.py`, `pkg/mod.py`).
pub fn scan(
    language: Language,
    source: &[u8],
    available: &[String],
    run_files: &[String],
) -> Scan {
    let imports = imports(language, source);
    let available: BTreeSet<&str> = available.iter().map(String::as_str).collect();
    let local = local_modules(language, run_files);
    let missing = imports
        .iter()
        .filter(|name| !available.contains(name.as_str()) && !local.contains(name.as_str()))
        .cloned()
        .collect();
    Scan { imports, missing }
}

/// The modules a run's own files provide: `helper.py` and `helper/` give
/// `helper` (Python); JavaScript imports its own files by relative path,
/// which the scanner already treats as local.
fn local_modules(language: Language, files: &[String]) -> BTreeSet<String> {
    let mut local = BTreeSet::new();
    for file in files {
        let path = file
            .trim()
            .trim_start_matches(crate::structs::run::RUN_DIR)
            .trim_start_matches('/');
        let first = path.split('/').next().unwrap_or_default();
        if first.is_empty() {
            continue;
        }
        if path.contains('/') {
            // A directory: a package (Python) or a folder of modules.
            local.insert(first.to_string());
            continue;
        }
        let stem = match language {
            Language::Python => first.strip_suffix(".py"),
            Language::JavaScript => first
                .strip_suffix(".mjs")
                .or_else(|| first.strip_suffix(".js")),
        };
        if let Some(stem) = stem {
            local.insert(stem.to_string());
        }
    }
    local
}

/// The top-level module names `source` imports, in first-seen order.
pub fn imports(language: Language, source: &[u8]) -> Vec<String> {
    let source = &source[..source.len().min(MAX_SCAN_BYTES)];
    let tokens = match language {
        Language::Python => lex_python(source),
        Language::JavaScript => lex_js(source),
    };
    let mut found = Vec::new();
    let mut seen = BTreeSet::new();
    let mut push = |name: String| {
        if !name.is_empty() && seen.insert(name.clone()) {
            found.push(name);
        }
    };
    match language {
        Language::Python => python_imports(&tokens, &mut push),
        Language::JavaScript => js_imports(&tokens, &mut push),
    }
    found
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Str(String),
    Punct(u8),
    /// A logical line end (Python) or nothing (JavaScript).
    Newline,
}

// ---------------------------------------------------------------------------
// Python
// ---------------------------------------------------------------------------

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn is_ident(b: u8) -> bool {
    is_ident_start(b) || b.is_ascii_digit()
}

fn lex_python(src: &[u8]) -> Vec<Tok> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut depth = 0usize;
    while i < src.len() {
        let b = src[i];
        match b {
            b'#' => {
                while i < src.len() && src[i] != b'\n' {
                    i += 1;
                }
            }
            b'\\' if src.get(i + 1) == Some(&b'\n') => i += 2,
            b'\n' => {
                if depth == 0 && out.last() != Some(&Tok::Newline) {
                    out.push(Tok::Newline);
                }
                i += 1;
            }
            b'\'' | b'"' => {
                let (value, next) = python_string(src, i);
                out.push(Tok::Str(value));
                i = next;
            }
            _ if is_ident_start(b) => {
                let start = i;
                while i < src.len() && is_ident(src[i]) {
                    i += 1;
                }
                let word = &src[start..i];
                // A string prefix (r, b, f, u, rb, br, fr, rf in any case).
                if i < src.len()
                    && (src[i] == b'\'' || src[i] == b'"')
                    && word.len() <= 2
                    && word
                        .iter()
                        .all(|c| matches!(c.to_ascii_lowercase(), b'r' | b'b' | b'f' | b'u'))
                {
                    let (value, next) = python_string(src, i);
                    out.push(Tok::Str(value));
                    i = next;
                    continue;
                }
                out.push(Tok::Ident(String::from_utf8_lossy(word).into_owned()));
            }
            b'(' | b'[' | b'{' => {
                depth += 1;
                out.push(Tok::Punct(b));
                i += 1;
            }
            b')' | b']' | b'}' => {
                depth = depth.saturating_sub(1);
                out.push(Tok::Punct(b));
                i += 1;
            }
            _ if b.is_ascii_whitespace() => i += 1,
            _ => {
                out.push(Tok::Punct(b));
                i += 1;
            }
        }
    }
    out
}

/// A Python string literal starting at `start` (a quote): its value (raw,
/// escapes left as written) and the index after it. Unterminated strings
/// end at the line end (single) or the source end (triple).
fn python_string(src: &[u8], start: usize) -> (String, usize) {
    let quote = src[start];
    let triple = src.get(start + 1) == Some(&quote) && src.get(start + 2) == Some(&quote);
    let mut i = start + if triple { 3 } else { 1 };
    let body_start = i;
    while i < src.len() {
        let b = src[i];
        if b == b'\\' {
            i += 2;
            continue;
        }
        if triple {
            if b == quote && src.get(i + 1) == Some(&quote) && src.get(i + 2) == Some(&quote) {
                let value = String::from_utf8_lossy(&src[body_start..i]).into_owned();
                return (value, i + 3);
            }
        } else if b == quote {
            let value = String::from_utf8_lossy(&src[body_start..i]).into_owned();
            return (value, i + 1);
        } else if b == b'\n' {
            return (String::new(), i);
        }
        i += 1;
    }
    (String::new(), src.len().min(i))
}

fn top_python(name: &str) -> String {
    name.split('.').next().unwrap_or_default().trim().to_string()
}

fn python_imports(tokens: &[Tok], push: &mut impl FnMut(String)) {
    let ident = |i: usize| match tokens.get(i) {
        Some(Tok::Ident(word)) => Some(word.as_str()),
        _ => None,
    };
    let punct = |i: usize, c: u8| tokens.get(i) == Some(&Tok::Punct(c));
    let mut i = 0;
    let mut statement_start = true;
    while i < tokens.len() {
        let at_start = statement_start;
        statement_start = matches!(
            tokens[i],
            Tok::Newline | Tok::Punct(b';') | Tok::Punct(b':')
        );
        match &tokens[i] {
            Tok::Ident(word) if at_start && word == "import" => {
                // import a.b as c, d
                i += 1;
                loop {
                    let Some(name) = ident(i) else { break };
                    push(top_python(name));
                    i += 1;
                    while punct(i, b'.') && ident(i + 1).is_some() {
                        i += 2;
                    }
                    if ident(i) == Some("as") {
                        i += 2;
                    }
                    if punct(i, b',') {
                        i += 1;
                        continue;
                    }
                    break;
                }
                continue;
            }
            Tok::Ident(word) if at_start && word == "from" => {
                // from x.y import z; from . import z (relative: local)
                if punct(i + 1, b'.') {
                    i += 1;
                    continue;
                }
                if let Some(name) = ident(i + 1) {
                    if name != "import" {
                        push(top_python(name));
                    }
                }
                i += 1;
                continue;
            }
            Tok::Ident(word) if word == "__import__" || word == "import_module" => {
                if punct(i + 1, b'(') {
                    if let Some(Tok::Str(name)) = tokens.get(i + 2) {
                        if !name.starts_with('.') {
                            push(top_python(name));
                        }
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// JavaScript
// ---------------------------------------------------------------------------

fn lex_js(src: &[u8]) -> Vec<Tok> {
    let mut out: Vec<Tok> = Vec::new();
    let mut i = 0;
    while i < src.len() {
        let b = src[i];
        match b {
            b'/' if src.get(i + 1) == Some(&b'/') => {
                while i < src.len() && src[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if src.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < src.len() && !(src[i] == b'*' && src.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(src.len());
            }
            b'/' if regex_allowed(out.last()) => {
                // A regular expression literal: skip it, classes included.
                i += 1;
                let mut class = false;
                while i < src.len() && src[i] != b'\n' {
                    match src[i] {
                        b'\\' => i += 1,
                        b'[' => class = true,
                        b']' => class = false,
                        b'/' if !class => break,
                        _ => {}
                    }
                    i += 1;
                }
                i += 1;
                while i < src.len() && src[i].is_ascii_alphabetic() {
                    i += 1;
                }
                out.push(Tok::Str(String::new()));
            }
            b'\'' | b'"' => {
                let quote = b;
                i += 1;
                let start = i;
                while i < src.len() && src[i] != quote && src[i] != b'\n' {
                    if src[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                let end = i.min(src.len());
                out.push(Tok::Str(String::from_utf8_lossy(&src[start..end]).into_owned()));
                i = end + 1;
            }
            b'`' => {
                // A template literal; its `${...}` parts are skipped too.
                i += 1;
                let mut depth = 0usize;
                while i < src.len() {
                    match src[i] {
                        b'\\' => i += 1,
                        b'`' if depth == 0 => break,
                        b'$' if src.get(i + 1) == Some(&b'{') => {
                            depth += 1;
                            i += 1;
                        }
                        b'}' if depth > 0 => depth -= 1,
                        _ => {}
                    }
                    i += 1;
                }
                i += 1;
                out.push(Tok::Str(String::new()));
            }
            _ if is_ident_start(b) || b == b'$' => {
                let start = i;
                while i < src.len() && (is_ident(src[i]) || src[i] == b'$') {
                    i += 1;
                }
                out.push(Tok::Ident(
                    String::from_utf8_lossy(&src[start..i]).into_owned(),
                ));
            }
            _ if b.is_ascii_digit() => {
                while i < src.len() && (src[i].is_ascii_alphanumeric() || src[i] == b'.') {
                    i += 1;
                }
                out.push(Tok::Ident(String::new()));
            }
            _ if b.is_ascii_whitespace() => i += 1,
            _ => {
                out.push(Tok::Punct(b));
                i += 1;
            }
        }
    }
    out
}

/// Whether a `/` after `previous` starts a regular expression rather than
/// dividing: after an operator, an opening bracket, or a keyword.
fn regex_allowed(previous: Option<&Tok>) -> bool {
    match previous {
        None => true,
        Some(Tok::Punct(c)) => !matches!(c, b')' | b']' | b'}'),
        Some(Tok::Ident(word)) => matches!(
            word.as_str(),
            "return" | "typeof" | "case" | "do" | "else" | "in" | "of" | "new" | "delete"
                | "void" | "throw" | "yield" | "await"
        ),
        Some(Tok::Str(_)) | Some(Tok::Newline) => false,
    }
}

/// The package a JavaScript specifier names, or `None` for the run's own
/// files (relative or absolute paths) and URLs.
fn js_package(spec: &str) -> Option<String> {
    let spec = spec.trim();
    if spec.is_empty()
        || spec.starts_with('.')
        || spec.starts_with('/')
        || spec.contains("://")
        || spec.starts_with("data:")
    {
        return None;
    }
    let spec = spec.strip_prefix("node:").unwrap_or(spec);
    let mut parts = spec.split('/');
    let first = parts.next().unwrap_or_default();
    if first.starts_with('@') {
        let second = parts.next().unwrap_or_default();
        return Some(format!("{first}/{second}"));
    }
    Some(first.to_string())
}

fn js_imports(tokens: &[Tok], push: &mut impl FnMut(String)) {
    let mut push_spec = |spec: &str| {
        if let Some(name) = js_package(spec) {
            push(name);
        }
    };
    let punct = |i: usize, c: u8| tokens.get(i) == Some(&Tok::Punct(c));
    let is_member = |i: usize| i > 0 && punct(i - 1, b'.');
    let mut in_module_statement = false;
    for (i, tok) in tokens.iter().enumerate() {
        match tok {
            Tok::Punct(b';') => in_module_statement = false,
            Tok::Ident(word) if word == "import" && !is_member(i) => {
                if punct(i + 1, b'(') {
                    if let (Some(Tok::Str(spec)), true) = (tokens.get(i + 2), punct(i + 3, b')'))
                    {
                        push_spec(spec);
                    }
                } else if punct(i + 1, b'.') {
                    // import.meta
                } else if let Some(Tok::Str(spec)) = tokens.get(i + 1) {
                    push_spec(spec);
                } else {
                    in_module_statement = true;
                }
            }
            Tok::Ident(word) if word == "export" && !is_member(i) => in_module_statement = true,
            Tok::Ident(word) if word == "from" && in_module_statement => {
                if let Some(Tok::Str(spec)) = tokens.get(i + 1) {
                    push_spec(spec);
                    in_module_statement = false;
                }
            }
            Tok::Ident(word) if word == "require" && !is_member(i) => {
                if let (true, Some(Tok::Str(spec)), true) =
                    (punct(i + 1, b'('), tokens.get(i + 2), punct(i + 3, b')'))
                {
                    push_spec(spec);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod preflight_test {
    use super::*;

    fn py(source: &str) -> Vec<String> {
        imports(Language::Python, source.as_bytes())
    }

    fn js(source: &str) -> Vec<String> {
        imports(Language::JavaScript, source.as_bytes())
    }

    #[test]
    fn python_import_forms() {
        assert_eq!(py("import json\nimport os.path"), vec!["json", "os"]);
        assert_eq!(
            py("import numpy as np, pandas as pd\n"),
            vec!["numpy", "pandas"]
        );
        assert_eq!(py("from collections.abc import Mapping"), vec!["collections"]);
        assert_eq!(py("from torch import nn\n"), vec!["torch"]);
        assert_eq!(py("x = 1; import re"), vec!["re"]);
        assert_eq!(py("if True: import csv"), vec!["csv"]);
        assert_eq!(
            py("try:\n    import ujson as json\nexcept ImportError:\n    import json\n"),
            vec!["ujson", "json"]
        );
        assert_eq!(
            py("from os import (\n    path,\n    sep,\n)\nimport sys"),
            vec!["os", "sys"]
        );
    }

    #[test]
    fn python_dynamic_imports_need_a_literal() {
        assert_eq!(py("m = __import__('torch')"), vec!["torch"]);
        assert_eq!(
            py("import importlib\nm = importlib.import_module(\"scipy.stats\")"),
            vec!["importlib", "scipy"]
        );
        assert_eq!(py("m = __import__(name)"), Vec::<String>::new());
        assert_eq!(py("importlib.import_module('.sibling')"), Vec::<String>::new());
    }

    #[test]
    fn python_relative_imports_are_local() {
        assert_eq!(py("from . import helper\nfrom .util import x"), Vec::<String>::new());
        assert_eq!(py("from ..pkg import y"), Vec::<String>::new());
    }

    #[test]
    fn python_comments_and_strings_are_not_imports() {
        assert_eq!(py("# import torch\nx = 'import torch'"), Vec::<String>::new());
        assert_eq!(
            py("doc = \"\"\"\nimport torch\nfrom x import y\n\"\"\"\nimport json"),
            vec!["json"]
        );
        assert_eq!(py("s = r'\\'import torch'\n"), Vec::<String>::new());
        assert_eq!(py("print(f\"import {torch}\")"), Vec::<String>::new());
        assert_eq!(py("value.import_thing = 1"), Vec::<String>::new());
        // `import` not at a statement start is not a statement.
        assert_eq!(py("x = y.from_x\nprint(import_)"), Vec::<String>::new());
    }

    #[test]
    fn python_line_continuations_and_unterminated_strings() {
        assert_eq!(py("import json, \\\n    csv"), vec!["json", "csv"]);
        assert_eq!(py("x = 'unterminated\nimport re"), vec!["re"]);
        assert_eq!(py("x = '''never closed\nimport re"), Vec::<String>::new());
    }

    #[test]
    fn javascript_import_forms() {
        assert_eq!(js("import fs from 'node:fs';"), vec!["fs"]);
        assert_eq!(js("import { a, b } from \"lodash/fp\""), vec!["lodash"]);
        assert_eq!(js("import * as z from '@scope/pkg/sub'"), vec!["@scope/pkg"]);
        assert_eq!(js("import 'side-effect'"), vec!["side-effect"]);
        assert_eq!(js("const m = await import('sharp')"), vec!["sharp"]);
        assert_eq!(js("const x = require(\"crypto\")"), vec!["crypto"]);
        assert_eq!(js("export { y } from 'zod'"), vec!["zod"]);
        assert_eq!(
            js("import a from 'a'\nimport b from 'b'\n"),
            vec!["a", "b"]
        );
    }

    #[test]
    fn javascript_local_and_dynamic_specifiers() {
        assert_eq!(js("import h from './helper.mjs'"), Vec::<String>::new());
        assert_eq!(js("import x from '../x.js'; import y from '/abs.js'"), Vec::<String>::new());
        assert_eq!(js("import(name); require(variable)"), Vec::<String>::new());
        assert_eq!(js("import('https://esm.sh/x')"), Vec::<String>::new());
        assert_eq!(js("console.log(import.meta.url)"), Vec::<String>::new());
        assert_eq!(js("obj.require('x'); obj.import('y')"), Vec::<String>::new());
    }

    #[test]
    fn javascript_comments_strings_and_regexes_are_not_imports() {
        assert_eq!(js("// import x from 'x'\n/* require('y') */"), Vec::<String>::new());
        assert_eq!(js("const s = \"require('x')\""), Vec::<String>::new());
        assert_eq!(js("const t = `import x from 'x' ${\"require('y')\"}`"), Vec::<String>::new());
        assert_eq!(
            js("const re = /require\\('x'\\)/g; const q = a / b; require('ok')"),
            vec!["ok"]
        );
        assert_eq!(js("const r = /[/'\"]/; import z from 'zz'"), vec!["zz"]);
    }

    #[test]
    fn scan_reports_missing_against_the_image_and_the_runs_files() {
        let available = vec!["json".to_string(), "re".to_string()];
        let files = vec!["main.py".to_string(), "helper.py".to_string(), "pkg/__init__.py".to_string()];
        let source = "import json\nimport helper\nfrom pkg import x\nimport torch\nimport re";
        let scan = scan(Language::Python, source.as_bytes(), &available, &files);
        assert_eq!(scan.imports, vec!["json", "helper", "pkg", "torch", "re"]);
        assert_eq!(scan.missing, vec!["torch"]);
    }

    #[test]
    fn scan_is_capped_and_never_panics_on_garbage() {
        let mut source = "import json\n".to_string();
        source.push_str(&"x".repeat(MAX_SCAN_BYTES));
        source.push_str("\nimport torch\n");
        assert_eq!(py(&source), vec!["json"]);
        let garbage: Vec<u8> = (0..20_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        let _ = imports(Language::Python, &garbage);
        let _ = imports(Language::JavaScript, &garbage);
        let _ = imports(Language::JavaScript, b"/");
        let _ = imports(Language::JavaScript, b"'");
        let _ = imports(Language::Python, b"'''");
        let _ = imports(Language::Python, b"\\");
    }
}
