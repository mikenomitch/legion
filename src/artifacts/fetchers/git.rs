//! `git` fetcher: shallow clones with `ref=` query support
//! (`git::https://github.com/acme/repo?ref=v1.2.0`) and `resolve()` via
//! `git ls-remote` so refs pin to commit SHAs at submit.
//!
//! Credentials are delivered through `GIT_CONFIG_*` environment variables
//! (an `http.extraHeader` Authorization header) so they never appear in
//! process arguments or logs.

use std::path::Path;
use std::process::Command;

use base64::Engine;
use url::Url;

use crate::artifacts::fetcher::{
    ArtifactFetcher, Credential, FetchRequest, FetchedArtifact, FetchedPayload, ResolvedIdentity,
};
use crate::artifacts::stage::ArtifactSink;
use crate::prelude::*;

#[derive(Debug, Default)]
pub struct GitFetcher;

impl GitFetcher {
    pub fn new() -> Self {
        Self
    }
}

/// Clone URL with the `ref`/`depth` query params stripped (they direct the
/// fetcher, not the server) and any userinfo removed.
fn clone_url(url: &Url) -> Url {
    let mut clean = url.clone();
    clean.set_query(None);
    clean.set_fragment(None);
    let _ = clean.set_username("");
    let _ = clean.set_password(None);
    clean
}

fn requested_ref(url: &Url) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == "ref")
        .map(|(_, value)| value.into_owned())
}

fn is_full_sha(reference: &str) -> bool {
    reference.len() == 40 && reference.chars().all(|c| c.is_ascii_hexdigit())
}

/// Transports a git artifact may name. `ext::` and `fd::` run commands or
/// read descriptors, so they never are.
const GIT_TRANSPORTS: &[&str] = &["https", "http", "ssh", "git", "file"];

/// Builds a `git` command for `url` with credentials injected via
/// environment-scoped config so secrets stay out of argv, allowed to speak
/// only the URL's own transport (`GIT_ALLOW_PROTOCOL`), so a remote cannot
/// switch the fetch to another one.
fn git_command(url: &Url, credential: Option<&Credential>) -> R<Command> {
    let transport = url.scheme();
    if !GIT_TRANSPORTS.contains(&transport) {
        bail!("git artifacts do not support the {transport:?} transport");
    }
    let mut cmd = Command::new("git");
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    cmd.env("GIT_ALLOW_PROTOCOL", transport);
    match credential {
        None => {}
        Some(Credential::Basic { username, password }) => {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            cmd.env("GIT_CONFIG_COUNT", "1");
            cmd.env("GIT_CONFIG_KEY_0", "http.extraHeader");
            cmd.env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Basic {encoded}"),
            );
        }
        Some(Credential::Bearer { token }) => {
            cmd.env("GIT_CONFIG_COUNT", "1");
            cmd.env("GIT_CONFIG_KEY_0", "http.extraHeader");
            cmd.env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Bearer {token}"),
            );
        }
        Some(other) => bail!("git artifacts do not support {other:?}"),
    }
    Ok(cmd)
}

fn run_git(mut cmd: Command, action: &str, redacted_source: &str) -> R<std::process::Output> {
    let output = cmd
        .output()
        .with_context(|| format!("spawn git for {redacted_source}"))?;
    if !output.status.success() {
        // git stderr can echo URLs; it never sees credentials (env-injected),
        // but trim it to one line to keep errors readable.
        let stderr = String::from_utf8_lossy(&output.stderr);
        let first_line = stderr.lines().last().unwrap_or("").trim();
        bail!("git {action} failed for {redacted_source}: {first_line}");
    }
    Ok(output)
}

fn shallow_clone(
    url: &Url,
    reference: Option<&str>,
    dest: &Path,
    credential: Option<&Credential>,
    redacted_source: &str,
) -> R<()> {
    let clean = clone_url(url);
    match reference {
        // A full SHA cannot be cloned shallowly by name on all servers;
        // init + fetch the exact object instead.
        Some(sha) if is_full_sha(sha) => {
            std::fs::create_dir_all(dest)?;
            let mut init = git_command(&clean, credential)?;
            init.arg("init").arg("--quiet").arg(dest);
            run_git(init, "init", redacted_source)?;

            let mut fetch = git_command(&clean, credential)?;
            fetch
                .current_dir(dest)
                .arg("fetch")
                .arg("--quiet")
                .arg("--depth")
                .arg("1")
                .arg(clean.as_str())
                .arg(sha);
            run_git(fetch, "fetch", redacted_source)?;

            let mut checkout = git_command(&clean, credential)?;
            checkout
                .current_dir(dest)
                .arg("checkout")
                .arg("--quiet")
                .arg("FETCH_HEAD");
            run_git(checkout, "checkout", redacted_source)?;
            Ok(())
        }
        Some(branch_or_tag) => {
            let mut clone = git_command(&clean, credential)?;
            clone
                .arg("clone")
                .arg("--quiet")
                .arg("--depth")
                .arg("1")
                .arg("--branch")
                .arg(branch_or_tag)
                .arg(clean.as_str())
                .arg(dest);
            run_git(clone, "clone", redacted_source)?;
            Ok(())
        }
        None => {
            let mut clone = git_command(&clean, credential)?;
            clone
                .arg("clone")
                .arg("--quiet")
                .arg("--depth")
                .arg("1")
                .arg(clean.as_str())
                .arg(dest);
            run_git(clone, "clone", redacted_source)?;
            Ok(())
        }
    }
}

fn dir_size(dir: &Path) -> R<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

impl ArtifactFetcher for GitFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["git"]
    }

    /// `git ls-remote` the requested ref to a commit SHA. Returns `None` for
    /// already-pinned (full SHA) refs.
    fn resolve(&self, req: &FetchRequest<'_>) -> R<Option<ResolvedIdentity>> {
        let reference = requested_ref(req.url);
        if reference.as_deref().is_some_and(is_full_sha) {
            return Ok(None);
        }
        let clean = clone_url(req.url);
        let mut cmd = git_command(&clean, req.credential)?;
        cmd.arg("ls-remote").arg(clean.as_str());
        let pattern = reference.clone().unwrap_or_else(|| "HEAD".to_string());
        cmd.arg(&pattern);
        let output = run_git(cmd, "ls-remote", req.redacted_source)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Prefer the peeled tag object (`refs/tags/x^{}`) when present: that
        // is the commit the tag points at.
        let mut resolved: Option<String> = None;
        for line in stdout.lines() {
            let Some((sha, name)) = line.split_once('\t') else {
                continue;
            };
            if name.ends_with("^{}") {
                resolved = Some(sha.to_string());
                break;
            }
            if resolved.is_none() {
                resolved = Some(sha.to_string());
            }
        }
        let sha = resolved.with_context(|| {
            format!(
                "git ls-remote found no ref {pattern:?} at {}",
                req.redacted_source
            )
        })?;

        let mut pinned = req.url.clone();
        let other_params: Vec<(String, String)> = pinned
            .query_pairs()
            .filter(|(key, _)| key != "ref")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        pinned.set_query(None);
        {
            let mut qp = pinned.query_pairs_mut();
            for (key, value) in &other_params {
                qp.append_pair(key, value);
            }
            qp.append_pair("ref", &sha);
        }
        Ok(Some(ResolvedIdentity {
            pinned_source: format!("git::{pinned}"),
            digest: sha,
        }))
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let reference = requested_ref(req.url);
        let tree_dir = sink.dir().join("repo");
        shallow_clone(
            req.url,
            reference.as_deref(),
            &tree_dir,
            req.credential,
            req.redacted_source,
        )?;
        // The .git directory is metadata, not artifact content; drop it so
        // tasks get a clean worktree (and the size budget reflects content).
        let _ = std::fs::remove_dir_all(tree_dir.join(".git"));
        sink.record_external_bytes(dir_size(&tree_dir)?)?;
        Ok(FetchedArtifact {
            bytes: sink.bytes_written(),
            payload: FetchedPayload::Tree { dir: tree_dir },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::fetcher::FetchLimits;
    use crate::structs::parse_artifact_source;
    use tempfile::tempdir;

    fn local_repo() -> R<(tempfile::TempDir, String)> {
        let dir = tempdir()?;
        let run = |args: &[&str]| -> R<()> {
            let output = Command::new("git")
                // Host-level signing/hooks config must not leak into the
                // fixture repo.
                .args([
                    "-c",
                    "commit.gpgsign=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                ])
                .args(args)
                .current_dir(dir.path())
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@t")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@t")
                .output()
                .context("run git")?;
            if !output.status.success() {
                bail!(
                    "git {args:?} failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Ok(())
        };
        run(&["init", "--quiet", "--initial-branch=main", "."])?;
        std::fs::write(dir.path().join("tool.sh"), "#!/bin/sh\necho hi\n")?;
        run(&["add", "."])?;
        run(&["commit", "--quiet", "-m", "init"])?;
        let url = format!("file://{}", dir.path().display());
        Ok((dir, url))
    }

    #[test]
    fn clones_and_strips_git_dir() -> R<()> {
        let (_repo, url) = local_repo()?;
        let source = format!("git::{url}");
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let staging = tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().join("sink"), 10 * 1024 * 1024)?;
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: &source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };
        let fetched = GitFetcher::new().fetch(&request, &mut sink)?;
        match fetched.payload {
            FetchedPayload::Tree { dir } => {
                assert!(dir.join("tool.sh").exists());
                assert!(!dir.join(".git").exists(), ".git must be stripped");
            }
            other => panic!("expected tree, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn resolve_pins_ref_to_commit_sha() -> R<()> {
        let (_repo, url) = local_repo()?;
        let source = format!("git::{url}?ref=main");
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: &source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };
        let resolved = GitFetcher::new()
            .resolve(&request)?
            .expect("branch resolves");
        assert!(is_full_sha(&resolved.digest), "{}", resolved.digest);
        assert!(
            resolved
                .pinned_source
                .contains(&format!("ref={}", resolved.digest)),
            "{}",
            resolved.pinned_source
        );

        // Pinned refs need no resolution.
        let pinned = parse_artifact_source(&resolved.pinned_source)?;
        let request = FetchRequest {
            scheme: &pinned.scheme,
            url: &pinned.url,
            redacted_source: &resolved.pinned_source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };
        assert!(GitFetcher::new().resolve(&request)?.is_none());
        Ok(())
    }
}
