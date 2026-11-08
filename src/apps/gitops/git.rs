//! The `git` the gitops app runs: `ls-remote` to find the commit a ref names,
//! and a shallow fetch of that one commit (docs/gitops.md, "How a sync
//! works").
//!
//! Like the artifact fetcher (`src/artifacts/fetchers/git.rs`), it runs the
//! `git` binary and hands it a token through `GIT_CONFIG_*` environment
//! variables, so a secret never appears in a process's arguments or in a
//! log. Each command may speak only its URL's own transport
//! (`GIT_ALLOW_PROTOCOL`), never prompts, ignores the node's system and
//! global git config, and is killed when it runs past its timeout.

use std::path::Path;
use std::time::Duration;

use base64::Engine;

use crate::prelude::*;

/// How long resolving a ref may take.
pub const RESOLVE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long fetching one commit may take.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(120);

/// A repository gitops reads, with the token to read it with.
#[derive(Clone)]
pub struct Remote {
    url: String,
    scheme: String,
    /// `(username, token)`, sent as basic auth.
    credential: Option<(String, String)>,
    /// The git config file git reads as its global one (`/dev/null` when
    /// unset): a `file://` remote needs `safe.directory`, which git honors
    /// only from a protected scope, never from `GIT_CONFIG_*`.
    global_config: Option<std::path::PathBuf>,
}

/// The global git config a `file://` remote is read with: a local
/// repository belongs to another user than the app's own, and
/// `allowFileUrls` already said reading it is fine.
pub const FILE_REMOTE_CONFIG: &str = "[safe]\n\tdirectory = *\n";

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url)
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Why a URL is refused: a reason (a condition's) and a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlRefusal {
    pub reason: &'static str,
    pub message: String,
}

/// Check a Repo's URL: `https`, `http` (without credentials), or `file`
/// when the cluster allows it, and never with credentials inside it.
pub fn check_url(url: &str, has_credentials: bool, allow_file: bool) -> Result<String, UrlRefusal> {
    let parsed = url::Url::parse(url.trim()).map_err(|err| UrlRefusal {
        reason: "InvalidUrl",
        message: format!("url {url:?} is not a URL: {err}"),
    })?;
    let credentials_in_url = || {
        Err(UrlRefusal {
            reason: "CredentialsInUrl",
            message: "url carries credentials, which every reader of the Repo would see: put \
                      the token in KV and name its key in credentials.kv"
                .into(),
        })
    };
    let has_userinfo = !parsed.username().is_empty() || parsed.password().is_some();
    match parsed.scheme() {
        "https" | "http" if has_userinfo => return credentials_in_url(),
        "https" => {}
        "http" if !has_credentials => {}
        "http" => {
            return Err(UrlRefusal {
                reason: "InsecureCredentials",
                message: "credentials are sent only to an https:// URL".into(),
            })
        }
        "file" if allow_file => {}
        "file" => {
            return Err(UrlRefusal {
                reason: "FileUrlsDisabled",
                message: "file:// repositories are turned off on this cluster; an operator turns \
                          them on with `legion app config set gitops` and allowFileUrls = true"
                    .into(),
            })
        }
        other => {
            return Err(UrlRefusal {
                reason: "UnsupportedUrl",
                message: format!(
                    "{other}:// repositories are not supported: use an https:// URL (SSH is not \
                     supported yet)"
                ),
            })
        }
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(UrlRefusal {
            reason: "InvalidUrl",
            message: "url names the repository alone: put the branch, tag, or commit in ref".into(),
        });
    }
    Ok(parsed.scheme().to_string())
}

/// The line of git's stderr that says what went wrong: its first `fatal:`
/// or `error:` line (git explains the cause first, then adds generic
/// advice), else its last line.
pub fn failure_line(stderr: &str) -> String {
    let lines: Vec<&str> = stderr
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    lines
        .iter()
        .find_map(|line| {
            line.strip_prefix("fatal: ")
                .or_else(|| line.strip_prefix("error: "))
        })
        .or_else(|| lines.last().copied())
        .unwrap_or("it exited unsuccessfully")
        .to_string()
}

/// A full 40-character commit SHA.
pub fn is_full_sha(reference: &str) -> bool {
    reference.len() == 40 && reference.chars().all(|c| c.is_ascii_hexdigit())
}

/// The first 7 characters of a commit.
pub fn short(commit: &str) -> String {
    commit.chars().take(7).collect()
}

/// What `ls-remote` says `reference` names: a branch first, then a tag
/// (its commit, when the tag is annotated). `(commit, refname)`.
pub fn parse_ls_remote(output: &str, reference: &str) -> Option<(String, String)> {
    let mut branch = None;
    let mut tag = None;
    let mut peeled = None;
    let head = format!("refs/heads/{reference}");
    let tag_ref = format!("refs/tags/{reference}");
    let peeled_ref = format!("refs/tags/{reference}^{{}}");
    for line in output.lines() {
        let Some((commit, name)) = line.split_once('\t') else {
            continue;
        };
        let commit = commit.trim();
        if !is_full_sha(commit) {
            continue;
        }
        let name = name.trim();
        if name == head {
            branch = Some(commit.to_lowercase());
        } else if name == peeled_ref {
            peeled = Some(commit.to_lowercase());
        } else if name == tag_ref {
            tag = Some(commit.to_lowercase());
        }
    }
    branch
        .map(|commit| (commit, head))
        .or_else(|| peeled.or(tag).map(|commit| (commit, tag_ref)))
}

/// A fetched commit: its SHA and subject line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub commit: String,
    pub subject: String,
}

impl Remote {
    /// A remote for `url` (already checked with [`check_url`]), with
    /// `credential` (`(username, token)`) when the Repo has one.
    pub fn new(url: &str, scheme: &str, credential: Option<(String, String)>) -> Remote {
        Remote {
            url: url.trim().to_string(),
            scheme: scheme.to_string(),
            credential,
            global_config: None,
        }
    }

    /// Read `path` as git's global config (a `file://` remote's
    /// [`FILE_REMOTE_CONFIG`]).
    pub fn with_global_config(mut self, path: std::path::PathBuf) -> Remote {
        self.global_config = Some(path);
        self
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// A `git` command for this remote.
    fn command(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("git");
        command
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ALLOW_PROTOCOL", &self.scheme)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                self.global_config
                    .as_deref()
                    .unwrap_or(std::path::Path::new("/dev/null")),
            )
            .env("LC_ALL", "C")
            .kill_on_drop(true)
            .stdin(std::process::Stdio::null());
        let mut config: Vec<(&str, String)> = vec![("advice.detachedHead", "false".into())];
        if let Some((username, token)) = &self.credential {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{token}"));
            config.push((
                "http.extraHeader",
                format!("Authorization: Basic {encoded}"),
            ));
        }
        command.env("GIT_CONFIG_COUNT", config.len().to_string());
        for (index, (key, value)) in config.into_iter().enumerate() {
            command
                .env(format!("GIT_CONFIG_KEY_{index}"), key)
                .env(format!("GIT_CONFIG_VALUE_{index}"), value);
        }
        command
    }

    /// Run `git <args>` (in `dir`, when given) within `timeout`, returning
    /// stdout. A failure is git's last line of stderr, which never holds the
    /// token (it is in the environment, not the URL).
    async fn run(
        &self,
        action: &str,
        args: &[&str],
        dir: Option<&Path>,
        timeout: Duration,
    ) -> R<String> {
        let mut command = self.command();
        command.args(args);
        if let Some(dir) = dir {
            command.current_dir(dir);
        }
        let output = match tokio::time::timeout(timeout, command.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) if err.kind() == std::io::ErrorKind::NotFound => bail!(
                "git is not installed on this node: gitops runs the `git` binary, which must be \
                 on the PATH of the nodes it runs on"
            ),
            Ok(Err(err)) => bail!("git {action} could not start: {err}"),
            Err(_) => bail!(
                "git {action} of {} took longer than {}s",
                self.url,
                timeout.as_secs()
            ),
        };
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "git {action} of {} failed: {}",
                self.url,
                failure_line(&stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// The commit `reference` names now: a full SHA as it is, else the
    /// branch or tag `ls-remote` finds. `(commit, refname)`; the refname is
    /// what [`Remote::fetch`] fetches (the SHA itself for a SHA).
    pub async fn resolve(&self, reference: &str) -> R<(String, String)> {
        let reference = reference.trim();
        if is_full_sha(reference) {
            let commit = reference.to_lowercase();
            return Ok((commit.clone(), commit));
        }
        let heads = format!("refs/heads/{reference}");
        let tags = format!("refs/tags/{reference}");
        // An annotated tag's own object is not a commit: ask for the
        // commit it points at too.
        let peeled = format!("refs/tags/{reference}^{{}}");
        let output = self
            .run(
                "ls-remote",
                &["ls-remote", "--", &self.url, &heads, &tags, &peeled],
                None,
                RESOLVE_TIMEOUT,
            )
            .await?;
        parse_ls_remote(&output, reference)
            .ok_or_else(|| anyhow!("{} has no branch or tag named {reference:?}", self.url))
    }

    /// Fetch `refname` (from [`Remote::resolve`]) into `dest`, a directory
    /// that is created fresh, and check the commit out. Returns the commit
    /// fetched, which is newer than the one resolved when the ref moved in
    /// between, and its subject.
    pub async fn fetch(&self, refname: &str, dest: &Path) -> R<Fetched> {
        if dest.exists() {
            std::fs::remove_dir_all(dest)
                .with_context(|| format!("failed to clear {}", dest.display()))?;
        }
        std::fs::create_dir_all(dest)
            .with_context(|| format!("failed to create {}", dest.display()))?;
        self.run("init", &["init", "--quiet"], Some(dest), RESOLVE_TIMEOUT)
            .await?;
        self.run(
            "fetch",
            &[
                "fetch",
                "--quiet",
                "--depth",
                "1",
                "--no-tags",
                "--",
                &self.url,
                refname,
            ],
            Some(dest),
            FETCH_TIMEOUT,
        )
        .await?;
        let commit = self
            .run(
                "rev-parse",
                &["rev-parse", "FETCH_HEAD^{commit}"],
                Some(dest),
                RESOLVE_TIMEOUT,
            )
            .await?
            .trim()
            .to_lowercase();
        self.run(
            "checkout",
            &["checkout", "--quiet", "--force", "--detach", &commit],
            Some(dest),
            FETCH_TIMEOUT,
        )
        .await?;
        let subject = self
            .run(
                "log",
                &["log", "-1", "--format=%s", &commit],
                Some(dest),
                RESOLVE_TIMEOUT,
            )
            .await
            .map(|subject| subject.trim().chars().take(200).collect())
            .unwrap_or_default();
        Ok(Fetched { commit, subject })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_checked_test() {
        assert_eq!(
            check_url("https://github.com/acme/deploy", true, false).unwrap(),
            "https"
        );
        assert_eq!(
            check_url("http://git.local/deploy", false, false).unwrap(),
            "http"
        );
        assert_eq!(
            check_url("http://git.local/deploy", true, false)
                .unwrap_err()
                .reason,
            "InsecureCredentials"
        );
        assert_eq!(
            check_url("https://bob:secret@github.com/acme/deploy", false, false)
                .unwrap_err()
                .reason,
            "CredentialsInUrl"
        );
        assert_eq!(
            check_url("file:///srv/deploy", false, false)
                .unwrap_err()
                .reason,
            "FileUrlsDisabled"
        );
        assert_eq!(
            check_url("file:///srv/deploy", false, true).unwrap(),
            "file"
        );
        assert_eq!(
            check_url("ssh://git@github.com/acme/deploy", false, false)
                .unwrap_err()
                .reason,
            "UnsupportedUrl"
        );
        assert_eq!(
            check_url("https://github.com/acme/deploy?ref=main", false, false)
                .unwrap_err()
                .reason,
            "InvalidUrl"
        );
        assert_eq!(
            check_url("not a url", false, false).unwrap_err().reason,
            "InvalidUrl"
        );
    }

    #[test]
    fn ls_remote_prefers_the_branch_then_the_peeled_tag_test() {
        let a = "a".repeat(40);
        let b = "b".repeat(40);
        let c = "c".repeat(40);
        let output =
            format!("{a}\trefs/tags/v1\n{b}\trefs/tags/v1^{{}}\n{c}\trefs/heads/feature/v1\n");
        assert_eq!(
            parse_ls_remote(&output, "v1"),
            Some((b.clone(), "refs/tags/v1".to_string()))
        );
        let output = format!("{a}\trefs/tags/main\n{c}\trefs/heads/main\n");
        assert_eq!(
            parse_ls_remote(&output, "main"),
            Some((c, "refs/heads/main".to_string()))
        );
        let output = format!("{a}\trefs/tags/v2\n");
        assert_eq!(
            parse_ls_remote(&output, "v2"),
            Some((a, "refs/tags/v2".to_string()))
        );
        assert_eq!(parse_ls_remote("", "main"), None);
        assert!(is_full_sha(&"0".repeat(40)));
        assert!(!is_full_sha("main"));
        assert_eq!(short(&b), "bbbbbbb");
    }

    #[test]
    fn gits_first_fatal_line_is_the_failure_test() {
        let stderr = "fatal: detected dubious ownership in repository at '/srv/x/.git'\n\
                      To add an exception for this directory, call:\n\n\
                      fatal: Could not read from remote repository.\n\n\
                      Please make sure you have the correct access rights\n\
                      and the repository exists.\n";
        assert_eq!(
            failure_line(stderr),
            "detected dubious ownership in repository at '/srv/x/.git'"
        );
        assert_eq!(failure_line("remote: nope\n"), "remote: nope");
        assert_eq!(failure_line(""), "it exited unsuccessfully");
    }

    #[test]
    fn the_token_stays_out_of_debug_output_test() {
        let remote = Remote::new(
            "https://github.com/acme/deploy",
            "https",
            Some(("x-access-token".into(), "ghp_secret".into())),
        );
        assert!(!format!("{remote:?}").contains("ghp_secret"));
    }
}
