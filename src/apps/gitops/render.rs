//! Turning a fetched commit into jobspecs (docs/gitops.md, "What a Repo
//! reads").
//!
//! Every `.ncl` and `.json` file directly in the Repo's `path` is a jobspec,
//! or a list of them, in name order; a name starting with `.` or `_` is
//! skipped, and subdirectories are not read (shared Nickel lives there and
//! is imported). Each file is evaluated with
//! [`crate::config_lang::eval_file_confined`], so its imports stay inside
//! the checkout, and, in the app, in a child process
//! (`legion:self app gitops-render`) that is killed after
//! [`RENDER_TIMEOUT`]: a repository's author can write a Nickel program
//! that never ends, and it must not take the app with it (gitops plan
//! calls entry 6).

use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::prelude::*;
use crate::structs::Submission;
use serde::{Deserialize, Serialize};

/// The most jobspec files a Repo's `path` may hold.
pub const MAX_FILES: usize = 200;
/// How long one file may take to render.
pub const RENDER_TIMEOUT: Duration = Duration::from_secs(10);
/// The most a rendered file may print.
pub const MAX_RENDER_OUTPUT: usize = 16 * 1024 * 1024;

/// One source file an evaluation read, relative to the repository root.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RenderedSource {
    pub path: String,
    pub content: String,
    pub primary: bool,
}

/// What rendering one file produced: its value, and the sources it read.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RenderedFile {
    /// The file, relative to the repository root.
    pub file: String,
    pub value: Value,
    pub sources: Vec<RenderedSource>,
}

/// The child process's answer on stdout.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RenderAnswer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rendered: Option<RenderedFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// One job a commit defines.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedJob {
    /// The file it came from, relative to the repository root.
    pub file: String,
    /// Its bare name.
    pub name: String,
    /// The namespace the jobspec names, if it names one (in `namespace`, or
    /// as `name = "<namespace>/<name>"`).
    pub namespace: Option<String>,
    /// The jobspec, as `RunJob` takes it.
    pub spec: Value,
    /// A digest of the jobspec: a changed digest is a changed job.
    pub digest: String,
    /// The files it was rendered from, as `RunJob` records them.
    pub submissions: Vec<Submission>,
}

/// How the app renders files.
#[derive(Debug, Clone)]
pub enum Renderer {
    /// In this process: tests, and the child process itself.
    InProcess,
    /// In a child process of this binary, killed after [`RENDER_TIMEOUT`].
    Subprocess { exe: PathBuf },
}

impl Renderer {
    /// The app's renderer: this binary, run as `app gitops-render`.
    pub fn subprocess() -> R<Renderer> {
        Ok(Renderer::Subprocess {
            exe: std::env::current_exe().context("failed to find the legion binary")?,
        })
    }

    /// Render `file` of the checkout at `root`.
    pub async fn render(&self, root: &Path, file: &Path) -> R<RenderedFile> {
        match self {
            Renderer::InProcess => {
                let (root, file) = (root.to_path_buf(), file.to_path_buf());
                tokio::task::spawn_blocking(move || render_file(&root, &file))
                    .await
                    .context("the renderer panicked")?
            }
            Renderer::Subprocess { exe } => render_in_child(exe, root, file).await,
        }
    }
}

async fn render_in_child(exe: &Path, root: &Path, file: &Path) -> R<RenderedFile> {
    let shown = relative(root, file);
    let mut command = tokio::process::Command::new(exe);
    command
        .arg("app")
        .arg(super::RENDER_COMMAND)
        .arg("--root")
        .arg(root)
        .arg(file)
        // The child needs nothing from the app's environment: not its API
        // socket, not its config.
        .env_clear()
        .env("RUST_LOG", "error")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(RENDER_TIMEOUT, command.output()).await {
        Ok(output) => output.context("failed to start the renderer")?,
        Err(_) => bail!(
            "{shown} took longer than {}s to evaluate",
            RENDER_TIMEOUT.as_secs()
        ),
    };
    if output.stdout.len() > MAX_RENDER_OUTPUT {
        bail!("{shown} rendered to more than {MAX_RENDER_OUTPUT} bytes");
    }
    let answer: RenderAnswer = serde_json::from_slice(&output.stdout).map_err(|_| {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let line = stderr
            .lines()
            .map(str::trim)
            .rfind(|line| !line.is_empty())
            .unwrap_or("no output");
        anyhow!("{shown}: the renderer failed ({}): {line}", output.status)
    })?;
    match (answer.rendered, answer.error) {
        (Some(rendered), None) => Ok(rendered),
        (_, Some(error)) => Err(anyhow!("{error}")),
        (None, None) => bail!("{shown}: the renderer answered nothing"),
    }
}

/// `legion app gitops-render --root <root> <file>`: render one file and
/// print the answer as JSON on stdout. Errors are part of the answer.
pub fn render_command(root: &Path, file: &Path) {
    let answer = match render_file(root, file) {
        Ok(rendered) => RenderAnswer {
            rendered: Some(rendered),
            error: None,
        },
        Err(err) => RenderAnswer {
            rendered: None,
            error: Some(format!("{err:#}")),
        },
    };
    println!(
        "{}",
        serde_json::to_string(&answer).unwrap_or_else(|_| "{}".to_string())
    );
}

fn relative(root: &Path, file: &Path) -> String {
    let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    file.strip_prefix(&root)
        .or_else(|_| file.strip_prefix(root.as_path()))
        .unwrap_or(file)
        .to_string_lossy()
        .to_string()
}

/// Render `file` of the checkout at `root` in this process.
pub fn render_file(root: &Path, file: &Path) -> R<RenderedFile> {
    let (json, sources) = crate::config_lang::eval_file_confined(file, root)?;
    let value: Value = serde_json::from_str(&json).context("the file did not render to JSON")?;
    let file = sources
        .iter()
        .find(|source| source.is_primary)
        .map(|source| source.path.clone())
        .unwrap_or_else(|| relative(root, file));
    Ok(RenderedFile {
        file,
        value,
        sources: sources
            .into_iter()
            .map(|source| RenderedSource {
                path: source.path,
                content: source.content,
                primary: source.is_primary,
            })
            .collect(),
    })
}

/// The jobspec files of `path` in the checkout at `root`, in name order.
pub fn discover(root: &Path, path: &str) -> R<Vec<PathBuf>> {
    let root = std::fs::canonicalize(root)
        .with_context(|| format!("failed to read the checkout {}", root.display()))?;
    let trimmed = path.trim().trim_matches('/');
    let dir = if trimmed.is_empty() || trimmed == "." {
        root.clone()
    } else {
        root.join(trimmed)
    };
    let dir = std::fs::canonicalize(&dir)
        .map_err(|_| anyhow!("path {path:?} does not exist in the repository"))?;
    if !dir.starts_with(&root) {
        bail!("path {path:?} leads outside the repository");
    }
    if !dir.is_dir() {
        bail!("path {path:?} is not a directory");
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir).with_context(|| format!("failed to read {path:?}"))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.starts_with('_') {
            continue;
        }
        let candidate = entry.path();
        if !crate::config_lang::is_config_file(&candidate) || candidate.is_dir() {
            continue;
        }
        files.push(candidate);
    }
    files.sort();
    if files.len() > MAX_FILES {
        bail!(
            "path {path:?} holds {} jobspec files; a Repo reads at most {MAX_FILES}",
            files.len()
        );
    }
    Ok(files)
}

/// The jobs one rendered file defines: an object is one jobspec, a list is
/// several.
pub fn jobs_of(rendered: &RenderedFile) -> R<Vec<RenderedJob>> {
    let specs: Vec<&Value> = match &rendered.value {
        Value::Object(_) => vec![&rendered.value],
        Value::Array(items) => items.iter().collect(),
        _ => bail!(
            "{} is not a jobspec: it renders to {}, not a record or a list of records",
            rendered.file,
            kind_of(&rendered.value)
        ),
    };
    let submissions: Vec<Submission> = rendered
        .sources
        .iter()
        .map(|source| Submission {
            path: source.path.clone(),
            content: source.content.clone(),
            is_primary: source.primary,
        })
        .collect();
    let mut jobs = Vec::new();
    for (index, spec) in specs.into_iter().enumerate() {
        let at = if rendered.value.is_array() {
            format!("{}[{index}]", rendered.file)
        } else {
            rendered.file.clone()
        };
        if !spec.is_object() {
            bail!("{at} is not a jobspec: it is {}", kind_of(spec));
        }
        let Some(full) = spec
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
        else {
            bail!("{at} has no name: every jobspec names its job");
        };
        let (named_namespace, name) = match full.split_once('/') {
            Some((namespace, name)) => (Some(namespace.to_string()), name.to_string()),
            None => (None, full.to_string()),
        };
        let field_namespace = spec
            .get("namespace")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|namespace| !namespace.is_empty())
            .map(str::to_string);
        jobs.push(RenderedJob {
            file: rendered.file.clone(),
            name,
            namespace: field_namespace.or(named_namespace),
            digest: digest(spec),
            spec: spec.clone(),
            submissions: submissions.clone(),
        });
    }
    Ok(jobs)
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "a record",
    }
}

/// A short digest of a jobspec (16 hex characters of its SHA-256).
pub fn digest(spec: &Value) -> String {
    let bytes = serde_json::to_vec(spec).unwrap_or_default();
    Sha256::digest(&bytes)
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Render every jobspec file of `path` in the checkout at `root`: the jobs,
/// in file order. Every file that fails is reported (up to five), so one
/// sync shows a commit's problems together.
pub async fn render_tree(renderer: &Renderer, root: &Path, path: &str) -> R<Vec<RenderedJob>> {
    let files = discover(root, path)?;
    let mut jobs = Vec::new();
    let mut errors = Vec::new();
    for file in &files {
        match renderer
            .render(root, file)
            .await
            .and_then(|rendered| jobs_of(&rendered))
        {
            Ok(mut found) => jobs.append(&mut found),
            Err(err) => errors.push(format!("{err:#}")),
        }
    }
    match errors.len() {
        0 => Ok(jobs),
        1 => Err(anyhow!("{}", errors[0])),
        n => {
            let shown: Vec<String> = errors.into_iter().take(5).collect();
            Err(anyhow!("{n} files failed to render: {}", shown.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn discovery_reads_one_directory_in_name_order_test() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "jobs/b.ncl", "{ name = \"b\" }");
        write(root, "jobs/a.json", "{\"name\": \"a\"}");
        write(root, "jobs/_partial.ncl", "{}");
        write(root, "jobs/.hidden.ncl", "{}");
        write(root, "jobs/README.md", "docs");
        write(root, "jobs/lib/common.ncl", "{}");
        let files = discover(root, "jobs").unwrap();
        let names: Vec<String> = files
            .iter()
            .map(|file| file.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["a.json", "b.ncl"]);
        assert_eq!(discover(root, "jobs/").unwrap().len(), 2);
        let err = discover(root, "nope").unwrap_err().to_string();
        assert!(err.contains("does not exist"), "{err}");
        let err = discover(root, "jobs/b.ncl").unwrap_err().to_string();
        assert!(err.contains("not a directory"), "{err}");
        // The root itself.
        write(root, "top.ncl", "{ name = \"top\" }");
        assert_eq!(discover(root, ".").unwrap().len(), 1);
    }

    #[test]
    fn files_render_to_one_job_or_a_list_test() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "lib/base.ncl", "{ runtime = \"noop\" }");
        write(
            root,
            "web.ncl",
            "let base = import \"lib/base.ncl\" in { name = \"web\", tasks = [base & { name = \"t\" }] }",
        );
        write(
            root,
            "pair.ncl",
            "[{ name = \"api\" }, { name = \"team-a/worker\" }, { name = \"x\", namespace = \"prod\" }]",
        );
        let rendered = render_file(root, &root.join("web.ncl")).unwrap();
        assert_eq!(rendered.file, "web.ncl");
        let jobs = jobs_of(&rendered).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "web");
        assert_eq!(jobs[0].namespace, None);
        assert_eq!(jobs[0].submissions.len(), 2);
        assert!(jobs[0]
            .submissions
            .iter()
            .any(|s| s.path == "lib/base.ncl" && !s.is_primary));
        assert_eq!(jobs[0].digest.len(), 16);
        let jobs = jobs_of(&render_file(root, &root.join("pair.ncl")).unwrap()).unwrap();
        let named: Vec<(&str, Option<&str>)> = jobs
            .iter()
            .map(|job| (job.name.as_str(), job.namespace.as_deref()))
            .collect();
        assert_eq!(
            named,
            vec![
                ("api", None),
                ("worker", Some("team-a")),
                ("x", Some("prod"))
            ]
        );
    }

    #[test]
    fn a_file_that_is_not_a_jobspec_says_why_test() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "n.ncl", "42");
        write(root, "anon.ncl", "{ tasks = [] }");
        let err = jobs_of(&render_file(root, &root.join("n.ncl")).unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("n.ncl is not a jobspec") && err.contains("a number"),
            "{err}"
        );
        let err = jobs_of(&render_file(root, &root.join("anon.ncl")).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("anon.ncl has no name"), "{err}");
    }

    #[tokio::test]
    async fn a_tree_reports_every_broken_file_test() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.ncl", "{ name = 1 + \"x\" }");
        write(root, "b.ncl", "{ name = \"b\" }");
        write(root, "c.ncl", "import \"/etc/hostname\" as 'Text");
        let err = render_tree(&Renderer::InProcess, root, ".")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("2 files failed to render"), "{err}");
        assert!(
            err.contains("a.ncl") && err.contains("absolute path"),
            "{err}"
        );
        std::fs::remove_file(root.join("a.ncl")).unwrap();
        std::fs::remove_file(root.join("c.ncl")).unwrap();
        let jobs = render_tree(&Renderer::InProcess, root, ".").await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn digests_follow_the_spec_test() {
        let a = json!({ "name": "web", "count": 1 });
        let b = json!({ "name": "web", "count": 2 });
        assert_eq!(digest(&a), digest(&a.clone()));
        assert_ne!(digest(&a), digest(&b));
    }
}
