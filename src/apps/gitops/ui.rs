//! gitops's UI, as app views (docs/gitops.md, "In the UI"; docs/apps.md,
//! "Views and actions"). The manifest registers the slots; these handlers
//! answer with `legion-view/1` documents, which the SPA, `legion app view`,
//! and the `legion_app_view` MCP tool render:
//!
//! - **`repos`**: the GitOps page in the sidebar: every Repo in a namespace
//!   where the viewer holds `job:read`, with its state, ref, commit, and jobs.
//! - **`job`**: the GitOps panel on a job a Repo deployed: the repository,
//!   ref, commit, file, the job's state, and Sync now. Hidden on other jobs.
//! - **`sync`**: the Sync now action (`job:write` on the job): it wakes the
//!   deploying Repo's reconcile at once ([`crate::apps::objects::Reconciler::wake`]).
//!
//! Everything is drawn from the app's own memory ([`super::Book`]): no view
//! makes a Legion call.

use std::collections::BTreeMap;
use std::sync::RwLock;

use super::sync::Health;
use super::{Book, BookEntry, JobState, QUALIFIED_KIND};
use crate::apps::objects::ObjectCache;
use crate::apps::ui::{ActionOutcome, Refused, ViewRequest};
use crate::apps::AppContext;
use crate::prelude::*;
use crate::structs::app_ui::{
    ActionButton, ActionsBlock, AppView, Block, Cell, Column, ColumnType, KeyValueBlock,
    KeyValueItem, MarkdownBlock, NoticeBlock, RichText, Stat, StatsBlock, TableBlock, Tone,
};

/// Register the views and the action.
pub fn register(
    context: &AppContext,
    book: Arc<Book>,
    live: Arc<RwLock<ObjectCache>>,
    wake: tokio::sync::mpsc::UnboundedSender<u64>,
) {
    {
        let (book, live) = (book.clone(), live.clone());
        context.view("repos", move |request| {
            let entries = visible(&book, &live);
            async move { Ok(repos_view(&request, entries)) }
        });
    }
    {
        let (book, live) = (book.clone(), live.clone());
        context.view("job", move |request| {
            let entries = visible(&book, &live);
            async move { Ok(job_view(&request, &entries)) }
        });
    }
    context.action("sync", move |request| {
        let entries = visible(&book, &live);
        let wake = wake.clone();
        async move {
            let namespace = namespace_of(&request.context.namespace);
            let Some(entry) = deploying(&entries, &namespace, &request.context.job) else {
                return Err(Refused(format!(
                    "{} was not deployed by a gitops Repo",
                    request.context.job
                ))
                .into());
            };
            wake.send(entry.repo.uid)
                .map_err(|_| Refused("gitops is stopping; try again in a moment".into()))?;
            info!(
                repo = %entry.repo.id(),
                viewer = %request.viewer.principal,
                "Sync requested from the UI"
            );
            Ok(ActionOutcome::done(format!(
                "Syncing Repo {} now: gitops checks {} for a new commit and deploys it",
                entry.repo.id(),
                entry.spec.url
            )))
        }
    });
}

/// The Repos that exist now (a Repo deleted with `--now` never reached the
/// app's cleanup), from what the app last reported.
fn visible(book: &Book, live: &RwLock<ObjectCache>) -> Vec<BookEntry> {
    let entries = book.entries();
    let Ok(live) = live.read() else {
        return entries;
    };
    if !live.synced() {
        return entries;
    }
    entries
        .into_iter()
        .filter(|entry| live.get(entry.repo.uid).is_some())
        .collect()
}

fn namespace_of(namespace: &str) -> String {
    if namespace.is_empty() {
        crate::structs::namespace::DEFAULT_NAMESPACE.to_string()
    } else {
        namespace.to_string()
    }
}

fn deploying<'a>(entries: &'a [BookEntry], namespace: &str, job: &str) -> Option<&'a BookEntry> {
    entries.iter().find(|entry| {
        entry.repo.namespace == namespace && entry.status.jobs.iter().any(|j| j.name == job)
    })
}

fn segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn job_route(namespace: &str, job: &str) -> String {
    format!("/jobs/{}/{}", segment(namespace), segment(job))
}

fn verdict_tone(entry: &BookEntry) -> Tone {
    match entry.verdict.health {
        Health::Ready => Tone::Ok,
        Health::Progressing => Tone::Info,
        Health::NotReady if entry.verdict.reason == "Suspended" => Tone::Neutral,
        Health::NotReady => Tone::Warn,
        Health::Rejected => Tone::Danger,
    }
}

fn state_tone(state: JobState) -> Tone {
    match state {
        JobState::Deployed => Tone::Ok,
        JobState::Progressing | JobState::Pending => Tone::Info,
        JobState::Stopped => Tone::Neutral,
        JobState::Failed => Tone::Danger,
    }
}

/// A link to the repository, when it is a web URL.
fn repo_href(url: &str) -> Option<String> {
    (url.starts_with("https://") || url.starts_with("http://"))
        .then(|| url.trim_end_matches(".git").to_string())
}

fn commit_text(entry: &BookEntry) -> String {
    match (&entry.status.short, &entry.status.subject) {
        (Some(short), Some(subject)) => format!("{short} {subject}"),
        (Some(short), None) => short.clone(),
        _ => "-".to_string(),
    }
}

/// The GitOps page.
fn repos_view(request: &ViewRequest, entries: Vec<BookEntry>) -> AppView {
    let entries: Vec<BookEntry> = entries
        .into_iter()
        .filter(|entry| request.viewer.sees_namespace(&entry.repo.namespace))
        .collect();
    if entries.is_empty() {
        return AppView::new(vec![Block::Markdown(MarkdownBlock {
            text: format!(
                "No Repos yet. A **Repo** points gitops at a git repository: every `.ncl` and \
                 `.json` jobspec in its `path` is deployed into the Repo's namespace, and kept \
                 deployed as the repository changes.\n\n\
                 ```\nlegion gitops add web --url https://github.com/acme/deploy --path jobs --wait\n```\n\n\
                 Or write a `{QUALIFIED_KIND}` object and `legion object apply -f repo.ncl`. \
                 See [GitOps](https://github.com/mikenomitch/legion/blob/main/docs/gitops.md)."
            ),
        })]);
    }
    let ready = entries
        .iter()
        .filter(|entry| entry.verdict.health == Health::Ready)
        .count();
    let jobs: u32 = entries.iter().map(|entry| entry.status.deployed).sum();
    let mut blocks = vec![Block::Stats(StatsBlock {
        title: None,
        items: vec![
            Stat {
                label: "Repos".into(),
                value: entries.len().to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Ready".into(),
                value: ready.to_string(),
                tone: Some(if ready == entries.len() {
                    Tone::Ok
                } else {
                    Tone::Neutral
                }),
                hint: None,
            },
            Stat {
                label: "Not ready".into(),
                value: (entries.len() - ready).to_string(),
                tone: Some(if ready == entries.len() {
                    Tone::Neutral
                } else {
                    Tone::Warn
                }),
                hint: None,
            },
            Stat {
                label: "Jobs deployed".into(),
                value: jobs.to_string(),
                tone: None,
                hint: None,
            },
        ],
    })];
    let rows = entries
        .iter()
        .map(|entry| {
            let mut row = BTreeMap::new();
            row.insert(
                "namespace".to_string(),
                Cell::Text(entry.repo.namespace.clone()),
            );
            row.insert(
                "repo".to_string(),
                Cell::Rich(RichText {
                    text: entry.repo.name.clone(),
                    route: None,
                    href: repo_href(&entry.spec.url),
                    tone: None,
                }),
            );
            row.insert(
                "status".to_string(),
                Cell::Rich(RichText {
                    text: entry.verdict.label(),
                    route: None,
                    href: None,
                    tone: Some(verdict_tone(entry)),
                }),
            );
            row.insert("ref".to_string(), Cell::Text(entry.spec.reference.clone()));
            row.insert("commit".to_string(), Cell::Text(commit_text(entry)));
            row.insert(
                "jobs".to_string(),
                Cell::Number(entry.status.deployed as f64),
            );
            row.insert(
                "checked".to_string(),
                Cell::Number((entry.checked_ms / 1000) as f64),
            );
            row.insert(
                "detail".to_string(),
                Cell::Text(entry.verdict.message.clone()),
            );
            row
        })
        .collect();
    blocks.push(Block::Table(TableBlock {
        title: Some("Repos".into()),
        columns: vec![
            column("namespace", "Namespace", ColumnType::Text),
            column("repo", "Repo", ColumnType::Text),
            column("status", "Status", ColumnType::Text),
            column("ref", "Ref", ColumnType::Text),
            column("commit", "Commit", ColumnType::Text),
            column("jobs", "Jobs", ColumnType::Number),
            column("checked", "Checked", ColumnType::Time),
            column("detail", "Detail", ColumnType::Text),
        ],
        rows,
        empty: None,
    }));
    let job_rows: Vec<BTreeMap<String, Cell>> = entries
        .iter()
        .flat_map(|entry| {
            entry.status.jobs.iter().map(move |job| {
                let mut row = BTreeMap::new();
                row.insert(
                    "job".to_string(),
                    Cell::Rich(RichText {
                        text: job.name.clone(),
                        route: Some(job_route(&entry.repo.namespace, &job.name)),
                        href: None,
                        tone: None,
                    }),
                );
                row.insert("repo".to_string(), Cell::Text(entry.repo.id()));
                row.insert("file".to_string(), Cell::Text(job.file.clone()));
                row.insert(
                    "state".to_string(),
                    Cell::Rich(RichText {
                        text: job.state.as_str().to_string(),
                        route: None,
                        href: None,
                        tone: Some(state_tone(job.state)),
                    }),
                );
                row.insert(
                    "detail".to_string(),
                    Cell::Text(job.detail.clone().unwrap_or_default()),
                );
                row
            })
        })
        .collect();
    blocks.push(Block::Table(TableBlock {
        title: Some("Jobs".into()),
        columns: vec![
            column("job", "Job", ColumnType::Text),
            column("repo", "Repo", ColumnType::Text),
            column("file", "File", ColumnType::Text),
            column("state", "State", ColumnType::Text),
            column("detail", "Detail", ColumnType::Text),
        ],
        rows: job_rows,
        empty: Some("No jobs deployed yet".into()),
    }));
    AppView::new(blocks)
}

fn column(key: &str, label: &str, kind: ColumnType) -> Column {
    Column {
        key: key.into(),
        label: label.into(),
        kind,
    }
}

/// The GitOps panel on a job's page.
fn job_view(request: &ViewRequest, entries: &[BookEntry]) -> AppView {
    let namespace = namespace_of(&request.context.namespace);
    let Some(entry) = deploying(entries, &namespace, &request.context.job) else {
        return AppView::hidden();
    };
    let Some(job) = entry
        .status
        .jobs
        .iter()
        .find(|job| job.name == request.context.job)
    else {
        return AppView::hidden();
    };
    let mut blocks = Vec::new();
    if entry.verdict.health != Health::Ready {
        blocks.push(Block::Notice(NoticeBlock {
            tone: match verdict_tone(entry) {
                Tone::Danger => Tone::Danger,
                Tone::Info => Tone::Info,
                Tone::Neutral => Tone::Neutral,
                _ => Tone::Warn,
            },
            title: Some(format!(
                "Repo {}: {}",
                entry.repo.id(),
                entry.verdict.label()
            )),
            text: entry.verdict.message.clone(),
        }));
    }
    let mut items = vec![
        KeyValueItem {
            key: "Repo".into(),
            value: format!("{} ({QUALIFIED_KIND})", entry.repo.id()),
            tone: Some(verdict_tone(entry)),
            route: None,
        },
        KeyValueItem {
            key: "Repository".into(),
            value: entry.spec.url.clone(),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "Ref".into(),
            value: entry.spec.reference.clone(),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "Commit".into(),
            value: commit_text(entry),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "File".into(),
            value: job.file.clone(),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "State".into(),
            value: match &job.detail {
                Some(detail) => format!("{} ({detail})", job.state.as_str()),
                None => job.state.as_str().to_string(),
            },
            tone: Some(state_tone(job.state)),
            route: None,
        },
    ];
    if let Some(synced) = &entry.status.synced_at {
        items.push(KeyValueItem {
            key: "Synced".into(),
            value: synced.clone(),
            tone: None,
            route: None,
        });
    }
    if entry.spec.suspend {
        items.push(KeyValueItem {
            key: "Syncing".into(),
            value: "suspended (spec.suspend)".into(),
            tone: Some(Tone::Neutral),
            route: None,
        });
    }
    blocks.push(Block::KeyValue(KeyValueBlock { title: None, items }));
    blocks.push(Block::Markdown(MarkdownBlock {
        text: format!(
            "Deployed from git: gitops writes back a version submitted by hand within {}, and \
             leaves a stopped job stopped until its file changes. Change it in the repository, \
             or `legion object apply` the Repo with `suspend = true` to pause syncing.",
            entry.spec.interval
        ),
    }));
    blocks.push(Block::Actions(ActionsBlock {
        items: vec![ActionButton {
            action: "sync".into(),
            label: Some("Sync now".into()),
            inputs: Map::new(),
        }],
    }));
    AppView::new(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::gitops::sync::{RepoRef, Verdict};
    use crate::apps::gitops::{InventoryEntry, RepoStatus};
    use crate::apps::ui::Viewer;
    use crate::structs::app_ui::{AssertionScope, ViewContext};

    fn entry(namespace: &str, name: &str, uid: u64, ready: bool) -> BookEntry {
        BookEntry {
            repo: RepoRef::new(namespace, name, uid),
            spec: serde_json::from_value(json!({ "url": "https://github.com/acme/deploy.git" }))
                .unwrap(),
            status: RepoStatus {
                commit: Some("a".repeat(40)),
                short: Some("aaaaaaa".into()),
                subject: Some("Bump web".into()),
                deployed: 1,
                jobs: vec![InventoryEntry {
                    name: "web".into(),
                    file: "web.ncl".into(),
                    digest: "d".into(),
                    version: None,
                    state: JobState::Deployed,
                    detail: None,
                }],
                ..Default::default()
            },
            verdict: Verdict::new(
                if ready {
                    Health::Ready
                } else {
                    Health::NotReady
                },
                if ready { "Synced" } else { "FetchFailed" },
                "at aaaaaaa: 1 job deployed",
                std::time::Duration::from_secs(60),
            ),
            checked_ms: 1_800_000_000_000,
        }
    }

    fn request(slot: &str, namespace: &str, job: &str, namespaces: &[&str]) -> ViewRequest {
        ViewRequest {
            view: slot.into(),
            context: ViewContext {
                slot: slot.into(),
                namespace: namespace.into(),
                job: job.into(),
                ..Default::default()
            },
            viewer: Viewer {
                principal: "alice".into(),
                accessor: None,
                permission: Some("job:read".into()),
                scope: if namespaces.is_empty() {
                    AssertionScope::Context
                } else {
                    AssertionScope::Any
                },
                namespaces: namespaces.iter().map(|ns| ns.to_string()).collect(),
                delegation: None,
            },
        }
    }

    #[test]
    fn the_page_lists_the_repos_the_viewer_may_read_test() {
        let entries = vec![
            entry("team-a", "storefront", 1, true),
            entry("team-b", "billing", 2, false),
        ];
        let view = repos_view(&request("nav", "", "", &["team-a"]), entries.clone());
        let text = serde_json::to_string(&view).unwrap();
        assert!(
            text.contains("storefront") && !text.contains("billing"),
            "{text}"
        );
        assert!(text.contains("/jobs/team-a/web"), "{text}");
        assert!(text.contains("https://github.com/acme/deploy\""), "{text}");
        let view = repos_view(&request("nav", "", "", &["nobody"]), entries);
        let text = serde_json::to_string(&view).unwrap();
        assert!(text.contains("legion gitops add"), "{text}");
    }

    #[test]
    fn the_job_panel_shows_only_for_deployed_jobs_test() {
        let entries = vec![entry("team-a", "storefront", 1, false)];
        let view = job_view(&request("job", "team-a", "web", &[]), &entries);
        assert!(!view.hidden);
        let text = serde_json::to_string(&view).unwrap();
        assert!(text.contains("aaaaaaa Bump web"), "{text}");
        assert!(text.contains("NotReady: FetchFailed"), "{text}");
        assert!(text.contains("\"action\":\"sync\""), "{text}");
        assert!(job_view(&request("job", "team-a", "api", &[]), &entries).hidden);
        assert!(job_view(&request("job", "team-b", "web", &[]), &entries).hidden);
        // The default namespace may come as an empty context namespace.
        let entries = vec![entry("default", "web", 3, true)];
        assert!(!job_view(&request("job", "", "web", &[]), &entries).hidden);
    }
}
