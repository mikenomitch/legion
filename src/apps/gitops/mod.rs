//! The bundled GitOps app (docs/gitops.md; gitops plan): it deploys the
//! jobspecs in git repositories and keeps them deployed.
//!
//! People write **Repos** (`gitops/Repo`, an object kind its manifest
//! declares, `bundled/gitops.ncl`): a URL, a ref, a path, an interval. The
//! app reconciles each Repo with the object framework's reconcile loop
//! ([`crate::apps::objects`]): every interval, on any change to the Repo,
//! and at once on a Sync request, it runs one [`sync::Syncer::sync`]. Every
//! Legion call a sync makes acts for the Repo (`legion-for-object`), so
//! the manager holds it to what the Repo's writer was checked for: the
//! app's own grant is all `via = "objects"`, and its own credential can do
//! nothing (gitops plan calls entry 1).
//!
//! - [`git`]: `ls-remote` and a shallow fetch, through the `git` binary.
//! - [`render`]: the jobspec files of a commit, evaluated in a child process
//!   with their imports confined to the checkout.
//! - [`sync`]: one sync: decide, preflight, apply, prune, health.
//! - [`ui`]: the GitOps page, the job panel, and the Sync now action.
//!
//! **State.** A Repo's inventory (the jobs it deployed, with a digest of
//! each) is its status. Status reports never wake the app's watch, so the
//! app keeps the status it last reported in memory ([`Book`]) and reads the
//! stored one only for a Repo it has not synced since it started.

pub mod git;
pub mod render;
pub mod sync;
pub mod ui;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use crate::apps::objects::{run_object_watch, ObjectCache, ObjectOutcome, ShardSelection};
use crate::apps::{AppContext, AppFuture};
use crate::prelude::*;
use crate::structs::AppObject;
use serde::{Deserialize, Serialize};
use sync::{RepoRef, Syncer, Verdict};

/// The app's name.
pub const APP: &str = "gitops";
/// Its kind, as declared.
pub const KIND: &str = "Repo";
/// Its kind, qualified.
pub const QUALIFIED_KIND: &str = "gitops/Repo";
/// The label `legion gitops sync` sets to ask for a sync now; the app
/// echoes its value as `status.fields.syncRequest`.
pub const SYNC_LABEL: &str = "gitops/sync";
/// The hidden command a file is rendered with: `legion app gitops-render`.
pub const RENDER_COMMAND: &str = "gitops-render";
/// The most jobs one Repo deploys (its inventory has to fit in status).
pub const MAX_JOBS: usize = 100;

fn default_ref() -> String {
    "main".to_string()
}

fn default_path() -> String {
    ".".to_string()
}

fn default_interval() -> String {
    "1m".to_string()
}

fn default_username() -> String {
    "x-access-token".to_string()
}

fn yes() -> bool {
    true
}

/// A Repo's spec (`bundled/gitops.ncl` holds its schema; a unit test keeps
/// the two in step).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepoSpec {
    pub url: String,
    /// A branch, a tag, or a full commit SHA.
    #[serde(rename = "ref", default = "default_ref")]
    pub reference: String,
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jobs: Option<Vec<String>>,
    #[serde(default = "yes")]
    pub prune: bool,
    #[serde(default)]
    pub suspend: bool,
    #[serde(default)]
    pub host_level: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kv_keys: Vec<String>,
}

/// A private repository's token, in KV.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Credentials {
    pub kv: String,
    #[serde(default = "default_username")]
    pub username: String,
}

/// What a Repo reports in `status.fields`: the deployed commit and its
/// inventory.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RepoStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_request: Option<String>,
    #[serde(default)]
    pub deployed: u32,
    #[serde(default)]
    pub jobs: Vec<InventoryEntry>,
}

impl RepoStatus {
    /// The status an object carries, or an empty one.
    pub fn of(object: &AppObject) -> RepoStatus {
        object
            .status
            .as_ref()
            .and_then(|status| serde_json::from_str(&status.fields_json).ok())
            .unwrap_or_default()
    }
}

/// One job a Repo deployed.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InventoryEntry {
    pub name: String,
    /// The file it is defined in, relative to the repository root.
    pub file: String,
    /// The digest of the jobspec gitops submitted.
    pub digest: String,
    /// The job config version that submit produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub state: JobState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Where a deployed job stands.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// Its rollout completed, or it runs (or ran to completion).
    Deployed,
    /// Its rollout is under way.
    Progressing,
    /// Its rollout failed, or the job did.
    Failed,
    /// Stopped by hand: gitops leaves it until its file changes.
    Stopped,
    /// Waiting to be placed.
    Pending,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Deployed => "deployed",
            JobState::Progressing => "progressing",
            JobState::Failed => "failed",
            JobState::Stopped => "stopped",
            JobState::Pending => "pending",
        }
    }
}

/// The app's config (`legion app config set gitops`).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitopsConfig {
    /// Allow `file://` Repos: repositories on the disk of the nodes the app
    /// runs on (dev clusters and tests).
    #[serde(default)]
    pub allow_file_urls: bool,
}

/// What the app knows about one Repo: what it last reported, for the next
/// sync's inventory and for the views.
#[derive(Debug, Clone)]
pub struct BookEntry {
    pub repo: RepoRef,
    pub spec: RepoSpec,
    pub status: RepoStatus,
    pub verdict: Verdict,
    /// When it was last synced (unix ms).
    pub checked_ms: i64,
}

/// The app's memory of its Repos, by uid.
#[derive(Debug, Default)]
pub struct Book {
    entries: RwLock<BTreeMap<u64, BookEntry>>,
}

impl Book {
    pub fn record(&self, entry: BookEntry) {
        if let Ok(mut entries) = self.entries.write() {
            entries.insert(entry.repo.uid, entry);
        }
    }

    pub fn forget(&self, uid: u64) {
        if let Ok(mut entries) = self.entries.write() {
            entries.remove(&uid);
        }
    }

    /// The status last reported for `uid`.
    pub fn status(&self, uid: u64) -> Option<RepoStatus> {
        self.entries
            .read()
            .ok()?
            .get(&uid)
            .map(|entry| entry.status.clone())
    }

    /// Every Repo, in uid order.
    pub fn entries(&self) -> Vec<BookEntry> {
        self.entries
            .read()
            .map(|entries| entries.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Forget every Repo but `live` (uids).
    pub fn retain(&self, live: &std::collections::HashSet<u64>) {
        if let Ok(mut entries) = self.entries.write() {
            entries.retain(|uid, _| live.contains(uid));
        }
    }

    /// The Repo whose inventory holds job `name` in `namespace`.
    pub fn deploying(&self, namespace: &str, name: &str) -> Option<BookEntry> {
        self.entries.read().ok()?.values().find_map(|entry| {
            (entry.repo.namespace == namespace
                && entry.status.jobs.iter().any(|job| job.name == name))
            .then(|| entry.clone())
        })
    }
}

/// The app's entrypoint (`legion app run gitops`).
pub fn run(context: AppContext) -> AppFuture {
    Box::pin(run_gitops(context))
}

/// Where checkouts go while a commit renders: the task directory, which
/// the worker gives the app's own user.
fn cache_dir() -> PathBuf {
    match std::env::var("LEGION_TASK_DIR") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir.trim()).join("gitops-cache"),
        _ => std::env::temp_dir().join(format!("legion-gitops-{}", std::process::id())),
    }
}

async fn run_gitops(mut context: AppContext) -> R<()> {
    let config: GitopsConfig = serde_json::from_value(context.config().clone())
        .context("the gitops config does not parse")?;
    if context.legion_addr().is_none() {
        let why = "LEGION_ADDR is not set: workload identity is turned off on the managers \
                   (workloadIdentity.enabled = false), so gitops has no way to act for its Repos";
        warn!("{why}");
        context.set_health(false, why);
        context.stopped().await;
        return Ok(());
    }
    let cache = cache_dir();
    std::fs::create_dir_all(&cache)
        .with_context(|| format!("failed to create {}", cache.display()))?;
    let syncer = Arc::new(Syncer::new(
        render::Renderer::subprocess()?,
        cache,
        config.allow_file_urls,
    ));
    let book = Arc::new(Book::default());
    let live = Arc::new(RwLock::new(ObjectCache::default()));
    let (wake_tx, wake_rx) = tokio::sync::mpsc::unbounded_channel();
    ui::register(&context, book.clone(), live.clone(), wake_tx);
    register_validate(&context, config.allow_file_urls);

    // The Repos that exist, for the views (a Repo deleted with `--now`
    // never reaches the cleanup handler).
    let (_selection_tx, selection) = tokio::sync::watch::channel(ShardSelection::All);
    let watch = tokio::spawn(run_object_watch(
        context.client().await?,
        APP.to_string(),
        vec![KIND.to_string()],
        selection,
        live.clone(),
    ));
    let sweeper = {
        let (book, live) = (book.clone(), live.clone());
        tokio::spawn(async move {
            let mut every = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                every.tick().await;
                let uids = live.read().ok().and_then(|cache| {
                    cache
                        .synced()
                        .then(|| cache.objects().map(|object| object.uid).collect())
                });
                if let Some(uids) = uids {
                    book.retain(&uids);
                }
            }
        })
    };

    let reconciler = context.reconciler(KIND).wake(wake_rx).on_cleanup({
        let (syncer, book) = (syncer.clone(), book.clone());
        move |request| {
            let (syncer, book) = (syncer.clone(), book.clone());
            async move { cleanup(&syncer, &book, request).await }
        }
    });
    let reconciling = context.start_reconciler(reconciler, {
        let (syncer, book) = (syncer.clone(), book.clone());
        move |request| {
            let (syncer, book) = (syncer.clone(), book.clone());
            async move { reconcile(&syncer, &book, request).await }
        }
    })?;
    context.set_health(true, "syncing Repos");
    info!(
        allow_file_urls = config.allow_file_urls,
        "gitops is reconciling Repos"
    );
    context.stopped().await;
    watch.abort();
    sweeper.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), reconciling).await;
    Ok(())
}

/// Reconcile one Repo: sync it, remember what was reported, and hand the
/// outcome to the loop.
pub async fn reconcile(
    syncer: &Syncer,
    book: &Book,
    request: crate::apps::objects::ObjectRequest,
) -> R<ObjectOutcome> {
    let repo = RepoRef::from_object(&request.object);
    let spec: RepoSpec = match request.spec_as() {
        Ok(spec) => spec,
        Err(err) => return Ok(ObjectOutcome::rejected("InvalidSpec", format!("{err:#}"))),
    };
    let previous = book
        .status(repo.uid)
        .unwrap_or_else(|| RepoStatus::of(&request.object));
    let token = request.object.labels.get(SYNC_LABEL).map(String::as_str);
    let client = request.client()?;
    let synced = syncer.sync(&client, &repo, &spec, &previous, token).await;
    let outcome = synced.verdict.outcome(&synced.status);
    book.record(BookEntry {
        repo,
        spec,
        status: synced.status,
        verdict: synced.verdict,
        checked_ms: chrono::Utc::now().timestamp_millis(),
    });
    Ok(outcome)
}

/// Clean up after a deleted Repo: stop the jobs it deployed (with `prune`),
/// then forget it. An error retries until the kind's cleanup window ends.
pub async fn cleanup(
    syncer: &Syncer,
    book: &Book,
    request: crate::apps::objects::ObjectRequest,
) -> R<()> {
    let repo = RepoRef::from_object(&request.object);
    let previous = book
        .status(repo.uid)
        .unwrap_or_else(|| RepoStatus::of(&request.object));
    let client = request.client()?;
    match request.spec_as::<RepoSpec>() {
        Ok(spec) => syncer.cleanup(&client, &repo, &spec, &previous).await?,
        Err(_) => syncer.forget(repo.uid).await,
    }
    book.forget(repo.uid);
    Ok(())
}

/// Answer the validate call on each apply: what the manager's checks cannot
/// know (this cluster's `allowFileUrls`, a duration). Advice: a Repo that
/// slips past it reads `Rejected`.
fn register_validate(context: &AppContext, allow_file_urls: bool) {
    context
        .ui()
        .object_validate(KIND, move |request| async move {
            let mut answer = crate::structs::app_object::ObjectValidateAnswer::default();
            match serde_json::from_value::<RepoSpec>(request.call.effective.clone()) {
                Err(err) => answer
                    .errors
                    .push(format!("the spec does not parse: {err}")),
                Ok(spec) => {
                    if let Err(refusal) =
                        git::check_url(&spec.url, spec.credentials.is_some(), allow_file_urls)
                    {
                        answer.errors.push(refusal.message);
                    }
                    if let Err(message) = sync::parse_interval(&spec.interval) {
                        answer.errors.push(message);
                    }
                    if spec.jobs.is_none() && spec.host_level {
                        answer.warnings.push(
                            "hostLevel without jobs asks for host-level job:write in the whole \
                         namespace; list the jobs to narrow it"
                                .into(),
                        );
                    }
                }
            }
            Ok(answer)
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> &'static crate::structs::app::AppManifest {
        crate::apps::bundled_manifest(APP).expect("gitops is bundled")
    }

    /// The manifest's examples are specs this app parses, every schema
    /// property is a field of [`RepoSpec`], and a spec with every field
    /// round-trips.
    #[test]
    fn the_schema_and_the_spec_type_agree_test() {
        let manifest = manifest();
        let kind = manifest
            .kinds
            .as_ref()
            .and_then(Value::as_array)
            .and_then(|kinds| kinds.iter().find(|kind| kind["name"] == KIND))
            .expect("the Repo kind")
            .clone();
        for example in kind["examples"].as_array().expect("examples") {
            serde_json::from_value::<RepoSpec>(example.clone()).expect("an example parses");
        }
        let mut full = Map::new();
        for (field, schema) in kind["schema"]["properties"]
            .as_object()
            .expect("properties")
        {
            let value = match schema["type"].as_str() {
                Some("string") => json!("main"),
                Some("boolean") => json!(true),
                Some("array") => json!(["web"]),
                Some("object") => json!({ "kv": "secret/token", "username": "u" }),
                other => panic!("{field}: unexpected type {other:?}"),
            };
            full.insert(field.clone(), value);
        }
        let spec: RepoSpec =
            serde_json::from_value(Value::Object(full.clone())).expect("every field parses");
        assert_eq!(serde_json::to_value(&spec).unwrap(), Value::Object(full));
        // Defaults agree.
        let spec: RepoSpec = serde_json::from_value(json!({ "url": "https://x/y" })).unwrap();
        for (field, value) in [
            ("ref", json!(spec.reference)),
            ("path", json!(spec.path)),
            ("interval", json!(spec.interval)),
            ("prune", json!(spec.prune)),
            ("suspend", json!(spec.suspend)),
            ("hostLevel", json!(spec.host_level)),
        ] {
            assert_eq!(
                kind["schema"]["properties"][field]["default"], value,
                "{field}"
            );
        }
        // The status the app reports matches its status schema.
        let status = RepoStatus {
            commit: Some("a".repeat(40)),
            short: Some("aaaaaaa".into()),
            latest: Some("a".repeat(40)),
            subject: Some("Bump".into()),
            synced_at: Some("2026-11-07T10:00:00Z".into()),
            sync_request: Some("1".into()),
            deployed: 1,
            jobs: vec![InventoryEntry {
                name: "web".into(),
                file: "web.ncl".into(),
                digest: "d".into(),
                version: Some("v".into()),
                state: JobState::Deployed,
                detail: Some("ok".into()),
            }],
        };
        let fields = serde_json::to_value(&status).unwrap();
        let properties = kind["status"]["schema"]["properties"].as_object().unwrap();
        for key in fields.as_object().unwrap().keys() {
            assert!(
                properties.contains_key(key),
                "status field {key} is not in the schema"
            );
        }
    }

    /// gitops asks for nothing its own credential can use: every rule acts
    /// for a Repo.
    #[test]
    fn every_permission_is_for_its_objects_test() {
        let manifest = manifest();
        assert!(!manifest.permissions.is_empty());
        for rule in &manifest.permissions {
            let rule = serde_json::to_value(rule).unwrap();
            assert_eq!(rule["via"], "objects", "{rule}");
        }
        let ui = manifest.ui_spec().expect("the ui block parses");
        assert_eq!(
            ui.slots(),
            vec![
                crate::structs::app_ui::Slot::Nav,
                crate::structs::app_ui::Slot::Job
            ]
        );
        assert_eq!(manifest.placement, crate::structs::app::Placement::Managers);
    }

    /// The shipped examples work: `configs/gitops/repo.ncl` is a Repo
    /// this app parses, and `configs/gitops/deploy` renders to jobspecs
    /// the manager's parser takes, which a default Repo may deploy.
    #[tokio::test]
    async fn the_shipped_examples_are_valid_test() {
        let root = env!("CARGO_MANIFEST_DIR");
        let object: Value = serde_json::from_str(
            &crate::config_lang::eval_file(format!("{root}/configs/gitops/repo.ncl"))
                .expect("the example Repo evaluates"),
        )
        .expect("JSON");
        assert_eq!(object["kind"], QUALIFIED_KIND);
        let spec: RepoSpec = serde_json::from_value(object["spec"].clone()).expect("a Repo spec");
        git::check_url(&spec.url, spec.credentials.is_some(), false).expect("an https URL");
        sync::parse_interval(&spec.interval).expect("an interval");

        let deploy = std::path::Path::new(root).join("configs/gitops/deploy");
        let jobs = render::render_tree(&render::Renderer::InProcess, &deploy, "jobs")
            .await
            .expect("the example jobs render");
        let names: Vec<&str> = jobs.iter().map(|job| job.name.as_str()).collect();
        assert_eq!(names, vec!["web", "worker"]);
        for job in &jobs {
            crate::structs::JobConfig::parse_submitted_str(
                &job.spec.to_string(),
                crate::structs::spec_fields::UnknownFieldMode::Reject,
                crate::structs::spec_fields::ExtensionCheck::Offline,
            )
            .unwrap_or_else(|err| panic!("{}: {err:#}", job.file));
        }
        let default: RepoSpec =
            serde_json::from_value(json!({ "url": "https://github.com/acme/deploy" })).unwrap();
        sync::check_jobs(&jobs, &default, "default").expect("a default Repo deploys them");
    }

    #[test]
    fn the_book_finds_the_repo_deploying_a_job_test() {
        let book = Book::default();
        let status = RepoStatus {
            jobs: vec![InventoryEntry {
                name: "web".into(),
                file: "web.ncl".into(),
                digest: "d".into(),
                version: None,
                state: JobState::Deployed,
                detail: None,
            }],
            ..Default::default()
        };
        book.record(BookEntry {
            repo: RepoRef::new("team-a", "storefront", 4),
            spec: serde_json::from_value(json!({ "url": "https://x/y" })).unwrap(),
            status,
            verdict: Verdict::new(
                sync::Health::Ready,
                "Synced",
                "",
                std::time::Duration::from_secs(60),
            ),
            checked_ms: 0,
        });
        assert_eq!(book.deploying("team-a", "web").unwrap().repo.uid, 4);
        assert!(book.deploying("team-b", "web").is_none());
        assert!(book.status(4).is_some());
        book.retain(&std::collections::HashSet::new());
        assert!(book.status(4).is_none());
    }
}
