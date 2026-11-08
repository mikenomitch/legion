//! The bundled gitops app (docs/gitops.md; gitops plan §9), against local
//! `file://` git repositories made with the `git` binary.
//!
//! - **End to end, over gRPC** (ACLs off): the app's own reconcile and
//!   cleanup handlers run in the framework's reconcile loop against a real
//!   `ManagerDaemonService`. A Repo deploys its jobs and reports `Ready`
//!   with its inventory; a new commit resubmits only the job whose file
//!   changed; a version submitted by hand is written back; a job stopped by
//!   hand stays stopped; a deleted file's job is stopped; a commit that does
//!   not render deploys nothing; the `gitops/sync` label is echoed; and
//!   deleting the Repo stops its jobs.
//! - **Bounded by the Repo's writer** (ACLs on, in process): every call a
//!   sync makes acts for the Repo, so the manager refuses a host-level job
//!   the Repo's writer was not checked for, and a KV key the Repo does not
//!   name, until someone who holds them re-applies the Repo; the app's own
//!   credential deploys nothing.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use legion::apps::gitops::render::{RenderedJob, Renderer};
use legion::apps::gitops::sync::{JobView, RepoApi, RepoRef, Syncer};
use legion::apps::gitops::{self, Book, JobState, RepoSpec, RepoStatus};
use legion::apps::objects::Reconciler;
use legion::cmd::helpers::{manager_connection_at, AuthedManagerClient, ClientTimeouts};
use legion::manager::acl::Authenticator;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::FOR_OBJECT_HEADER;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use tonic::{Code, Request, Status};

type R<T> = anyhow::Result<T>;

/// A local git repository the tests commit to.
struct GitRepo {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl GitRepo {
    fn new() -> R<GitRepo> {
        let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
        let path = dir.path().join("deploy");
        std::fs::create_dir_all(&path)?;
        let repo = GitRepo { _dir: dir, path };
        repo.git(&["-c", "init.defaultBranch=main", "init", "--quiet"])?;
        Ok(repo)
    }

    fn git(&self, args: &[&str]) -> R<String> {
        let output = Command::new("git")
            .args(["-c", "user.name=Test", "-c", "user.email=test@example.com"])
            .args(args)
            .current_dir(&self.path)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .context("git")?;
        if !output.status.success() {
            bail!(
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn write(&self, file: &str, content: &str) -> R<()> {
        let path = self.path.join(file);
        std::fs::create_dir_all(path.parent().context("parent")?)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    fn remove(&self, file: &str) -> R<()> {
        std::fs::remove_file(self.path.join(file))?;
        Ok(())
    }

    /// Commit everything; the new commit's SHA.
    fn commit(&self, message: &str) -> R<String> {
        self.git(&["add", "--all"])?;
        self.git(&["commit", "--quiet", "--allow-empty", "-m", message])?;
        self.git(&["rev-parse", "HEAD"])
    }

    fn url(&self) -> String {
        format!("file://{}", self.path.display())
    }
}

/// A job (`runtime` is `exec` or `container`) that imports a shared file;
/// `size` tells versions apart.
fn job_file(name: &str, runtime: &str, size: u64) -> String {
    let config = match runtime {
        "container" => "{ image = \"alpine\", command = [\"sleep\", \"3600\"] }",
        _ => "{ command = [\"sleep\", \"3600\"] }",
    };
    format!(
        "let common = import \"../lib/common.ncl\" in\n\
         {{ name = \"{name}\", task = {{ name = \"main\", runtime = \"{runtime}\", \
            config = {config}, resources = {{ cpu = common.cpu + {size}, memory = 64 }} }} }}\n"
    )
}

fn exec_job(name: &str, size: u64) -> String {
    job_file(name, "exec", size)
}

fn container_job(name: &str, size: u64) -> String {
    job_file(name, "container", size)
}

fn seed_gitops(store: &Store) -> R<()> {
    let manifest = legion::apps::bundled_manifest("gitops").context("gitops is bundled")?;
    let mut record = legion::manager::apps::seed_record(manifest, None);
    record.config = json!({ "allowFileUrls": true });
    store.seed_app(record)?;
    Ok(())
}

fn status_of(object: &AppObject) -> RepoStatus {
    RepoStatus::of(object)
}

// ---------------------------------------------------------------------------
// End to end over gRPC, ACLs off.
// ---------------------------------------------------------------------------

fn node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({ "maxAllocations": 50 }).to_string(),
        fingerprint_attrs: json!({
            "memory_total": (16384u64 * 1024 * 1024).to_string(),
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

/// A manager's own worker, on this build: where gitops's job runs.
fn manager_node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.into(),
        pool: "legion-system".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({
            "legion.role": "manager",
            "namespaces": ["legion-system"],
            "maxAllocations": 10,
            "resources": { "memory": 65536, "cpu": 40000 },
        })
        .to_string(),
        fingerprint_attrs: json!({
            "memory_total": (16384u64 * 1024 * 1024).to_string(),
            "cpus": { "cpu-1": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: json!({ "buildVersion": env!("CARGO_PKG_VERSION"), "buildId": "test" })
            .to_string(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

fn drain(store: &Store) -> R<()> {
    while let Some(eval) = store.claim_eval()? {
        let ok = eval.handle(store).is_ok();
        store.complete_eval(&eval.id, ok)?;
    }
    Ok(())
}

/// Submit gitops's job as the app reconciler does, place it, and report
/// it healthy.
async fn start_app(svc: &Arc<ManagerDaemonService>, store: &Store) -> R<()> {
    let mut reconciler = AppReconciler::new(
        svc.clone(),
        ReconcilerSettings {
            seed: BTreeMap::new(),
            own_build: BuildMarker {
                version: env!("CARGO_PKG_VERSION").into(),
                build_id: "test".into(),
            },
        },
    );
    let outcome = reconciler.reconcile_app("gitops").await;
    assert!(
        matches!(
            outcome,
            legion::manager::apps::PassOutcome::Submitted { .. }
        ),
        "{outcome:?}"
    );
    let job = "legion-system/gitops";
    let config_id = store.get_job_config(job)?.id();
    store.enqueue_eval("job_reschedule", Some(job), Some(&config_id), None, None)?;
    drain(store)?;
    let allocation = store
        .allocs_for_job(job)?
        .into_iter()
        .next()
        .with_context(|| format!("{job} should be placed"))?;
    store.upsert_allocation_health_snapshots(
        &allocation.node_id,
        &[AllocationHealth {
            allocation_id: allocation.id,
            healthy: true,
            status: "running".into(),
            artifact_states: None,
        }],
    )?;
    Ok(())
}

struct Cluster {
    _dir: tempfile::TempDir,
    client: AuthedManagerClient,
    address: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Cluster {
    async fn start() -> R<Cluster> {
        let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir
                .path()
                .join("store")
                .to_str()
                .context("utf-8")?
                .to_string(),
            ..Default::default()
        })?;
        store.ensure_system_namespace()?;
        store.insert_node(node("n0"))?;
        store.insert_node(node("n1"))?;
        store.insert_node(manager_node("m1"))?;
        seed_gitops(&store)?;
        let svc = Arc::new(ManagerDaemonService::new(store.clone()));
        svc.sync_object_kinds_for("gitops")
            .map_err(|status| anyhow!("syncing kinds: {}", status.message()))?;
        // gitops's own job runs and is healthy, so its Repos are not Stale.
        start_app(&svc, &store).await?;
        let service = ManagerDaemonService::new(store.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let incoming = legion::manager::daemon::manager_incoming(&store, listener, None)?;
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ManagerServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
        // The eval workers, as the daemon runs them.
        let evals = {
            let store = store.clone();
            tokio::spawn(async move {
                loop {
                    while let Ok(Some(eval)) = store.claim_eval() {
                        let ok = eval.handle(&store).is_ok();
                        let _ = store.complete_eval(&eval.id, ok);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        };
        let client = legion::cmd::helpers::connect_manager_client_at(&address, timeouts())?;
        Ok(Cluster {
            _dir: dir,
            client,
            address,
            tasks: vec![server, evals],
        })
    }

    async fn apply(
        &self,
        name: &str,
        spec: Value,
        labels: BTreeMap<String, String>,
    ) -> R<AppObject> {
        let object = json!({ "kind": "gitops/Repo", "name": name, "spec": spec, "labels": labels });
        self.client
            .clone()
            .apply_object(ApplyObjectRequest {
                object_json: object.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow!("apply {name}: {}", status.message()))?
            .into_inner()
            .object
            .context("the object")
    }

    async fn repo(&self, name: &str) -> R<AppObject> {
        self.client
            .clone()
            .get_object(GetObjectRequest {
                kind: "gitops/Repo".into(),
                name: name.into(),
                namespace: "default".into(),
            })
            .await
            .map_err(|status| anyhow!("get {name}: {}", status.message()))?
            .into_inner()
            .object
            .context("the object")
    }

    /// Ask for a sync the way `legion gitops sync` does, and wait until the
    /// app has handled it.
    async fn sync(&self, name: &str, token: &str) -> R<AppObject> {
        let current = self.repo(name).await?;
        let spec: Value = serde_json::from_str(&current.spec_json)?;
        let mut labels: BTreeMap<String, String> = current.labels.into_iter().collect();
        labels.insert(gitops::SYNC_LABEL.into(), token.into());
        self.apply(name, spec, labels).await?;
        self.until(name, &format!("sync {token} handled"), |object| {
            status_of(object).sync_request.as_deref() == Some(token)
        })
        .await
    }

    async fn until(
        &self,
        name: &str,
        what: &str,
        check: impl Fn(&AppObject) -> bool,
    ) -> R<AppObject> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let object = self.repo(name).await?;
            if check(&object) {
                return Ok(object);
            }
            if Instant::now() > deadline {
                bail!(
                    "timed out waiting for {what}: {} ({}) {:?}",
                    object.summary,
                    object.summary_detail,
                    object.status
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn job(&self, name: &str) -> R<Option<JobView>> {
        self.client
            .job(name)
            .await
            .map_err(|status| anyhow!("{name}: {}", status.message()))
    }

    async fn job_cpu(&self, name: &str) -> R<u64> {
        let spec = self
            .client
            .clone()
            .get_job_spec(GetJobSpecRequest { name: name.into() })
            .await?
            .into_inner();
        let rendered: Value = serde_json::from_str(&spec.rendered_jobspec)?;
        rendered["tasks"][0]["resources"]["cpu"]
            .as_u64()
            .context("cpu")
    }
}

fn timeouts() -> ClientTimeouts {
    ClientTimeouts {
        connect: Some(Duration::from_secs(5)),
        request: Some(Duration::from_secs(30)),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repo_deploys_and_keeps_its_jobs_deployed_test() -> R<()> {
    let cluster = Cluster::start().await?;
    let git = GitRepo::new()?;
    git.write("lib/common.ncl", "{ cpu = 100 }")?;
    git.write("jobs/web.ncl", &exec_job("web", 1))?;
    git.write("jobs/api.ncl", &exec_job("api", 1))?;
    git.write("jobs/README.md", "not a jobspec")?;
    let first = git.commit("Add web and api")?;

    // The app's own handlers, in the framework's loop.
    let cache = tempfile::tempdir_in(crate::support::scratch_root())?;
    let syncer = Arc::new(Syncer::new(
        Renderer::InProcess,
        cache.path().to_path_buf(),
        true,
    ));
    let book = Arc::new(Book::default());
    let (_wake, wake) = tokio::sync::mpsc::unbounded_channel();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let connection = manager_connection_at(&cluster.address, timeouts())?;
    let reconciling = Reconciler::new("gitops", "Repo")
        .resync(Duration::from_secs(3600))
        .connection(connection)
        .wake(wake)
        .on_cleanup({
            let (syncer, book) = (syncer.clone(), book.clone());
            move |request| {
                let (syncer, book) = (syncer.clone(), book.clone());
                async move { gitops::cleanup(&syncer, &book, request).await }
            }
        })
        .run(cluster.client.clone(), stop, {
            let (syncer, book) = (syncer.clone(), book.clone());
            move |request| {
                let (syncer, book) = (syncer.clone(), book.clone());
                async move { gitops::reconcile(&syncer, &book, request).await }
            }
        });

    // 1. A Repo deploys every job in its path, and reads Ready.
    cluster
        .apply(
            "shop",
            // Its jobs are `exec` tasks, which are host-level.
            json!({ "url": git.url(), "path": "jobs", "interval": "1h", "hostLevel": true }),
            BTreeMap::new(),
        )
        .await?;
    let object = cluster
        .until("shop", "Ready", |object| object.summary == "Ready")
        .await?;
    let status = status_of(&object);
    assert_eq!(status.commit.as_deref(), Some(first.as_str()));
    assert_eq!(status.subject.as_deref(), Some("Add web and api"));
    assert_eq!(status.deployed, 2);
    let names: Vec<&str> = status.jobs.iter().map(|job| job.name.as_str()).collect();
    assert_eq!(names, vec!["api", "web"], "file order");
    assert!(status
        .jobs
        .iter()
        .all(|job| job.state == JobState::Deployed));
    assert_eq!(
        object.summary_detail,
        format!("Synced: at {}: 2 jobs deployed", &first[..7])
    );
    let api_version = cluster.job("api").await?.context("api")?.version;
    // The source files are recorded with the version.
    let spec = cluster
        .client
        .clone()
        .get_job_spec(GetJobSpecRequest { name: "web".into() })
        .await?
        .into_inner();
    let paths: Vec<&str> = spec.submissions.iter().map(|s| s.path.as_str()).collect();
    assert_eq!(&paths[..2], ["jobs/web.ncl", "lib/common.ncl"]);
    assert!(spec.submissions[0].is_primary);

    // 2. A new commit resubmits only the job whose file changed, and adds
    //    a new one; the sync label is echoed.
    git.write("jobs/web.ncl", &exec_job("web", 2))?;
    git.write("jobs/worker.ncl", &exec_job("worker", 1))?;
    let second = git.commit("Grow web, add worker")?;
    let object = cluster.sync("shop", "1").await?;
    let status = status_of(&object);
    assert_eq!(status.commit.as_deref(), Some(second.as_str()));
    assert_eq!(status.deployed, 3);
    assert_eq!(cluster.job_cpu("web").await?, 102);
    assert_eq!(
        cluster.job("api").await?.context("api")?.version,
        api_version
    );
    assert!(cluster.job("worker").await?.is_some());

    // 3. A version submitted by hand is written back.
    let mut by_hand: Value = serde_json::from_str(
        &cluster
            .client
            .clone()
            .get_job_spec(GetJobSpecRequest { name: "web".into() })
            .await?
            .into_inner()
            .rendered_jobspec,
    )?;
    by_hand["tasks"][0]["resources"]["cpu"] = json!(999);
    cluster
        .client
        .clone()
        .run_job(RunJobRequest {
            parsed_jobspec: by_hand.to_string(),
            ..Default::default()
        })
        .await?;
    cluster.sync("shop", "2").await?;
    assert_eq!(cluster.job_cpu("web").await?, 102, "written back");

    // 4. A job stopped by hand stays stopped.
    cluster
        .client
        .clone()
        .stop_job(StopJobRequest {
            name: "api".into(),
            ..Default::default()
        })
        .await?;
    let object = cluster.sync("shop", "3").await?;
    assert_eq!(cluster.job("api").await?.context("api")?.status, "stopped");
    let status = status_of(&object);
    let api = status
        .jobs
        .iter()
        .find(|job| job.name == "api")
        .context("api")?;
    assert_eq!(api.state, JobState::Stopped);

    // 5. A file that leaves the repository stops its job.
    git.remove("jobs/worker.ncl")?;
    let third = git.commit("Remove worker")?;
    let object = cluster.sync("shop", "4").await?;
    assert_eq!(status_of(&object).commit.as_deref(), Some(third.as_str()));
    assert_eq!(
        cluster.job("worker").await?.context("worker")?.status,
        "stopped"
    );
    assert!(status_of(&object)
        .jobs
        .iter()
        .all(|job| job.name != "worker"));

    // 6. A commit that does not render deploys nothing: the last good
    //    commit keeps running.
    git.write("jobs/web.ncl", "{ name = \"web\", task = ")?;
    let broken = git.commit("Break web")?;
    let object = cluster.sync("shop", "5").await?;
    assert_eq!(
        object.summary, "NotReady: RenderFailed",
        "{}",
        object.summary_detail
    );
    assert!(
        object.summary_detail.contains("jobs/web.ncl"),
        "{}",
        object.summary_detail
    );
    let status = status_of(&object);
    assert_eq!(status.commit.as_deref(), Some(third.as_str()));
    assert_eq!(status.latest.as_deref(), Some(broken.as_str()));
    assert_eq!(cluster.job_cpu("web").await?, 102);

    // 7. A commit that removes every job is taken for a mistake.
    git.remove("jobs/web.ncl")?;
    git.remove("jobs/api.ncl")?;
    git.commit("Remove everything")?;
    let object = cluster.sync("shop", "6").await?;
    assert_eq!(object.summary, "NotReady: RenderFailed");
    assert!(
        object.summary_detail.contains("no jobs"),
        "{}",
        object.summary_detail
    );
    assert_ne!(cluster.job("web").await?.context("web")?.status, "stopped");

    // 8. Deleting the Repo stops the jobs it deployed.
    cluster
        .client
        .clone()
        .delete_object(DeleteObjectRequest {
            kind: "gitops/Repo".into(),
            name: "shop".into(),
            namespace: "default".into(),
            ..Default::default()
        })
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while cluster.repo("shop").await.is_ok() {
        if Instant::now() > deadline {
            bail!("the Repo was not released");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(cluster.job("web").await?.context("web")?.status, "stopped");

    stop_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(10), reconciling).await??;
    Ok(())
}

// ---------------------------------------------------------------------------
// Bounded by the Repo's writer: ACLs on, in process.
// ---------------------------------------------------------------------------

const ISSUER: &str = "https://legion.test:4747";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

/// A sync's Legion calls, made in process as the app acting for the Repo:
/// the app's own token, and `legion-for-object` naming the Repo.
struct ForObject {
    svc: Arc<ManagerDaemonService>,
    token: String,
    reference: String,
}

impl ForObject {
    fn request<T>(&self, inner: T) -> Request<T> {
        let mut request = req(inner, &self.token);
        request.metadata_mut().insert(
            FOR_OBJECT_HEADER,
            self.reference.parse().expect("ascii reference"),
        );
        request
    }
}

#[async_trait::async_trait]
impl RepoApi for ForObject {
    async fn job(&self, job: &str) -> Result<Option<JobView>, Status> {
        let info = match self
            .svc
            .get_job_info(self.request(GetJobInfoRequest { name: job.into() }))
            .await
        {
            Ok(reply) => reply.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        let version = self
            .svc
            .list_job_spec_history(self.request(ListJobSpecHistoryRequest { name: job.into() }))
            .await?
            .into_inner()
            .items
            .into_iter()
            .next()
            .map(|item| item.config_id);
        Ok(Some(JobView {
            status: info.job.map(|job| job.status).unwrap_or_default(),
            stamp: info
                .managed_by
                .map(|managed| managed.object)
                .filter(|object| !object.is_empty()),
            version,
        }))
    }

    async fn plan(&self, spec: &Value, namespace: &str) -> Result<(), Status> {
        self.svc
            .plan_job(self.request(PlanJobRequest {
                parsed_jobspec: spec.to_string(),
                namespace: Some(namespace.into()),
                allow_unknown_fields: false,
            }))
            .await?;
        Ok(())
    }

    async fn submit(&self, job: &RenderedJob, namespace: &str) -> Result<String, Status> {
        self.svc
            .run_job(self.request(RunJobRequest {
                parsed_jobspec: job.spec.to_string(),
                submissions: job.submissions.clone(),
                namespace: Some(namespace.into()),
                ..Default::default()
            }))
            .await?;
        let id = legion::structs::namespace::job_id(namespace, &job.name);
        Ok(self
            .svc
            .list_job_spec_history(self.request(ListJobSpecHistoryRequest { name: id }))
            .await?
            .into_inner()
            .items
            .into_iter()
            .next()
            .map(|item| item.config_id)
            .unwrap_or_default())
    }

    async fn stop(&self, job: &str) -> Result<(), Status> {
        self.svc
            .stop_job(self.request(StopJobRequest {
                name: job.into(),
                ..Default::default()
            }))
            .await?;
        Ok(())
    }

    async fn latest_rollout(&self, job: &str) -> Result<Option<Rollout>, Status> {
        Ok(self
            .svc
            .list_rollouts(self.request(ListRolloutsRequest {
                job_id: Some(job.into()),
                ..Default::default()
            }))
            .await?
            .into_inner()
            .rollouts
            .into_iter()
            .max_by_key(|rollout| rollout.id))
    }

    async fn read_kv(&self, key: &str) -> Result<Option<String>, Status> {
        match self
            .svc
            .get_kv(self.request(GetKvRequest {
                key: key.into(),
                metadata_only: false,
            }))
            .await
        {
            Ok(reply) => Ok(reply.into_inner().entry.map(|entry| entry.value)),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }
}

fn acl_node(id: &str, pool: &str, manager: bool) -> NodeMsg {
    let mut config = json!({
        "node_id": id,
        "maxAllocations": 20,
        "resources": { "memory": 65536, "cpu": 40000 },
    });
    if manager {
        config["legion.role"] = json!("manager");
        config["namespaces"] = json!(["legion-system"]);
    }
    NodeMsg::from(Node {
        id: id.into(),
        pool: pool.into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: config,
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        }),
        dynamic_attrs: json!({ "capabilities": [] }),
        health_checks: json!({}),
    })
}

struct Secured {
    svc: Arc<ManagerDaemonService>,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
}

impl Secured {
    async fn start() -> R<Secured> {
        let dir = tempfile::Builder::new()
            .prefix("legion-gitops-")
            .tempdir_in(crate::support::scratch_root())?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("store path")?.to_string(),
            ..Default::default()
        })?;
        store.ensure_system_namespace()?;
        store.insert_worker_type(&WorkerConfig {
            name: "legion-system".into(),
            pool: "legion-system".into(),
            namespaces: vec!["legion-system".into()],
            ..Default::default()
        })?;
        store.insert_worker_type(&WorkerConfig {
            name: "default".into(),
            pool: "default".into(),
            ..Default::default()
        })?;
        store.insert_node(manager_node("mgr-1"))?;
        store.insert_node(acl_node("node-1", "default", false))?;
        let state = Arc::new(WorkloadIdentityState::initialize(
            &store,
            &WorkloadIdentityConfigFull {
                enabled: true,
                issuer: Some(ISSUER.into()),
                ..Default::default()
            },
            ISSUER,
            std::time::SystemTime::now(),
        )?);
        let mut auth = Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )?;
        auth.register_method(Box::new(ControlPlaneIdentityMethod::new(
            state.clone(),
            Vec::new(),
            Duration::from_secs(3600),
            store.clone(),
        )))?;
        let auth = Arc::new(auth);
        let (_, admin) = auth
            .mint_initial_management_token(&store, None)
            .map_err(|s| anyhow!(s.to_string()))?
            .context("a fresh store has no management token")?;
        let svc = Arc::new(
            ManagerDaemonService::with_acl(store.clone(), auth.clone())
                .with_workload_identity(Some(state.clone())),
        );
        seed_gitops(&store)?;
        svc.sync_object_kinds_for("gitops")
            .map_err(|status| anyhow!("syncing kinds: {}", status.message()))?;
        let fx = Secured {
            svc,
            store,
            state,
            admin,
        };
        fx.svc
            .apply_namespace(req(
                ApplyNamespaceRequest {
                    namespace: Some(Namespace {
                        name: "team-a".into(),
                        ..Default::default()
                    }),
                    dry_run: false,
                },
                &fx.admin,
            ))
            .await?;
        fx.start_app().await?;
        Ok(fx)
    }

    async fn start_app(&self) -> R<()> {
        start_app(&self.svc, &self.store).await
    }

    /// The `legion` token the manager would issue gitops's allocation.
    fn app_token(&self) -> R<String> {
        let job = "legion-system/gitops";
        let allocation = self
            .store
            .allocs_for_job(job)?
            .into_iter()
            .next()
            .with_context(|| format!("{job} should be placed"))?;
        let task = self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
            .context("task config")?;
        let claims = claims_for_allocation(&self.store, &allocation, &task, "auto")?;
        let identity = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            ..Default::default()
        };
        Ok(self
            .state
            .issue(&claims, &identity, std::time::SystemTime::now())?
            .token)
    }

    async fn person(&self, name: &str, rules: Value) -> R<String> {
        self.svc
            .upsert_acl_policy(req(
                UpsertAclPolicyRequest {
                    name: name.into(),
                    description: String::new(),
                    rules_json: rules.to_string(),
                },
                &self.admin,
            ))
            .await?;
        Ok(self
            .svc
            .create_acl_token(req(
                CreateAclTokenRequest {
                    name: name.into(),
                    token_type: "client".into(),
                    policies: vec![name.into()],
                    ttl: None,
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .secret)
    }

    async fn apply(&self, spec: Value, token: &str) -> Result<AppObject, Status> {
        let object = json!({ "kind": "gitops/Repo", "name": "site", "namespace": "team-a",
                             "spec": spec });
        Ok(self
            .svc
            .apply_object(req(
                ApplyObjectRequest {
                    object_json: object.to_string(),
                    ..Default::default()
                },
                token,
            ))
            .await?
            .into_inner()
            .object
            .expect("the object"))
    }

    fn api(&self, object: &AppObject, token: &str) -> ForObject {
        ForObject {
            svc: self.svc.clone(),
            token: token.into(),
            reference: format!(
                "gitops/Repo/{}/{}#{}@{}",
                object.namespace, object.name, object.uid, object.generation
            ),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repo_deploys_only_what_its_writer_could_test() -> R<()> {
    let fx = Secured::start().await?;
    let app = fx.app_token()?;
    // Alice may write Repos and jobs in team-a: nothing host-level, no KV.
    let alice = fx
        .person(
            "alice",
            json!([
                { "resources": ["object"], "actions": ["read", "write"],
                  "namespaces": ["team-a"], "kinds": ["gitops/Repo"] },
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["team-a"] },
            ]),
        )
        .await?;
    fx.svc
        .put_kv(req(
            PutKvRequest {
                key: "secret/db".into(),
                value: "postgres://db".into(),
                value_type: KvValueType::String as i32,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;

    let git = GitRepo::new()?;
    git.write("lib/common.ncl", "{ cpu = 100 }")?;
    git.write("jobs/web.ncl", &container_job("web", 1))?;
    git.commit("web")?;
    let cache = tempfile::tempdir_in(crate::support::scratch_root())?;
    let syncer = Syncer::new(Renderer::InProcess, cache.path().to_path_buf(), true);
    let spec = json!({ "url": git.url(), "path": "jobs" });
    let repo = fx.apply(spec.clone(), &alice).await?;
    let sync = |object: AppObject, previous: RepoStatus| {
        let api = fx.api(&object, &app);
        let spec: RepoSpec =
            serde_json::from_str(&object.effective_json).expect("the effective spec");
        let syncer = &syncer;
        async move {
            syncer
                .sync(&api, &RepoRef::from_object(&object), &spec, &previous, None)
                .await
        }
    };

    // 1. Within the authority: web deploys, stamped with the Repo.
    let synced = sync(repo.clone(), RepoStatus::default()).await;
    assert_eq!(synced.status.deployed, 1, "{:?}", synced.verdict);
    let info = fx
        .svc
        .get_job_info(req(
            GetJobInfoRequest {
                name: "team-a/web".into(),
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert_eq!(
        info.managed_by.context("stamped")?.object,
        format!("gitops/Repo/team-a/site#{}", repo.uid)
    );

    // 2. The app's own credential deploys nothing: every rule it holds is
    //    for its objects.
    let own = fx
        .svc
        .run_job(req(
            RunJobRequest {
                parsed_jobspec: json!({ "name": "web", "namespace": "team-a",
                    "task": { "name": "main", "runtime": "container",
                              "config": { "image": "alpine" } } })
                .to_string(),
                ..Default::default()
            },
            &app,
        ))
        .await;
    assert_eq!(own.expect_err("refused").code(), Code::PermissionDenied);

    // 3. A host-level job is beyond what alice was checked for: refused
    //    before anything of the commit is submitted.
    git.write(
        "jobs/runner.ncl",
        "{ name = \"runner\", task = { name = \"main\", runtime = \"exec\", \
           config = { command = [\"sleep\", \"60\"] } } }",
    )?;
    git.write("jobs/web.ncl", &container_job("web", 2))?;
    git.commit("runner")?;
    let synced = sync(repo.clone(), synced.status).await;
    assert_eq!(synced.verdict.label(), "NotReady: RenderFailed");
    assert!(
        synced.verdict.message.contains("runner") && synced.verdict.message.contains("hostLevel"),
        "{}",
        synced.verdict.message
    );
    let web_before = synced.status.jobs[0].version.clone();
    // Alice cannot ask for host-level authority she does not hold.
    let mut host = spec.clone();
    host["hostLevel"] = json!(true);
    let refused = fx.apply(host.clone(), &alice).await.expect_err("refused");
    assert_eq!(
        refused.code(),
        Code::PermissionDenied,
        "{}",
        refused.message()
    );
    assert!(refused.message().contains("host"), "{}", refused.message());
    // Someone who holds it re-applies the Repo, and records the authority.
    let repo = fx.apply(host.clone(), &fx.admin).await?;
    let synced = sync(repo.clone(), synced.status).await;
    assert_eq!(synced.status.deployed, 2, "{:?}", synced.verdict);
    assert_ne!(
        synced
            .status
            .jobs
            .iter()
            .find(|j| j.name == "web")
            .unwrap()
            .version,
        web_before
    );

    // 4. A KV key the Repo does not name is refused, until it does.
    git.write(
        "jobs/reader.ncl",
        "{ name = \"reader\", task = { name = \"main\", runtime = \"container\", \
           config = { image = \"alpine\", command = [\"connect\", \"$secret/db\"] } } }",
    )?;
    git.commit("reader")?;
    let synced = sync(repo.clone(), synced.status).await;
    assert_eq!(synced.verdict.label(), "NotReady: ApplyFailed");
    assert!(
        synced.verdict.message.contains("reader"),
        "{}",
        synced.verdict.message
    );
    let mut kv_only = spec.clone();
    kv_only["kvKeys"] = json!(["secret/db"]);
    let refused = fx.apply(kv_only, &alice).await.expect_err("refused");
    assert!(
        refused.message().contains("kv:read"),
        "{}",
        refused.message()
    );
    let mut with_kv = host.clone();
    with_kv["kvKeys"] = json!(["secret/db"]);
    let repo = fx.apply(with_kv, &fx.admin).await?;
    let synced = sync(repo.clone(), synced.status).await;
    assert_eq!(synced.status.deployed, 3, "{:?}", synced.verdict);

    // 5. Its jobs only: a jobs list narrows the authority, and a job the
    //    list does not name is refused before anything is asked.
    let mut listed = spec.clone();
    listed["jobs"] = json!(["web"]);
    let repo = fx.apply(listed, &alice).await?;
    let synced = sync(repo, synced.status).await;
    assert_eq!(synced.verdict.label(), "NotReady: RenderFailed");
    assert!(
        synced
            .verdict
            .message
            .contains("which spec.jobs does not list"),
        "{}",
        synced.verdict.message
    );
    Ok(())
}
