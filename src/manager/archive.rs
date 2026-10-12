//! The manager's side of archived instances (sandbox-scaling plan, Phase 5;
//! docs/suspend-and-resume.md, "Archived instances").
//!
//! Workers write archive manifests to their `contentStore` when they publish
//! a suspend checkpoint of a task that sets `suspend.archive`. The manager
//! only reads and deletes them: StartInstance reads one back to resume an
//! archived instance, DestroyInstance deletes it, and ListInstances with
//! `include_idle` lists a job's. It reaches the bucket with the credential the
//! worker type's `contentStore.auth.kv` names when the URL is under that
//! store, and with its own ambient credentials (the AWS environment, web
//! identity, or the instance profile) otherwise.

use std::path::PathBuf;

use futures::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use url::Url;

use crate::artifacts::Credential;
use crate::content_publish::{document_url, s3_client, s3_key, ContentStoreTarget};
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::archive::{
    archive_job_prefix, archive_object_name, parse_archive_object_name, ArchiveManifest,
};
use crate::structs::snapshot::SnapshotDurability;
use crate::structs::InstanceMode;

/// The largest archive manifest the manager reads. A manifest names a few
/// dozen parts; anything this big is not one.
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

/// Default and largest page of a bucket listing (`idle_page_size`).
pub const DEFAULT_IDLE_PAGE: usize = 1000;
pub const MAX_IDLE_PAGE: usize = 10_000;

/// Why an archive manifest could not be read.
#[derive(Debug)]
pub enum ArchiveReadError {
    /// No object at the URL.
    NotFound,
    /// The store answered with an error, or the document is not a manifest.
    Failed(anyhow::Error),
}

impl std::fmt::Display for ArchiveReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveReadError::NotFound => write!(f, "the manifest does not exist"),
            ArchiveReadError::Failed(err) => write!(f, "{err:#}"),
        }
    }
}

/// Every distinct `contentStore` the cluster's worker types publish to.
pub fn content_store_roots(store: &Store) -> Vec<ContentStoreTarget> {
    let mut roots: Vec<ContentStoreTarget> = Vec::new();
    for worker_type in store.list_worker_types().unwrap_or_default() {
        if worker_type.artifacts.trim().is_empty() {
            continue;
        }
        let Ok(payload) =
            serde_json::from_str::<crate::structs::WorkerArtifactsPayload>(&worker_type.artifacts)
        else {
            continue;
        };
        if let Some(target) = payload.content_store {
            if !roots.iter().any(|root| root.url == target.url) {
                roots.push(target);
            }
        }
    }
    roots.sort_by(|a, b| a.url.cmp(&b.url));
    roots
}

/// The credential for an `s3://` URL: the `auth.kv` credential of the worker
/// type whose `contentStore` the URL is under, read from the store, or `None`
/// for the ambient chain.
fn credential_for(store: &Store, url: &Url) -> R<Option<Credential>> {
    for root in content_store_roots(store) {
        let Some(kv) = root.auth.as_ref().and_then(|auth| auth.kv.as_deref()) else {
            continue;
        };
        let Ok(root_url) = Url::parse(root.url.trim()) else {
            continue;
        };
        let under = root_url.scheme() == url.scheme()
            && root_url.host_str() == url.host_str()
            && url
                .path()
                .trim_start_matches('/')
                .starts_with(root_url.path().trim_matches('/'));
        if !under {
            continue;
        }
        let entry = store
            .get_kv(kv)?
            .with_context(|| format!("contentStore.auth.kv {kv} not found"))?;
        return Ok(Some(Credential::from_json(&entry.value).with_context(
            || format!("kv {kv} does not hold a valid contentStore credential"),
        )?));
    }
    Ok(None)
}

enum Location {
    File(PathBuf),
    S3 {
        client: object_store::aws::AmazonS3,
        key: ObjectPath,
    },
}

fn locate(store: &Store, raw: &str) -> R<Location> {
    let url = Url::parse(raw).with_context(|| format!("archive manifest url {raw:?}"))?;
    match url.scheme() {
        "file" => Ok(Location::File(url.to_file_path().map_err(|_| {
            anyhow!("archive manifest url {raw:?} is not an absolute file path")
        })?)),
        "s3" => {
            let client = s3_client(&url, credential_for(store, &url)?)?;
            let key = ObjectPath::from(url.path().trim_start_matches('/'));
            Ok(Location::S3 { client, key })
        }
        other => bail!("archive manifest url scheme {other:?} is not supported"),
    }
}

/// Read and parse the archive manifest at `url`.
pub async fn read_manifest(store: &Store, url: &str) -> Result<ArchiveManifest, ArchiveReadError> {
    let location = locate(store, url).map_err(ArchiveReadError::Failed)?;
    let bytes = match location {
        Location::File(path) => match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ArchiveReadError::NotFound)
            }
            Err(err) => {
                return Err(ArchiveReadError::Failed(
                    anyhow!(err).context(format!("read {}", path.display())),
                ))
            }
        },
        Location::S3 { client, key } => match client.get(&key).await {
            Ok(result) => {
                if result.meta.size > MAX_MANIFEST_BYTES {
                    return Err(ArchiveReadError::Failed(anyhow!(
                        "{url} is {} bytes, larger than any archive manifest",
                        result.meta.size
                    )));
                }
                result
                    .bytes()
                    .await
                    .map_err(|err| ArchiveReadError::Failed(anyhow!(err)))?
                    .to_vec()
            }
            Err(object_store::Error::NotFound { .. }) => return Err(ArchiveReadError::NotFound),
            Err(err) => {
                return Err(ArchiveReadError::Failed(
                    anyhow!(err).context(format!("read {url}")),
                ))
            }
        },
    };
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(ArchiveReadError::Failed(anyhow!(
            "{url} is {} bytes, larger than any archive manifest",
            bytes.len()
        )));
    }
    ArchiveManifest::parse(&bytes).map_err(ArchiveReadError::Failed)
}

/// Delete the archive manifest at `url`. One that is already gone is not an
/// error. The checkpoint parts it names stay, like every published part,
/// until the bucket's lifecycle rule removes them.
pub async fn delete_manifest(store: &Store, url: &str) -> R<()> {
    match locate(store, url)? {
        Location::File(path) => match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(anyhow!(err).context(format!("delete {}", path.display()))),
        },
        Location::S3 { client, key } => match client.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(anyhow!(err).context(format!("delete {url}"))),
        },
    }
}

/// [`delete_manifest`] with a few retries, logging a failure instead of
/// returning it: for cleanup after a write already committed.
pub async fn delete_manifest_logged(store: &Store, url: &str, instance_id: &str) {
    let mut last = None;
    for attempt in 0..3u64 {
        match delete_manifest(store, url).await {
            Ok(()) => return,
            Err(err) => last = Some(err),
        }
        tokio::time::sleep(std::time::Duration::from_millis(200 * (attempt + 1))).await;
    }
    if let Some(err) = last {
        warn!(
            instance_id,
            url,
            error = %format!("{err:#}"),
            "Could not delete an archive manifest; the bucket lifecycle rule removes it"
        );
    }
}

/// The `(job, group, task)` tasks an unnamed id could be a `manifestOnly`
/// instance of: keyed tasks that publish their checkpoints (`suspend.durability
/// = "store"`) and whose name the id was minted from. The task's current
/// `archive` mode is deliberately not consulted, so turning `manifestOnly`
/// off leaves the instances it already archived reachable.
fn manifest_only_candidates(store: &Store, instance_id: &str) -> Vec<(String, String, String)> {
    if !crate::structs::is_minted_instance_id(instance_id) {
        return Vec::new();
    }
    let mut candidates = Vec::new();
    for job in store.get_jobs().unwrap_or_default() {
        let Ok(config) = store.get_job_config(&job.name) else {
            continue;
        };
        for task in &config.tasks {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed);
            let published = task
                .suspend
                .as_ref()
                .and_then(|suspend| suspend.resolve().ok())
                .is_some_and(|resolved| resolved.durability == SnapshotDurability::Store);
            if keyed && published && crate::structs::is_minted_for_task(instance_id, &task.name) {
                candidates.push((job.name.clone(), task.group.clone(), task.name.clone()));
            }
        }
    }
    candidates
}

/// Find the archive manifest of an instance archived with `archive =
/// "manifestOnly"`, which has no row: probe each candidate task's object in
/// each `contentStore` root. `Ok(None)` when no manifest exists, so the
/// instance does not either. A store that could not be read is an error
/// rather than a NotFound, since the instance may well be there.
pub async fn find_manifest_only(
    store: &Store,
    instance_id: &str,
) -> R<Option<(ArchiveManifest, String)>> {
    let candidates = manifest_only_candidates(store, instance_id);
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut failure = None;
    for root in content_store_roots(store) {
        let Ok(root_url) = root.validate() else {
            continue;
        };
        for (job, group, task) in &candidates {
            let url = document_url(
                &root_url,
                &archive_object_name(job, group, task, instance_id),
            );
            match read_manifest(store, &url).await {
                Ok(manifest) if manifest.envelope.instance_id == instance_id => {
                    return Ok(Some((manifest, url)))
                }
                Ok(_) | Err(ArchiveReadError::NotFound) => {}
                Err(ArchiveReadError::Failed(err)) => failure = Some(err),
            }
        }
    }
    match failure {
        Some(err) => Err(err.context(format!(
            "could not read the archive manifest of instance {instance_id}"
        ))),
        None => Ok(None),
    }
}

/// One archived instance a bucket listing found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedArchive {
    pub job_id: String,
    pub group: String,
    pub task: String,
    pub instance_id: String,
    pub url: String,
    /// RFC 3339 time the manifest was written.
    pub written_at: String,
}

/// One page of `job_id`'s archive manifests across every `contentStore`
/// root, and the token for the next page (empty when done). The token is
/// `<root index>:<last object name>`, so a listing resumes where it stopped.
pub async fn list_job_archives(
    store: &Store,
    job_id: &str,
    page_token: &str,
    page_size: usize,
) -> R<(Vec<ListedArchive>, String)> {
    let page_size = page_size.clamp(1, MAX_IDLE_PAGE);
    let roots = content_store_roots(store);
    let (mut root_index, mut after) = match page_token.split_once(':') {
        Some((index, after)) => (
            index
                .parse::<usize>()
                .with_context(|| format!("idle_page_token {page_token:?} is not valid"))?,
            after.to_string(),
        ),
        None if page_token.is_empty() => (0, String::new()),
        None => bail!("idle_page_token {page_token:?} is not valid"),
    };
    let prefix = archive_job_prefix(job_id);
    let mut listed = Vec::new();
    while root_index < roots.len() {
        let root_url = roots[root_index].validate()?;
        let want = page_size - listed.len();
        let (names, more) = list_names(store, &root_url, &prefix, &after, want).await?;
        for (name, written_at) in &names {
            if let Some((job, group, task, id)) = parse_archive_object_name(name) {
                listed.push(ListedArchive {
                    job_id: job,
                    group,
                    task,
                    instance_id: id,
                    url: document_url(&root_url, name),
                    written_at: written_at.clone(),
                });
            }
        }
        if more {
            let last = names.last().map(|(name, _)| name.clone()).unwrap_or(after);
            return Ok((listed, format!("{root_index}:{last}")));
        }
        root_index += 1;
        after = String::new();
        if listed.len() >= page_size {
            return Ok((
                listed,
                if root_index < roots.len() {
                    format!("{root_index}:")
                } else {
                    String::new()
                },
            ));
        }
    }
    Ok((listed, String::new()))
}

/// Up to `limit` object names under `prefix` in one root, after `after`, in
/// order, with when each was written, and whether more remain.
async fn list_names(
    store: &Store,
    root: &Url,
    prefix: &str,
    after: &str,
    limit: usize,
) -> R<(Vec<(String, String)>, bool)> {
    let rfc3339 = |time: chrono::DateTime<chrono::Utc>| {
        time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    };
    let mut names: Vec<(String, String)> = match root.scheme() {
        "file" => {
            let base = root
                .to_file_path()
                .map_err(|_| anyhow!("contentStore url {root} is not a file path"))?;
            let dir = base.join(prefix);
            let after = after.to_string();
            tokio::task::spawn_blocking(move || -> R<Vec<(String, String)>> {
                let mut found = Vec::new();
                let mut stack = vec![dir];
                while let Some(dir) = stack.pop() {
                    let entries = match std::fs::read_dir(&dir) {
                        Ok(entries) => entries,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(err) => return Err(anyhow!(err).context(format!("list {dir:?}"))),
                    };
                    for entry in entries {
                        let entry = entry?;
                        let path = entry.path();
                        let meta = entry.metadata()?;
                        if meta.is_dir() {
                            stack.push(path);
                            continue;
                        }
                        let Ok(rel) = path.strip_prefix(&base) else {
                            continue;
                        };
                        let rel = rel.to_string_lossy().replace('\\', "/");
                        if !rel.ends_with(".json") || rel.as_str() <= after.as_str() {
                            continue;
                        }
                        let written: chrono::DateTime<chrono::Utc> = meta
                            .modified()
                            .map(Into::into)
                            .unwrap_or_else(|_| chrono::Utc::now());
                        found.push((rel, rfc3339(written)));
                    }
                }
                Ok(found)
            })
            .await??
        }
        _ => {
            let client = s3_client(root, credential_for(store, root)?)?;
            let full_prefix = ObjectPath::from(s3_key(root, prefix));
            let root_prefix = s3_key(root, "");
            let offset = ObjectPath::from(s3_key(root, after));
            let mut stream = if after.is_empty() {
                client.list(Some(&full_prefix))
            } else {
                client.list_with_offset(Some(&full_prefix), &offset)
            };
            let mut found = Vec::new();
            while let Some(meta) = stream.try_next().await? {
                let key = meta.location.to_string();
                let rel = key
                    .strip_prefix(root_prefix.as_str())
                    .unwrap_or(&key)
                    .trim_start_matches('/')
                    .to_string();
                if rel.ends_with(".json") {
                    found.push((rel, rfc3339(meta.last_modified)));
                }
                if found.len() > limit {
                    break;
                }
            }
            found
        }
    };
    names.sort();
    let more = names.len() > limit;
    names.truncate(limit);
    Ok((names, more))
}

#[cfg(test)]
mod archive_test {
    use super::*;

    fn setup_store() -> R<Store> {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-archive-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let store_config = crate::structs::ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    #[tokio::test]
    async fn file_manifests_read_list_and_delete_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = setup_store()?;
        let root = Url::from_directory_path(dir.path()).map_err(|_| anyhow!("dir url"))?;
        let url = document_url(&root, &archive_object_name("agents", "g", "t", "t-1"));
        assert!(matches!(
            read_manifest(&store, &url).await,
            Err(ArchiveReadError::NotFound)
        ));
        delete_manifest(&store, &url).await?;
        let path = Url::parse(&url)?
            .to_file_path()
            .map_err(|_| anyhow!("path"))?;
        std::fs::create_dir_all(path.parent().context("parent")?)?;
        std::fs::write(&path, b"{\"version\":1}")?;
        assert!(matches!(
            read_manifest(&store, &url).await,
            Err(ArchiveReadError::Failed(_))
        ));
        for id in ["t-2", "t-3"] {
            let other = Url::parse(&document_url(
                &root,
                &archive_object_name("agents", "g", "t", id),
            ))?
            .to_file_path()
            .map_err(|_| anyhow!("path"))?;
            std::fs::write(other, b"{}")?;
        }
        let (names, more) = list_names(&store, &root, &archive_job_prefix("agents"), "", 2).await?;
        assert!(more);
        assert_eq!(
            names
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "manifests/agents/g/t/t-1.json",
                "manifests/agents/g/t/t-2.json"
            ]
        );
        let (rest, more) = list_names(
            &store,
            &root,
            &archive_job_prefix("agents"),
            "manifests/agents/g/t/t-2.json",
            2,
        )
        .await?;
        assert!(!more);
        assert_eq!(rest.len(), 1);
        delete_manifest(&store, &url).await?;
        assert!(!path.exists());
        Ok(())
    }
}
