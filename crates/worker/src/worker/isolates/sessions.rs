//! Session isolates on this node: one record each, kept in a small
//! node-local file beside the worker's state so a worker restart finds the
//! hibernated ones again, and their checkpoints, kept in the node content
//! store's `snapshots` class, pinned to the session.
//!
//! A checkpoint is one opaque blob per session and generation, written by
//! the host (`IsolateWorker.SaveCheckpoint`). The worker never reads it.

use crate::content_store::{ContentClass, ContentDigest, ContentStore, PinOwner, PutOptions};
use crate::prelude::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Where a session isolate is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Opened in a host; an instance exists or is made on first use.
    #[default]
    Resident,
    /// Its checkpoint is all there is.
    Hibernated,
}

/// A checkpoint the node keeps.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct StoredCheckpoint {
    /// The blob's digest in the content store, or empty when it is a file.
    #[serde(default)]
    pub digest: String,
    /// The file it is in, when the node has no content store.
    #[serde(default)]
    pub path: String,
    pub bytes: u64,
    pub taken_unix_ms: i64,
    /// The engine that wrote it, from the checkpoint header.
    #[serde(default)]
    pub engine_version: String,
    #[serde(default)]
    pub module_id: String,
}

/// One session isolate.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub job_id: String,
    pub task_name: String,
    #[serde(default)]
    pub tenant: String,
    /// The cordon replica that holds it.
    #[serde(default)]
    pub replica: u32,
    #[serde(default)]
    pub state: SessionState,
    pub started_unix_ms: i64,
    #[serde(default)]
    pub last_active_unix_ms: i64,
    /// Checkpoint generation, 0 before the first hibernation.
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub checkpoint: Option<StoredCheckpoint>,
    /// A resize of this session; 0 uses the task's.
    #[serde(default)]
    pub memory_max_bytes: u64,
    #[serde(default)]
    pub memory_reservation_bytes: u64,
    /// The idempotency key it was started with, when it had one.
    #[serde(default)]
    pub idempotency_key: String,
    /// The session ticket's id, for an edge start.
    #[serde(default)]
    pub ticket_jti: String,
    /// `manager` or `edge`.
    #[serde(default)]
    pub origin: String,
}

/// Every session isolate on the node, persisted as one JSON file.
#[derive(Debug)]
pub struct SessionBook {
    path: Option<PathBuf>,
    records: BTreeMap<String, SessionRecord>,
}

impl SessionBook {
    /// The book at `path`, loaded when the file exists. `None` keeps it in
    /// memory only (tests).
    pub fn open(path: Option<PathBuf>) -> Self {
        let records = path
            .as_ref()
            .and_then(|path| std::fs::read(path).ok())
            .and_then(|raw| {
                serde_json::from_slice::<Vec<SessionRecord>>(&raw)
                    .map_err(|err| {
                        warn!(error = %err, "Isolate session records unreadable; starting empty");
                    })
                    .ok()
            })
            .unwrap_or_default()
            .into_iter()
            .map(|record| (record.session_id.clone(), record))
            .collect();
        SessionBook { path, records }
    }

    pub fn get(&self, session_id: &str) -> Option<&SessionRecord> {
        self.records.get(session_id)
    }

    pub fn get_mut(&mut self, session_id: &str) -> Option<&mut SessionRecord> {
        self.records.get_mut(session_id)
    }

    pub fn insert(&mut self, record: SessionRecord) {
        self.records.insert(record.session_id.clone(), record);
        self.save();
    }

    pub fn remove(&mut self, session_id: &str) -> Option<SessionRecord> {
        let removed = self.records.remove(session_id);
        if removed.is_some() {
            self.save();
        }
        removed
    }

    pub fn records(&self) -> impl Iterator<Item = &SessionRecord> {
        self.records.values()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Write the book: a temp file renamed over the old one, owner-only.
    pub fn save(&self) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        let records: Vec<&SessionRecord> = self.records.values().collect();
        let written = (|| -> R<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, serde_json::to_vec(&records)?)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
            }
            std::fs::rename(&tmp, path)?;
            Ok(())
        })();
        if let Err(err) = written {
            warn!(path = %path.display(), error = %format!("{err:#}"), "Could not persist isolate session records");
        }
    }
}

/// Where checkpoints go: the node content store when the node has one,
/// else a directory the worker owns.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    pub store: Option<Arc<ContentStore>>,
    pub dir: PathBuf,
}

fn pin_owner(session_id: &str) -> PinOwner {
    PinOwner::new(format!("isolate-session:{session_id}"))
        .unwrap_or_else(|_| PinOwner::snapshot("isolate-session"))
}

impl CheckpointStore {
    /// Keep a checkpoint body. Blocking.
    pub fn put(
        &self,
        session_id: &str,
        generation: u64,
        body: &[u8],
        now_ms: i64,
    ) -> R<StoredCheckpoint> {
        if let Some(store) = self.store.as_ref() {
            let entry = store.put_bytes(
                ContentClass::Snapshots,
                body,
                PutOptions::default()
                    .pin(pin_owner(session_id))
                    .label("isolate checkpoint"),
            )?;
            return Ok(StoredCheckpoint {
                digest: entry.digest.as_str().to_string(),
                bytes: body.len() as u64,
                taken_unix_ms: now_ms,
                ..Default::default()
            });
        }
        std::fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(format!("{}.{generation}", file_safe(session_id)));
        std::fs::write(&path, body)?;
        Ok(StoredCheckpoint {
            path: path.display().to_string(),
            bytes: body.len() as u64,
            taken_unix_ms: now_ms,
            ..Default::default()
        })
    }

    /// Read a checkpoint body back. Blocking.
    pub fn get(&self, checkpoint: &StoredCheckpoint) -> R<Vec<u8>> {
        if !checkpoint.digest.is_empty() {
            let store = self
                .store
                .as_ref()
                .context("the checkpoint is in a content store this node no longer has")?;
            let digest = ContentDigest::parse(&checkpoint.digest)?;
            let entry = store
                .get(&digest)?
                .context("the checkpoint is gone from the node content store")?;
            return Ok(std::fs::read(&entry.path)?);
        }
        Ok(std::fs::read(&checkpoint.path)?)
    }

    /// Forget a checkpoint. Blocking; failures are logged.
    pub fn drop_checkpoint(&self, session_id: &str, checkpoint: &StoredCheckpoint) {
        if !checkpoint.digest.is_empty() {
            if let (Some(store), Ok(digest)) = (
                self.store.as_ref(),
                ContentDigest::parse(&checkpoint.digest),
            ) {
                let owner = pin_owner(session_id);
                if let Ok(true) = store.unpin(&digest, &owner) {
                    let _ = store.remove(&digest);
                }
            }
            return;
        }
        if !checkpoint.path.is_empty() {
            let _ = std::fs::remove_file(&checkpoint.path);
        }
    }
}

fn file_safe(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '.' { c } else { '_' })
        .collect()
}

/// The book's file beside the worker's state.
pub fn book_path(state_dir: &Path) -> PathBuf {
    state_dir.join("isolates").join("sessions.json")
}

#[cfg(test)]
mod sessions_test {
    use super::*;

    fn record(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.into(),
            job_id: "job".into(),
            task_name: "run".into(),
            started_unix_ms: 1,
            ..Default::default()
        }
    }

    #[test]
    fn records_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = book_path(dir.path());
        let mut book = SessionBook::open(Some(path.clone()));
        book.insert(record("a"));
        let mut hibernated = record("b");
        hibernated.state = SessionState::Hibernated;
        hibernated.generation = 3;
        hibernated.checkpoint = Some(StoredCheckpoint {
            path: "/x".into(),
            bytes: 9,
            ..Default::default()
        });
        book.insert(hibernated.clone());
        let reopened = SessionBook::open(Some(path.clone()));
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.get("b"), Some(&hibernated));
        let mut book = reopened;
        book.remove("a");
        assert_eq!(SessionBook::open(Some(path)).len(), 1);
    }

    #[test]
    fn a_corrupt_book_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = book_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{not json").unwrap();
        assert!(SessionBook::open(Some(path)).is_empty());
    }

    #[test]
    fn checkpoints_round_trip_through_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore {
            store: None,
            dir: dir.path().join("checkpoints"),
        };
        let kept = store.put("s/1", 2, b"state", 5).unwrap();
        assert_eq!(kept.bytes, 5);
        assert_eq!(store.get(&kept).unwrap(), b"state");
        store.drop_checkpoint("s/1", &kept);
        assert!(store.get(&kept).is_err());
    }
}
