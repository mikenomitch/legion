//! KV encryption at rest under the raft engine (docs/kv-encryption.md): a
//! value's plaintext never reaches the raft log, a snapshot, the state
//! database, its WAL, or a backup — only ciphertext does — while the KV API
//! keeps returning the plaintext. Single-node bootstrap, so the upgrade
//! gate is trivially open; the multi-node behavior (joiners receiving the
//! root key, the gate tracking voters) lives in `tests/raft_membership.rs`.

use std::path::{Path, PathBuf};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::KvValueType;

fn raft_config(root: &Path) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.topology = "single".to_string();
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-a".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    // Snapshot after a handful of entries so a snapshot image exists on disk
    // by the time the scan runs.
    config.persistence.raft.snapshot_entries = 2;
    Ok(config)
}

/// Every file under `root` whose bytes contain `needle`.
fn files_containing(root: &Path, needle: &[u8]) -> Vec<PathBuf> {
    fn walk(dir: &Path, needle: &[u8], hits: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, needle, hits);
            } else if let Ok(bytes) = std::fs::read(&path) {
                if bytes.windows(needle.len()).any(|window| window == needle) {
                    hits.push(path);
                }
            }
        }
    }
    let mut hits = Vec::new();
    walk(root, needle, &mut hits);
    hits
}

#[test]
fn plaintext_never_reaches_disk_under_raft() -> R<()> {
    let root = tempfile::tempdir()?;
    let config = raft_config(root.path())?;
    let store = Store::start_storage(&config)?;
    assert_eq!(
        store.keyring().snapshot().state,
        "unlocked",
        "a single-voter bootstrap opens the upgrade gate and initializes at start"
    );
    let root_key = root.path().join("store").join("keys").join("root.key");
    assert!(root_key.exists(), "the root key was minted under storePath");

    let plaintext = "s3cr3t-0xDEADBEEF-never-on-disk";
    for i in 0..6 {
        store.put_kv(
            &format!("secrets/probe-{i}"),
            &format!("{plaintext}-{i}"),
            KvValueType::String,
            None,
            None,
        )?;
    }
    // Read-your-writes through the API.
    let entry = store.get_kv("secrets/probe-0")?.expect("entry");
    assert_eq!(entry.value, format!("{plaintext}-0"));
    assert!(entry.encrypted);

    // Every durable artifact: a backup on top of the state DB, its WAL, the
    // raft log, and the snapshot images the low snapshot threshold produced.
    let backup = root.path().join("backup.sqlite");
    store.state_backup(Some(&backup))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let snapshots = files_containing(root.path(), b"SQLite format 3\0")
            .into_iter()
            .filter(|path| path.to_string_lossy().contains("snapshots"))
            .count();
        if snapshots > 0 || std::time::Instant::now() > deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let hits = files_containing(root.path(), plaintext.as_bytes());
    assert!(
        hits.is_empty(),
        "plaintext leaked into {hits:?} (state db, wal, raft log, snapshot, or backup)"
    );
    // The key PATH is not a secret and is stored as-is — proof the scan
    // actually looks inside the files.
    assert!(
        !files_containing(root.path(), b"secrets/probe-0").is_empty(),
        "the scan should find the (unencrypted) key path somewhere on disk"
    );
    // The root key never enters the store, the log, a snapshot, or the backup.
    let key_line = std::fs::read_to_string(&root_key)?
        .lines()
        .find(|line| !line.starts_with('#') && !line.trim().is_empty())
        .expect("a key line")
        .trim()
        .to_string();
    let key_hits: Vec<PathBuf> = files_containing(root.path(), key_line.as_bytes())
        .into_iter()
        .filter(|path| path != &root_key)
        .collect();
    assert!(
        key_hits.is_empty(),
        "root key material leaked into {key_hits:?}"
    );
    Ok(())
}

#[test]
fn rotation_and_rewrap_replicate_through_raft() -> R<()> {
    let root = tempfile::tempdir()?;
    let config = raft_config(root.path())?;
    let store = Store::start_storage(&config)?;
    store.put_kv("app/token", "abc", KvValueType::String, None, None)?;
    let (first, _) = store.keyring().active_key().expect("active key");

    let rotated = store.rotate_data_key("test")?;
    assert_ne!(rotated.key_id, first);
    assert_eq!(store.get_kv("app/token")?.expect("row").value, "abc");
    let run = store.run_rewrap(false)?;
    assert_eq!(run.rewrapped, 1);
    assert_eq!(run.collected_keys, vec![first]);
    assert_eq!(store.list_keyring_rows()?.len(), 1);

    // A restart on the same data dir replays or re-installs the keyring rows
    // and unwraps them with the file: the value is still readable.
    drop(store);
    let reopened = Store::start_storage(&config)?;
    assert_eq!(reopened.keyring().snapshot().state, "unlocked");
    assert_eq!(reopened.get_kv("app/token")?.expect("row").value, "abc");
    assert_eq!(reopened.list_keyring_rows()?[0].key_id, rotated.key_id);
    Ok(())
}
