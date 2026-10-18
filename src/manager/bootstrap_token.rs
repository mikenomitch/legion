//! The initial management token, minted by the manager at first start.
//!
//! ACLs are on by default, and there is no open bootstrap endpoint: an RPC
//! that works "whenever no management token exists" is a race any workload
//! that can reach the manager could win. Instead, when a manager starts with
//! ACLs on and no management token exists, it mints one itself (a replicated
//! control-lane write, so in raft mode only the control-lane leader does it)
//! and writes the secret to two root-only files:
//!
//! - `<dataDir>/bootstrap-token`, always;
//! - the CLI token file for the manager's local address
//!   (`~/.legion/tokens/<host>_<port>`, or `$LEGION_TOKEN_FILE`), so the CLI
//!   on the same machine works with nothing to set up. Tokens are keyed by
//!   address, so a second cluster on another port never shadows this one.
//!   A file already there is replaced: the mint only happens on a cluster
//!   with no management token, so whatever it held is from an earlier
//!   cluster at this address, or a lesser login to this one.
//!
//! The secret is never logged; the startup line names the accessor and the
//! files. See docs/auth-and-acls.md ("Quick start", "Recovery") and
//! internal_docs/plans/worker-plane-auth-plan.md §6.10.

use crate::manager::acl::Authenticator;
use crate::prelude::*;
use crate::storage::store::Store;
use std::path::{Path, PathBuf};

/// File name of the bootstrap token under the data dir.
pub const BOOTSTRAP_TOKEN_FILE: &str = "bootstrap-token";

/// Where the initial management token's secret is written.
#[derive(Debug, Clone)]
pub struct BootstrapTokenPaths {
    /// `<dataDir>/bootstrap-token`, written on every mint.
    pub data_dir_file: PathBuf,
    /// The CLI token file for the manager's local address, written on
    /// every mint.
    pub cli_file: PathBuf,
}

impl BootstrapTokenPaths {
    /// The paths for this process: `$LEGION_DATA_DIR/bootstrap-token` (else
    /// `./.legion/bootstrap-token`) and the CLI token path for
    /// `local_address`, the address a CLI on this machine dials
    /// ([`crate::structs::ServerConfigFull::local_address`]).
    pub fn for_this_process(local_address: &str) -> Self {
        BootstrapTokenPaths {
            data_dir_file: crate::constants::data_dir().join(BOOTSTRAP_TOKEN_FILE),
            cli_file: crate::cmd::helpers::cli_token_path(local_address),
        }
    }
}

/// What [`ensure_initial_management_token`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapOutcome {
    /// A management token already existed; nothing was minted or written.
    AlreadyExists,
    /// A new management token was minted and its secret written.
    Minted {
        accessor_id: String,
        /// Files the secret was written to, in order.
        written: Vec<PathBuf>,
        /// The CLI token file, when it already held a token that this mint
        /// replaced.
        cli_file_replaced: Option<PathBuf>,
    },
}

/// Mints the initial management token when ACLs are on and none exists, and
/// writes its secret to the bootstrap files (mode 0600).
///
/// The secret goes to `<dataDir>/bootstrap-token.pending` before the
/// replicated insert and is renamed into place only once the insert wins, so
/// a crash between the two never leaves a cluster whose only management
/// secret nobody holds (the next start finishes the rename), and a lost race
/// never leaves a wrong secret on disk.
pub fn ensure_initial_management_token(
    auth: &Authenticator,
    store: &Store,
    bootstrap_secret_file: Option<&str>,
    paths: &BootstrapTokenPaths,
) -> R<BootstrapOutcome> {
    if !auth.enabled {
        return Ok(BootstrapOutcome::AlreadyExists);
    }
    let pending = paths.data_dir_file.with_extension("pending");
    if let Some(parent) = paths.data_dir_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    recover_pending(store, &pending, &paths.data_dir_file)?;
    if store.has_management_token()? {
        return Ok(BootstrapOutcome::AlreadyExists);
    }
    let provided_secret = match bootstrap_secret_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("failed to read acl.bootstrapSecretFile {path}"))?
                .trim()
                .to_string(),
        ),
        None => None,
    };

    let secret = provided_secret.unwrap_or_else(crate::manager::acl::generate_token_secret);
    // On disk before the insert, under a name the CLI never reads: once the
    // replicated insert lands, the only copy of the secret must already be
    // somewhere a restart can find it.
    write_secret_file(&pending, &secret)?;
    let minted = auth.mint_initial_management_token(store, Some(&secret));
    let info = match minted {
        Ok(Some((info, _))) => info,
        Ok(None) => {
            let _ = std::fs::remove_file(&pending);
            return Ok(BootstrapOutcome::AlreadyExists);
        }
        Err(status) => {
            let _ = std::fs::remove_file(&pending);
            return Err(anyhow!(
                "failed to mint the initial management token: {}",
                status.message()
            ));
        }
    };
    std::fs::rename(&pending, &paths.data_dir_file).with_context(|| {
        format!(
            "failed to move the bootstrap token into {}",
            paths.data_dir_file.display()
        )
    })?;
    let cli_file_replaced = paths.cli_file.exists().then(|| paths.cli_file.clone());
    if let Some(parent) = paths.cli_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    write_secret_file(&paths.cli_file, &secret)?;
    Ok(BootstrapOutcome::Minted {
        accessor_id: info.accessor_id,
        written: vec![paths.data_dir_file.clone(), paths.cli_file.clone()],
        cli_file_replaced,
    })
}

/// Finishes a mint that crashed between the replicated insert and the
/// rename: a pending secret that matches a stored management token moves
/// into place, and any other pending file (a lost race, a failed insert) is
/// removed.
fn recover_pending(store: &Store, pending: &Path, target: &Path) -> R<()> {
    let Ok(raw) = std::fs::read_to_string(pending) else {
        return Ok(());
    };
    let secret = raw.trim();
    let stored = store.get_acl_token_by_hash(&crate::manager::acl::hash_secret(secret))?;
    if stored.is_some_and(|record| record.is_management()) {
        std::fs::rename(pending, target)
            .with_context(|| format!("failed to move {} into place", pending.display()))?;
        info!(
            "Recovered the initial management token from an interrupted first start;              secret written to {} (mode 0600)",
            target.display()
        );
    } else {
        std::fs::remove_file(pending)
            .with_context(|| format!("failed to remove {}", pending.display()))?;
    }
    Ok(())
}

/// Logs what [`ensure_initial_management_token`] did, once, without the
/// secret.
pub fn log_outcome(outcome: &BootstrapOutcome) {
    match outcome {
        BootstrapOutcome::AlreadyExists => {}
        BootstrapOutcome::Minted {
            accessor_id,
            written,
            cli_file_replaced,
        } => {
            let files = written
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(" and ");
            info!(
                accessor_id = %accessor_id,
                "ACLs are on. Created the initial management token (accessor {accessor_id}). \
                 Secret written to {files} (mode 0600). The CLI on this machine uses it \
                 automatically. Treat it like a root password."
            );
            if let Some(replaced) = cli_file_replaced {
                info!(
                    "Replaced the token previously stored in {}: this cluster had no \
                     management token, so it came from an earlier cluster at this address \
                     or was a lesser login",
                    replaced.display()
                );
            }
        }
    }
}

/// Writes `secret` to `path` with mode 0600, created that way so the secret
/// is never readable by others, even briefly.
fn write_secret_file(path: &Path, secret: &str) -> R<()> {
    crate::util::write_private_file(path, format!("{secret}\n").as_bytes())
}

#[cfg(test)]
mod bootstrap_token_test {
    use super::*;
    use crate::structs::{AclConfig, ManagerConfigFull};

    fn test_store() -> R<Store> {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-bootstrap-token-")
            .tempdir()?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: store_dir.to_string_lossy().into_owned(),
            ..Default::default()
        })
    }

    fn enabled_authenticator() -> Authenticator {
        Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("authenticator should build")
    }

    fn paths(dir: &Path) -> BootstrapTokenPaths {
        BootstrapTokenPaths {
            data_dir_file: dir.join("data").join(BOOTSTRAP_TOKEN_FILE),
            cli_file: dir
                .join("home")
                .join(".legion")
                .join("tokens")
                .join("127.0.0.1_4747"),
        }
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).expect("stat").permissions().mode() & 0o777
    }

    #[test]
    fn first_start_mints_once_and_writes_both_files_test() -> R<()> {
        let store = test_store()?;
        let auth = enabled_authenticator();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());

        let outcome = ensure_initial_management_token(&auth, &store, None, &paths)?;
        let BootstrapOutcome::Minted {
            written,
            cli_file_replaced,
            ..
        } = outcome
        else {
            panic!("first start should mint, got {outcome:?}");
        };
        assert_eq!(
            written,
            vec![paths.data_dir_file.clone(), paths.cli_file.clone()]
        );
        assert_eq!(cli_file_replaced, None);
        let secret = std::fs::read_to_string(&paths.data_dir_file)?;
        assert_eq!(secret, std::fs::read_to_string(&paths.cli_file)?);
        assert!(!paths.data_dir_file.with_extension("pending").exists());
        #[cfg(unix)]
        {
            assert_eq!(mode(&paths.data_dir_file), 0o600);
            assert_eq!(mode(&paths.cli_file), 0o600);
        }
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "authorization",
            format!("Bearer {}", secret.trim()).parse().expect("header"),
        );
        assert!(auth.authenticate(&metadata, &store)?.is_management);

        // A second start mints nothing and rewrites nothing.
        std::fs::remove_file(&paths.data_dir_file)?;
        let again = ensure_initial_management_token(&auth, &store, None, &paths)?;
        assert_eq!(again, BootstrapOutcome::AlreadyExists);
        assert!(!paths.data_dir_file.exists());
        assert_eq!(store.list_acl_tokens()?.len(), 1);
        Ok(())
    }

    #[test]
    fn a_stale_cli_token_for_the_same_address_is_replaced_test() -> R<()> {
        // The spot-check failure: a token left by an earlier cluster at this
        // address made the CLI present a secret the new cluster never issued.
        let store = test_store()?;
        let auth = enabled_authenticator();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());
        std::fs::create_dir_all(paths.cli_file.parent().expect("parent"))?;
        std::fs::write(&paths.cli_file, "earlier-clusters-token\n")?;
        let other_cluster = paths.cli_file.with_file_name("127.0.0.1_4757");
        std::fs::write(&other_cluster, "other-clusters-token\n")?;

        let outcome = ensure_initial_management_token(&auth, &store, None, &paths)?;
        let BootstrapOutcome::Minted {
            written,
            cli_file_replaced,
            ..
        } = outcome
        else {
            panic!("first start should mint");
        };
        assert_eq!(
            written,
            vec![paths.data_dir_file.clone(), paths.cli_file.clone()]
        );
        assert_eq!(cli_file_replaced, Some(paths.cli_file.clone()));
        assert_eq!(
            std::fs::read_to_string(&paths.cli_file)?,
            std::fs::read_to_string(&paths.data_dir_file)?
        );
        // Another address's token is never touched.
        assert_eq!(
            std::fs::read_to_string(&other_cluster)?,
            "other-clusters-token\n"
        );
        Ok(())
    }

    #[test]
    fn bootstrap_secret_file_seeds_the_secret_test() -> R<()> {
        let store = test_store()?;
        let auth = enabled_authenticator();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());
        let seed = dir.path().join("seed");
        std::fs::write(&seed, "lgn_preseeded_for_iac_0123456789\n")?;

        ensure_initial_management_token(&auth, &store, seed.to_str(), &paths)?;
        assert_eq!(
            std::fs::read_to_string(&paths.data_dir_file)?.trim(),
            "lgn_preseeded_for_iac_0123456789"
        );

        // A too-short seed fails the start, naming the setting.
        let store = test_store()?;
        std::fs::write(&seed, "short")?;
        let err = ensure_initial_management_token(&auth, &store, seed.to_str(), &paths)
            .expect_err("short seed refused");
        assert!(
            format!("{err:#}").contains("bootstrapSecretFile"),
            "{err:#}"
        );
        assert!(!store.has_management_token()?);
        Ok(())
    }

    #[test]
    fn deleting_every_management_token_lets_the_next_start_mint_test() -> R<()> {
        let store = test_store()?;
        let auth = enabled_authenticator();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());
        ensure_initial_management_token(&auth, &store, None, &paths)?;
        let first = std::fs::read_to_string(&paths.data_dir_file)?;
        for token in store.list_acl_tokens()? {
            store.delete_acl_token(&token.accessor_id)?;
        }
        let outcome = ensure_initial_management_token(&auth, &store, None, &paths)?;
        assert!(matches!(outcome, BootstrapOutcome::Minted { .. }));
        assert_ne!(first, std::fs::read_to_string(&paths.data_dir_file)?);
        Ok(())
    }

    #[test]
    fn an_interrupted_first_start_is_finished_by_the_next_test() -> R<()> {
        let store = test_store()?;
        let auth = enabled_authenticator();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());
        std::fs::create_dir_all(paths.data_dir_file.parent().expect("parent"))?;
        // A crash after the insert: the pending secret matches a stored token.
        let secret = "lgn_interrupted_first_start_0123456789";
        auth.mint_initial_management_token(&store, Some(secret))
            .map_err(|s| anyhow!(s.to_string()))?;
        let pending = paths.data_dir_file.with_extension("pending");
        std::fs::write(&pending, format!("{secret}\n"))?;

        let outcome = ensure_initial_management_token(&auth, &store, None, &paths)?;
        assert_eq!(outcome, BootstrapOutcome::AlreadyExists);
        assert_eq!(
            std::fs::read_to_string(&paths.data_dir_file)?.trim(),
            secret
        );
        assert!(!pending.exists());

        // A stale pending file that matches nothing is removed.
        std::fs::write(&pending, "lgn_never_inserted_000000000000000\n")?;
        ensure_initial_management_token(&auth, &store, None, &paths)?;
        assert!(!pending.exists());
        assert_eq!(
            std::fs::read_to_string(&paths.data_dir_file)?.trim(),
            secret
        );
        Ok(())
    }

    #[test]
    fn acls_off_mints_nothing_test() -> R<()> {
        let store = test_store()?;
        let auth = Authenticator::disabled();
        let dir = tempfile::tempdir()?;
        let paths = paths(dir.path());
        let outcome = ensure_initial_management_token(&auth, &store, None, &paths)?;
        assert_eq!(outcome, BootstrapOutcome::AlreadyExists);
        assert!(!paths.data_dir_file.exists());
        Ok(())
    }
}
