//! `file`: an operator-provided CA on each manager's disk (docs/cluster-ca.md,
//! "The file provider").
//!
//! - `certPath`: the signing CA certificate, PEM, optionally followed by the
//!   intermediates that chain it to the trust bundle. Typically an
//!   intermediate the organization's root issued for Legion.
//! - `keyPath`: its private key, PKCS#8 PEM. Read by this manager only;
//!   never written to the store, a changeset, a snapshot, or a backup.
//! - `bundlePath` (optional): the CA certificates every holder trusts.
//!   Default: the signing CA itself (cluster-ca-provider calls entry 4).
//!
//! The files are re-read when any of their mtimes or sizes changes, checked
//! on every read of the CA (callers already cache those for a second or
//! more). Every load runs a self-test (sign a probe leaf and verify it), so
//! a CA rcgen cannot sign under exactly, or one whose name constraints
//! exclude `.legion`, is refused at load rather than at a worker. A load
//! that fails at manager start stops the manager; one that fails later
//! keeps the last good CA signing and reports the problem.

use super::{verify, CaError, CaView, ClusterCaProvider};
use crate::manager::mesh_ca::{generate_csr, CaSigner, CsrSubject, LeafSpec};
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::config::CLUSTER_CA_FILE;
use std::path::PathBuf;
use std::time::SystemTime;

/// What identifies one version of the files: (path, mtime, length).
type Stamps = Vec<(PathBuf, Option<SystemTime>, u64)>;

struct Loaded {
    stamps: Stamps,
    view: CaView,
    signer: Arc<CaSigner>,
}

#[derive(Default)]
struct State {
    loaded: Option<Loaded>,
    /// Why the files on disk are not the ones in use, when they are not.
    problem: Option<String>,
    /// The files that last failed to load, so they are parsed once per
    /// change rather than on every read.
    failed_stamps: Option<Stamps>,
}

/// The `file` provider.
pub struct FileCa {
    cert_path: PathBuf,
    key_path: PathBuf,
    bundle_path: Option<PathBuf>,
    state: Mutex<State>,
}

impl FileCa {
    pub fn new(cert_path: PathBuf, key_path: PathBuf, bundle_path: Option<PathBuf>) -> FileCa {
        FileCa {
            cert_path,
            key_path,
            bundle_path,
            state: Mutex::new(State::default()),
        }
    }

    fn paths(&self) -> Vec<&PathBuf> {
        let mut paths = vec![&self.cert_path, &self.key_path];
        if let Some(bundle) = &self.bundle_path {
            paths.push(bundle);
        }
        paths
    }

    fn stamps(&self) -> Stamps {
        self.paths()
            .into_iter()
            .map(|path| {
                let meta = std::fs::metadata(path).ok();
                (
                    path.clone(),
                    meta.as_ref().and_then(|meta| meta.modified().ok()),
                    meta.map(|meta| meta.len()).unwrap_or(0),
                )
            })
            .collect()
    }

    fn read(path: &PathBuf, field: &str) -> R<String> {
        std::fs::read_to_string(path)
            .with_context(|| format!("clusterCa.{field}: cannot read {}", path.display()))
    }

    /// Read, parse, and self-test the files.
    fn load(&self, stamps: Stamps) -> R<Loaded> {
        let cert_pem = Self::read(&self.cert_path, "certPath")?;
        let key_pem = Self::read(&self.key_path, "keyPath")?;
        let bundle_pem = match &self.bundle_path {
            Some(path) => Self::read(path, "bundlePath")?,
            None => {
                // The signing CA itself: the first certificate in certPath.
                let first = crate::cluster_tls::certs_from_pem(&cert_pem)
                    .with_context(|| format!("clusterCa.certPath {}", self.cert_path.display()))?
                    .into_iter()
                    .next()
                    .with_context(|| {
                        format!(
                            "clusterCa.certPath {} holds no certificate",
                            self.cert_path.display()
                        )
                    })?;
                verify::der_to_pem(first.as_ref())
            }
        };
        let view = verify::assemble_view(CLUSTER_CA_FILE, self.location(), &cert_pem, &bundle_pem)
            .map_err(|err| anyhow!("clusterCa.certPath {}: {err}", self.cert_path.display()))?;
        let signer = CaSigner::from_pem(&cert_pem, &key_pem).with_context(|| {
            format!(
                "clusterCa.keyPath {} with certPath {}",
                self.key_path.display(),
                self.cert_path.display()
            )
        })?;
        let signer = Arc::new(signer);
        self_test(&signer, &view).with_context(|| {
            format!(
                "the CA in clusterCa.certPath {} cannot issue a certificate Legion accepts",
                self.cert_path.display()
            )
        })?;
        Ok(Loaded {
            stamps,
            view,
            signer,
        })
    }

    /// The CA in use: re-read when the files changed, the last good one
    /// when the new files are broken. `force` re-reads regardless.
    fn current(&self, force: bool) -> Result<(CaView, Arc<CaSigner>), CaError> {
        let stamps = self.stamps();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !force {
            if let Some(loaded) = state.loaded.as_ref().filter(|l| l.stamps == stamps) {
                return Ok((loaded.view.clone(), loaded.signer.clone()));
            }
            // These exact files already failed to load: not parsed again
            // until they change.
            if state.failed_stamps.as_ref() == Some(&stamps) {
                return match state.loaded.as_ref() {
                    Some(loaded) => Ok((loaded.view.clone(), loaded.signer.clone())),
                    None => Err(CaError::FailedPrecondition(
                        state.problem.clone().unwrap_or_default(),
                    )),
                };
            }
        }
        match self.load(stamps.clone()) {
            Ok(loaded) => {
                // A reload is news; the first load is logged by `start`, so
                // the conformance harness's JSON on stdout stays clean.
                if state.loaded.is_some() {
                    info!(
                        cert_path = %self.cert_path.display(),
                        anchor = %crate::cluster_tls::short_pin(&loaded.view.pin),
                        bundle = loaded.view.bundle_pins.len(),
                        "Reloaded the cluster CA from changed files"
                    );
                }
                super::count_call(CLUSTER_CA_FILE, "load", "ok");
                let result = (loaded.view.clone(), loaded.signer.clone());
                state.loaded = Some(loaded);
                state.problem = None;
                state.failed_stamps = None;
                Ok(result)
            }
            Err(err) => {
                super::count_call(CLUSTER_CA_FILE, "load", "error");
                let message = format!("{err:#}");
                state.failed_stamps = Some(stamps);
                match state.loaded.as_ref() {
                    Some(loaded) => {
                        error!(
                            error = %message,
                            "The cluster CA files changed but cannot be used; still signing \
                             with the previous CA"
                        );
                        let result = (loaded.view.clone(), loaded.signer.clone());
                        state.problem = Some(format!(
                            "the CA files changed but cannot be used, so this manager still \
                             signs with the previous CA: {message}"
                        ));
                        Ok(result)
                    }
                    None => {
                        state.problem = Some(message.clone());
                        Err(CaError::FailedPrecondition(message))
                    }
                }
            }
        }
    }
}

impl FileCa {
    /// Load the files now, ignoring the cache (the conformance harness).
    pub(crate) fn load_now(&self) -> Result<(CaView, Arc<CaSigner>), CaError> {
        self.current(true)
    }
}

/// Sign a probe node certificate and verify it the way a holder would. A
/// failure here is a CA that would hand out certificates nobody accepts.
pub(crate) fn self_test(signer: &CaSigner, view: &CaView) -> R<()> {
    let spec = LeafSpec::for_subject(&CsrSubject::Node("ca-self-test".into()));
    let (_, csr_pem) = generate_csr(&spec.common_name)?;
    let (leaf, _) = signer.sign(&csr_pem, &spec)?;
    let returned = format!("{leaf}{}", view.chain_pem);
    verify::verify_issued(
        &spec,
        &csr_pem,
        &returned,
        view,
        time::OffsetDateTime::now_utc(),
    )
    .map_err(|refusal| anyhow!("a probe certificate it signed was refused: {refusal}"))?;
    Ok(())
}

impl ClusterCaProvider for FileCa {
    fn kind(&self) -> &'static str {
        CLUSTER_CA_FILE
    }

    fn location(&self) -> String {
        match &self.bundle_path {
            Some(bundle) => format!("{}, bundle {}", self.cert_path.display(), bundle.display()),
            None => self.cert_path.display().to_string(),
        }
    }

    fn view(&self, _store: &Store) -> Result<Option<CaView>, CaError> {
        Ok(Some(self.current(false)?.0))
    }

    fn ensure(&self, _store: &Store) -> Result<CaView, CaError> {
        Ok(self.current(false)?.0)
    }

    fn sign(
        &self,
        _store: &Store,
        csr_pem: &str,
        spec: &LeafSpec,
    ) -> Result<(String, CaView), CaError> {
        let (view, signer) = self.current(false)?;
        let (leaf, _) = signer
            .sign(csr_pem, spec)
            .map_err(|err| CaError::Internal(err.context("signing with the file CA")))?;
        Ok((format!("{leaf}{}", view.chain_pem), view))
    }

    fn rotate(&self, _store: &Store) -> Result<CaView, CaError> {
        Err(CaError::FailedPrecondition(format!(
            "the cluster CA comes from the `file` provider ({}); Legion cannot rotate it. \
             Rotate it by replacing the files on every manager: add the new CA to \
             clusterCa.bundlePath on every manager and wait one heartbeat, switch \
             clusterCa.certPath/keyPath to the new CA, then remove the old CA from the bundle \
             a day later (docs/cluster-ca.md#rotation)",
            self.location()
        )))
    }

    fn reload(&self, _store: &Store) -> Result<Option<CaView>, CaError> {
        Ok(Some(self.current(true)?.0))
    }

    fn problem(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .problem
            .clone()
    }

    fn start(&self) -> R<()> {
        let (view, _) = self.current(true).map_err(|err| anyhow!("{err}"))?;
        info!(
            cert_path = %self.cert_path.display(),
            anchor = %crate::cluster_tls::short_pin(&view.pin),
            bundle = view.bundle_pins.len(),
            "Loaded the cluster CA from files"
        );
        Ok(())
    }
}
