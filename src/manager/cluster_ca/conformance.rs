//! The conformance harness behind `legion net ca provider test`
//! (docs/cluster-ca.md, "Testing a provider"): run a candidate `exec`
//! command, or load candidate `file` material, the way a manager will, and
//! report each check. Runs locally; no manager is involved.

use super::{exec, verify, CaError, CaView};
use crate::manager::mesh_ca::{generate_csr, CsrSubject, LeafSpec, ManagerCertKind};
use crate::prelude::*;
use std::path::PathBuf;
use std::time::Duration;

/// One check's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// What to test.
#[derive(Debug, Clone)]
pub enum Candidate {
    /// An `exec` signer command.
    Exec {
        command: Vec<String>,
        timeout: Duration,
    },
    /// `file` material.
    File {
        cert_path: PathBuf,
        key_path: PathBuf,
        bundle_path: Option<PathBuf>,
    },
}

impl Candidate {
    /// The `clusterCa` block that configures this candidate, for the
    /// "configure it with" line.
    pub fn config_line(&self) -> String {
        match self {
            Candidate::Exec { command, .. } => format!(
                "clusterCa = {{ provider = \"exec\", command = [{}] }}",
                command
                    .iter()
                    .map(|part| format!("\"{part}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Candidate::File {
                cert_path,
                key_path,
                bundle_path,
            } => {
                let bundle = bundle_path
                    .as_ref()
                    .map(|path| format!(", bundlePath = \"{}\"", path.display()))
                    .unwrap_or_default();
                format!(
                    "clusterCa = {{ provider = \"file\", certPath = \"{}\", keyPath = \"{}\"{bundle} }}",
                    cert_path.display(),
                    key_path.display()
                )
            }
        }
    }

    /// The candidate as words, for the header line.
    pub fn describe(&self) -> String {
        match self {
            Candidate::Exec { command, .. } => command.join(" "),
            Candidate::File {
                cert_path,
                key_path,
                bundle_path,
            } => {
                let mut text = format!("{} with key {}", cert_path.display(), key_path.display());
                if let Some(bundle) = bundle_path {
                    text.push_str(&format!(", bundle {}", bundle.display()));
                }
                text
            }
        }
    }
}

type Signer = Box<dyn Fn(&str, &LeafSpec) -> Result<String, CaError>>;

/// Run every check against `candidate`.
pub fn test_provider(candidate: &Candidate) -> Vec<ProviderCheck> {
    let mut checks = Vec::new();
    let (first, loaded): (&'static str, Result<(CaView, Signer), CaError>) = match candidate {
        Candidate::Exec { command, timeout } => {
            let command_for_sign = command.clone();
            let timeout = *timeout;
            (
                "describe",
                exec::describe(command, timeout).map(|view| {
                    let signer: Signer = Box::new(move |csr: &str, spec: &LeafSpec| {
                        exec::sign(&command_for_sign, timeout, csr, spec)
                    });
                    (view, signer)
                }),
            )
        }
        Candidate::File {
            cert_path,
            key_path,
            bundle_path,
        } => {
            let provider =
                super::file::FileCa::new(cert_path.clone(), key_path.clone(), bundle_path.clone());
            (
                "load",
                provider.load_now().map(|(view, signer)| {
                    let chain = view.chain_pem.clone();
                    let signer: Signer = Box::new(move |csr: &str, spec: &LeafSpec| {
                        signer
                            .sign(csr, spec)
                            .map(|(leaf, _)| format!("{leaf}{chain}"))
                            .map_err(CaError::Internal)
                    });
                    (view, signer)
                }),
            )
        }
    };
    let (view, signer) = match loaded {
        Ok(loaded) => {
            checks.push(ProviderCheck {
                name: first,
                passed: true,
                detail: format!(
                    "bundle of {} CA(s), anchor {}{}",
                    loaded.0.bundle_pins.len(),
                    crate::cluster_tls::short_pin(&loaded.0.pin),
                    if loaded.0.chain_pem.is_empty() {
                        String::new()
                    } else {
                        format!(
                            ", {} intermediate(s) in the chain",
                            crate::cluster_tls::bundle_pins(&loaded.0.chain_pem).len()
                        )
                    }
                ),
            });
            loaded
        }
        Err(err) => {
            checks.push(ProviderCheck {
                name: first,
                passed: false,
                detail: err.to_string(),
            });
            return checks;
        }
    };

    let cases: [(&'static str, LeafSpec); 3] = [
        (
            "sign node",
            LeafSpec::for_subject(&CsrSubject::Node("conformance-node".into())),
        ),
        (
            "sign manager server",
            LeafSpec::for_manager("conformance", ManagerCertKind::Server),
        ),
        (
            "sign manager client",
            LeafSpec::for_manager("conformance", ManagerCertKind::Client),
        ),
    ];
    for (name, spec) in cases {
        checks.push(match generate_csr(&spec.common_name) {
            Ok((_, csr)) => check_sign(name, &spec, &csr, &signer, &view),
            Err(err) => ProviderCheck {
                name,
                passed: false,
                detail: format!("could not build a test CSR: {err:#}"),
            },
        });
    }

    // A CSR that asks for the manager's names: the provider must sign the
    // names in the request, not the CSR's.
    let spec = LeafSpec::for_subject(&CsrSubject::Node("conformance-node-2".into()));
    checks.push(match hostile_csr() {
        Ok(csr) => {
            let mut check = check_sign("requested names win", &spec, &csr, &signer, &view);
            if check.passed {
                check.detail = "the CSR asked for manager.legion; the certificate names only \
                                the requested node"
                    .to_string();
            }
            check
        }
        Err(err) => ProviderCheck {
            name: "requested names win",
            passed: false,
            detail: format!("could not build a test CSR: {err:#}"),
        },
    });
    checks
}

fn check_sign(
    name: &'static str,
    spec: &LeafSpec,
    csr: &str,
    signer: &Signer,
    view: &CaView,
) -> ProviderCheck {
    let returned = match signer(csr, spec) {
        Ok(returned) => returned,
        Err(err) => {
            return ProviderCheck {
                name,
                passed: false,
                detail: err.to_string(),
            }
        }
    };
    match verify::verify_issued(spec, csr, &returned, view, time::OffsetDateTime::now_utc()) {
        Ok(issued) => ProviderCheck {
            name,
            passed: true,
            detail: format!(
                "{} verified, valid for {}",
                spec.uris.join(", "),
                // Rounded to the hour: "1day", not "23h 59m 58s".
                humantime::format_duration(Duration::from_secs(
                    ((issued.expires_at - time::OffsetDateTime::now_utc().unix_timestamp()).max(0)
                        as u64
                        + 1800)
                        / 3600
                        * 3600
                ))
            ),
        },
        Err(refusal) => ProviderCheck {
            name,
            passed: false,
            detail: refusal.to_string(),
        },
    }
}

/// A CSR whose own SANs claim to be a manager.
fn hostile_csr() -> R<String> {
    use crate::cluster_tls::MANAGER_SERVER_NAME;
    use rcgen::{CertificateParams, Ia5String, KeyPair, SanType};
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from(MANAGER_SERVER_NAME.to_string())?),
        SanType::URI(Ia5String::try_from(crate::manager::mesh_ca::manager_uri(
            "evil",
        ))?),
    ];
    Ok(params.serialize_request(&key)?.pem()?)
}
