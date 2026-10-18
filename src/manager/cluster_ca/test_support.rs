//! Test fixtures for the cluster CA providers: a root and an intermediate
//! like an organization's PKI, CA files on disk, and an `exec` helper that
//! signs for real by re-running the test binary (calls entry 15).

use crate::manager::mesh_ca::{CaSigner, LeafRole, LeafSpec};
use crate::prelude::*;
use crate::storage::store::Store;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The environment variable that turns the helper test into a helper.
pub const HELPER_DIR_ENV: &str = "LEGION_TEST_CA_HELPER_DIR";
/// Where the helper writes its JSON reply.
pub const HELPER_OUT_ENV: &str = "LEGION_TEST_CA_HELPER_OUT";
/// The helper test's full name in the lib test binary.
pub const HELPER_TEST: &str = "manager::cluster_ca::tests::exec_helper_main";

/// A root CA and an intermediate it issued.
pub struct TestPki {
    pub root_pem: String,
    pub root_key: String,
    pub inter_pem: String,
    pub inter_key: String,
}

fn ca_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "Acme Corp");
    dn.push(DnType::CommonName, common_name);
    params.distinguished_name = dn;
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
}

/// A fresh root and an intermediate under it.
pub fn pki(name: &str) -> R<TestPki> {
    let root_key = KeyPair::generate()?;
    let root = ca_params(&format!("{name} root")).self_signed(&root_key)?;
    let inter_key = KeyPair::generate()?;
    let inter = ca_params(&format!("{name} legion intermediate"))
        .signed_by(&inter_key, &root, &root_key)?;
    Ok(TestPki {
        root_pem: root.pem(),
        root_key: root_key.serialize_pem(),
        inter_pem: inter.pem(),
        inter_key: inter_key.serialize_pem(),
    })
}

/// Another intermediate under the same root as `pki`.
pub fn second_intermediate(pki: &TestPki, name: &str) -> R<(String, String)> {
    let root_key = KeyPair::from_pem(&pki.root_key)?;
    let root = CertificateParams::from_ca_cert_pem(&pki.root_pem)?.self_signed(&root_key)?;
    let key = KeyPair::generate()?;
    let cert = ca_params(name).signed_by(&key, &root, &root_key)?;
    Ok((cert.pem(), key.serialize_pem()))
}

/// Write `contents` to `dir/name` and return the path.
pub fn write(dir: &Path, name: &str, contents: &str) -> R<PathBuf> {
    let path = dir.join(name);
    std::fs::write(&path, contents)?;
    Ok(path)
}

/// A store with the given `clusterCa` provider.
pub fn store_with(cluster_ca: crate::structs::config::ClusterCaConfigFull) -> R<Store> {
    let dir = tempfile::tempdir_in(crate::test_scratch::root())?.keep();
    Store::start_storage(&crate::structs::ManagerConfigFull {
        store_path: dir.display().to_string(),
        cluster_ca,
        ..Default::default()
    })
}

/// `clusterCa` for a `file` CA.
pub fn file_config(
    cert: &Path,
    key: &Path,
    bundle: Option<&Path>,
) -> crate::structs::config::ClusterCaConfigFull {
    crate::structs::config::ClusterCaConfigFull {
        provider: "file".into(),
        cert_path: cert.display().to_string(),
        key_path: key.display().to_string(),
        bundle_path: bundle.map(|b| b.display().to_string()).unwrap_or_default(),
        ..Default::default()
    }
}

/// `clusterCa` for an `exec` signer.
pub fn exec_config(command: Vec<String>) -> crate::structs::config::ClusterCaConfigFull {
    crate::structs::config::ClusterCaConfigFull {
        provider: "exec".into(),
        command,
        timeout: "30s".into(),
        ..Default::default()
    }
}

/// An `exec` helper in `dir` that signs with `chain_pem`'s first CA (key
/// `key_pem`) and reports `bundle_pem`. `mode` (rewritable with
/// [`set_mode`]) shapes its answers: `good`, `wrong-uri`, `copy-csr`,
/// `long`, `fail`, `garbage`. Returns the command.
pub fn exec_helper(
    dir: &Path,
    chain_pem: &str,
    key_pem: &str,
    bundle_pem: &str,
    mode: &str,
) -> R<Vec<String>> {
    write(dir, "chain.pem", chain_pem)?;
    write(dir, "key.pem", key_pem)?;
    write(dir, "bundle.pem", bundle_pem)?;
    set_mode(dir, mode)?;
    let exe = std::env::current_exe()?;
    let script = format!(
        r#"#!/bin/sh
out=$(mktemp)
{HELPER_DIR_ENV}='{dir}' {HELPER_OUT_ENV}="$out" '{exe}' --exact {HELPER_TEST} --test-threads=1 --nocapture -q >/dev/null 2>"$out.err"
status=$?
if [ $status -ne 0 ]; then cat "$out.err" >&2; rm -f "$out" "$out.err"; exit $status; fi
cat "$out"
rm -f "$out" "$out.err"
"#,
        dir = dir.display(),
        exe = exe.display(),
    );
    let path = write(dir, "helper.sh", &script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(vec![path.display().to_string()])
}

/// Change how the helper in `dir` answers.
pub fn set_mode(dir: &Path, mode: &str) -> R<()> {
    write(dir, "mode", mode)?;
    Ok(())
}

/// The helper itself: runs inside the test binary when [`HELPER_DIR_ENV`]
/// is set. Exits the process, so the test harness never prints a verdict
/// into the reply.
pub fn run_helper(dir: &Path, out: &Path) -> ! {
    let result = (|| -> R<Value> {
        let mode = std::fs::read_to_string(dir.join("mode"))?
            .trim()
            .to_string();
        if mode == "fail" {
            eprintln!("helper: told to fail");
            std::process::exit(3);
        }
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
        let request: Value = serde_json::from_str(&input)?;
        let chain = std::fs::read_to_string(dir.join("chain.pem"))?;
        let bundle = std::fs::read_to_string(dir.join("bundle.pem"))?;
        match request["op"].as_str() {
            Some("describe") => Ok(json!({ "bundle_pem": bundle, "chain_pem": chain })),
            Some("sign") => {
                if mode == "garbage" {
                    return Ok(json!({ "cert_pem": "not a certificate" }));
                }
                let key = std::fs::read_to_string(dir.join("key.pem"))?;
                let strings = |field: &str| -> Vec<String> {
                    request[field]
                        .as_array()
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default()
                };
                let usages = strings("usages");
                let csr_pem = request["csr_pem"].as_str().unwrap_or_default().to_string();
                let mut spec = LeafSpec {
                    role: match request["role"].as_str() {
                        Some("fleet") => LeafRole::Fleet,
                        Some("manager_server") => LeafRole::ManagerServer,
                        Some("manager_client") => LeafRole::ManagerClient,
                        _ => LeafRole::Node,
                    },
                    common_name: request["common_name"].as_str().unwrap_or("x").to_string(),
                    dns_names: strings("dns_sans"),
                    uris: strings("uri_sans"),
                    server_auth: usages.iter().any(|u| u == "server_auth"),
                    client_auth: usages.iter().any(|u| u == "client_auth"),
                    ttl: Duration::from_secs(request["ttl_seconds"].as_u64().unwrap_or(3600)),
                };
                match mode.as_str() {
                    "wrong-uri" => spec.uris.push("spiffe://corp/x".into()),
                    "long" => spec.ttl += Duration::from_secs(2 * 24 * 60 * 60),
                    "copy-csr" => {
                        // A CA that honors the CSR's own names (Vault's
                        // `use_csr_sans` default).
                        let csr = rcgen::CertificateSigningRequestParams::from_pem(&csr_pem)?;
                        let mut dns = Vec::new();
                        let mut uris = Vec::new();
                        for san in &csr.params.subject_alt_names {
                            match san {
                                rcgen::SanType::DnsName(name) => {
                                    dns.push(name.as_str().to_string())
                                }
                                rcgen::SanType::URI(uri) => uris.push(uri.as_str().to_string()),
                                _ => {}
                            }
                        }
                        if !dns.is_empty() || !uris.is_empty() {
                            spec.dns_names = dns;
                            spec.uris = uris;
                        }
                    }
                    _ => {}
                }
                let signer = CaSigner::from_pem(&chain, &key)?;
                let (leaf, _) = signer.sign(&csr_pem, &spec)?;
                Ok(json!({ "cert_pem": leaf }))
            }
            other => bail!("unknown op {other:?}"),
        }
    })();
    match result {
        Ok(reply) => {
            if std::fs::write(out, reply.to_string()).is_err() {
                std::process::exit(4);
            }
            std::process::exit(0)
        }
        Err(err) => {
            eprintln!("helper: {err:#}");
            std::process::exit(2)
        }
    }
}
