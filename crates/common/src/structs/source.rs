//! One grammar for "this value comes from somewhere".
//!
//! A port's certificate, a mutual port's client CA, an external upstream's
//! trust bundle, and an injected credential all name where their value lives
//! the same way:
//!
//! ```nickel
//! { kv = "certs/app" }                                    # the manager reads it
//! { identity = "api" }                                    # a minted workload-identity token
//! { file = "/etc/app/ca.pem" }                            # read by the node itself
//! { file = { cert = "/etc/app/tls.crt", key = "/etc/app/tls.key" } }
//! ```
//!
//! Each field allows the kinds that make sense for it ([`Source::check`]):
//! an identity token is not a certificate, and a credential never comes from
//! a node's disk. `kv` and `identity` values are fetched by the node with
//! `FetchDatapathMaterial` and held in memory only; a `file` is read on the
//! node and never crosses the wire.

use anyhow::{bail, Result as R};
use serde::{Deserialize, Serialize};

/// `MaterialRef.kind` (proto `FetchDatapathMaterial`): a policy body by
/// content hash.
pub const MATERIAL_POLICY: &str = "policy";
/// `MaterialRef.kind`: the PEM bundle (chain, then key) behind a port's
/// `tls.cert = { kv }`; `key` is the port label.
pub const MATERIAL_PORT_CERT: &str = "port_cert";
/// `MaterialRef.kind`: the CA bundle behind a port's
/// `tls.clients.ca = { kv }`; `key` is the port label.
pub const MATERIAL_CLIENT_CA: &str = "client_ca";
/// `MaterialRef.kind`: the CA bundle behind an upstream's `ca = { kv }`;
/// `key` is the upstream key.
pub const MATERIAL_UPSTREAM_CA: &str = "upstream_ca";
/// `MaterialRef.kind`: one injected header value of an upstream credential.
pub const MATERIAL_CREDENTIAL: &str = "credential";
/// `MaterialRef.kind`: a network plugin object by name, versioned by its
/// revision; the value is the object's JSON (module URL and checksum,
/// stages, limits).
pub const MATERIAL_PLUGIN: &str = "plugin";
/// `MaterialRef.kind`: one of a plugin's signing keys; `key` is the plugin,
/// `part` the credential name. Held by netd, never handed to the module.
pub const MATERIAL_PLUGIN_CREDENTIAL: &str = "plugin_credential";

/// Where one value comes from. Exactly one key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Source {
    /// A KV path. The manager reads it for the node; the submitter needs
    /// `kv:read` on it.
    Kv(String),
    /// The name of one of the task's `identities`: the manager mints that
    /// workload-identity token for the allocation.
    Identity(String),
    /// A file on the node, read by the node itself.
    File(FileSource),
}

/// A `file` source: one path, or a certificate and key pair.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum FileSource {
    /// One PEM file: a CA bundle, or a certificate chain with its key.
    Path(String),
    /// A certificate chain and its private key, in two files.
    Pair { cert: String, key: String },
}

/// The kinds a [`Source`] can be, for [`Source::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Kv,
    Identity,
    File,
}

impl SourceKind {
    fn name(self) -> &'static str {
        match self {
            SourceKind::Kv => "kv",
            SourceKind::Identity => "identity",
            SourceKind::File => "file",
        }
    }
}

impl Source {
    pub fn kind(&self) -> SourceKind {
        match self {
            Source::Kv(_) => SourceKind::Kv,
            Source::Identity(_) => SourceKind::Identity,
            Source::File(_) => SourceKind::File,
        }
    }

    /// The KV path, when this is a `kv` source.
    pub fn kv_path(&self) -> Option<&str> {
        match self {
            Source::Kv(path) => Some(path.trim()).filter(|path| !path.is_empty()),
            _ => None,
        }
    }

    /// The identity name, when this is an `identity` source.
    pub fn identity_name(&self) -> Option<&str> {
        match self {
            Source::Identity(name) => Some(name.trim()).filter(|name| !name.is_empty()),
            _ => None,
        }
    }

    /// The file paths, when this is a `file` source: `(cert, key)` for a
    /// pair, `(path, None)` for one file.
    pub fn file_paths(&self) -> Option<(&str, Option<&str>)> {
        match self {
            Source::File(FileSource::Path(path)) => Some((path.as_str(), None)),
            Source::File(FileSource::Pair { cert, key }) => {
                Some((cert.as_str(), Some(key.as_str())))
            }
            _ => None,
        }
    }

    /// Trim surrounding whitespace from every path and name, as submit
    /// normalization does for every other jobspec string.
    pub fn trim(&mut self) {
        let trim = |value: &mut String| *value = value.trim().to_string();
        match self {
            Source::Kv(path) => trim(path),
            Source::Identity(name) => trim(name),
            Source::File(FileSource::Path(path)) => trim(path),
            Source::File(FileSource::Pair { cert, key }) => {
                trim(cert);
                trim(key);
            }
        }
    }

    /// Whether the node fetches this value from the manager (as opposed to
    /// reading it itself).
    pub fn is_fetched(&self) -> bool {
        !matches!(self, Source::File(_))
    }

    /// Validate the source for a field that allows `allowed` kinds. `what`
    /// names the field in the error (`port https tls.cert`).
    pub fn check(&self, what: &str, allowed: &[SourceKind]) -> R<()> {
        let kind = self.kind();
        if !allowed.contains(&kind) {
            let names: Vec<_> = allowed.iter().map(|k| k.name()).collect();
            bail!(
                "{what}: a {} source is not allowed here (use {})",
                kind.name(),
                names.join(" or ")
            );
        }
        let empty = match self {
            Source::Kv(path) => path.trim().is_empty(),
            Source::Identity(name) => name.trim().is_empty(),
            Source::File(FileSource::Path(path)) => path.trim().is_empty(),
            Source::File(FileSource::Pair { cert, key }) => {
                cert.trim().is_empty() || key.trim().is_empty()
            }
        };
        if empty {
            bail!("{what}: {} is empty", kind.name());
        }
        if let Some((cert, key)) = self.file_paths() {
            for path in std::iter::once(cert).chain(key) {
                if !path.starts_with('/') {
                    bail!("{what}: file path '{path}' must be absolute");
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for Source {
    /// Where the value comes from, as the jobspec names it, for status
    /// lines and logs: `kv certs/app`, `identity api`, `file /etc/ca.pem`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Kv(path) => write!(f, "kv {}", path.trim()),
            Source::Identity(name) => write!(f, "identity {}", name.trim()),
            Source::File(FileSource::Path(path)) => write!(f, "file {path}"),
            Source::File(FileSource::Pair { cert, key }) => write!(f, "file {cert} + {key}"),
        }
    }
}

#[cfg(test)]
mod source_test {
    use super::*;

    #[test]
    fn parses_each_kind_test() {
        let kv: Source = serde_json::from_str(r#"{"kv":"certs/app"}"#).unwrap();
        assert_eq!(kv, Source::Kv("certs/app".into()));
        assert_eq!(kv.to_string(), "kv certs/app");
        let id: Source = serde_json::from_str(r#"{"identity":"api"}"#).unwrap();
        assert_eq!(id.identity_name(), Some("api"));
        let one: Source = serde_json::from_str(r#"{"file":"/etc/ca.pem"}"#).unwrap();
        assert_eq!(one.file_paths(), Some(("/etc/ca.pem", None)));
        let pair: Source =
            serde_json::from_str(r#"{"file":{"cert":"/a.crt","key":"/a.key"}}"#).unwrap();
        assert_eq!(pair.file_paths(), Some(("/a.crt", Some("/a.key"))));
        assert!(!pair.is_fetched());
    }

    #[test]
    fn rejects_unknown_and_two_keys_test() {
        let err = serde_json::from_str::<Source>(r#"{"vault":"x"}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("kv") && err.contains("identity"), "{err}");
        assert!(serde_json::from_str::<Source>(r#"{"kv":"a","identity":"b"}"#).is_err());
    }

    #[test]
    fn check_enforces_allowed_kinds_test() {
        let id = Source::Identity("api".into());
        let err = id
            .check("port https tls.cert", &[SourceKind::Kv, SourceKind::File])
            .unwrap_err()
            .to_string();
        assert!(err.contains("identity source is not allowed"), "{err}");
        assert!(err.contains("kv or file"), "{err}");
        let rel = Source::File(FileSource::Path("ca.pem".into()));
        assert!(rel.check("x", &[SourceKind::File]).is_err());
        assert!(Source::Kv(" ".into())
            .check("x", &[SourceKind::Kv])
            .is_err());
    }
}
