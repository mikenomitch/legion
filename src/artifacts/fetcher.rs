//! The [`ArtifactFetcher`] trait, fetch request/limit types, resolved
//! credentials, and the scheme-keyed registry (artifact system plan §3).
//!
//! Fetchers are blocking (run on staging threads, never the async runtime)
//! and stream bytes through an [`crate::artifacts::stage::ArtifactSink`];
//! they never see task directories and never receive raw KV paths — only
//! already-resolved credentials.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Instant;

use libloading::{Library, Symbol};
use url::Url;

use crate::artifacts::stage::ArtifactSink;
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;
use crate::util::plugin_root_dir;

/// Bounds applied to a single artifact fetch.
#[derive(Debug, Clone)]
pub struct FetchLimits {
    /// Hard cap on fetched (and decompressed) bytes.
    pub max_bytes: u64,
    /// Absolute deadline for the whole fetch attempt.
    pub deadline: Option<Instant>,
    /// The worker's host guard, judging every address an `http(s)` fetch
    /// connects to, redirects included: the job chooses the source, and the
    /// response lands in its task directory, so loopback, link-local, cloud
    /// metadata, and this node's own addresses are refused unless a
    /// `hostGuard.allow` entry opens them. `None` (the manager, tests) leaves
    /// fetches unjudged.
    pub egress: Option<std::sync::Arc<crate::structs::host_guard::HostGuard>>,
}

impl Default for FetchLimits {
    fn default() -> Self {
        FetchLimits {
            max_bytes: crate::structs::WorkerArtifactFetchConfigFull::default()
                .max_artifact_size_bytes,
            deadline: None,
            egress: None,
        }
    }
}

impl FetchLimits {
    pub fn check_deadline(&self, redacted_source: &str) -> R<()> {
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                bail!("fetch deadline exceeded for {redacted_source}");
            }
        }
        Ok(())
    }
}

/// A resolved credential handed to a fetcher. Parsed from the KV credential
/// JSON shapes (see `docs/artifacts.md`) or minted from workload identity.
///
/// `Debug` is intentionally redacted: credentials must never reach logs,
/// traces, or error strings.
#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    Basic {
        username: String,
        password: String,
    },
    Bearer {
        token: String,
    },
    Aws {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
    },
    Headers {
        headers: BTreeMap<String, String>,
    },
    /// Sign S3 requests with the node's ambient AWS credential chain (the
    /// `AWS_*` variables, web identity, then the instance profile) instead
    /// of reading anonymously. Never parsed from KV: only a worker's own
    /// `contentStore` without `auth.kv` produces it, so a restoring node
    /// reads published parts the way it would publish them.
    AwsAmbient,
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Credential::Basic { .. } => "basic",
            Credential::Bearer { .. } => "bearer",
            Credential::Aws { .. } => "aws",
            Credential::Headers { .. } => "headers",
            Credential::AwsAmbient => "aws-ambient",
        };
        write!(f, "Credential({kind}, redacted)")
    }
}

impl Credential {
    /// Parses the KV credential JSON value (`type`-discriminated object).
    pub fn from_json(raw: &str) -> R<Credential> {
        let value: Value =
            serde_json::from_str(raw).context("artifact credential is not valid JSON")?;
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .context("artifact credential JSON requires a type field")?;
        let str_field = |name: &str| -> R<String> {
            value
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .with_context(|| format!("artifact credential type {kind} requires {name}"))
        };
        let opt_field = |name: &str| -> Option<String> {
            value.get(name).and_then(Value::as_str).map(str::to_string)
        };
        match kind {
            "basic" => Ok(Credential::Basic {
                username: str_field("username")?,
                password: str_field("password")?,
            }),
            "bearer" => Ok(Credential::Bearer {
                token: str_field("token")?,
            }),
            "aws" => Ok(Credential::Aws {
                access_key_id: str_field("accessKeyId")?,
                secret_access_key: str_field("secretAccessKey")?,
                session_token: opt_field("sessionToken"),
                region: opt_field("region"),
                endpoint: opt_field("endpoint"),
            }),
            "headers" => {
                let headers = value
                    .get("headers")
                    .and_then(Value::as_object)
                    .context("artifact credential type headers requires a headers object")?
                    .iter()
                    .map(|(key, val)| {
                        val.as_str()
                            .map(|v| (key.clone(), v.to_string()))
                            .with_context(|| format!("credential header {key} must be a string"))
                    })
                    .collect::<R<BTreeMap<_, _>>>()?;
                Ok(Credential::Headers { headers })
            }
            other => bail!("unknown artifact credential type {other}"),
        }
    }
}

/// One fetch, fully resolved: parsed source, optional checksum/credential,
/// and limits. `redacted_source` is the only display-safe form.
pub struct FetchRequest<'a> {
    /// Normalized scheme (the `git::https://…` prefix form yields `git`).
    pub scheme: &'a str,
    /// The transport URL (for `git::https://…` this is the inner https URL).
    pub url: &'a Url,
    /// Display-safe source for errors and tracing.
    pub redacted_source: &'a str,
    /// Expected `sha256:<hex>` checksum, when declared.
    pub checksum: Option<&'a str>,
    /// ORAS layer selector for `oci://` sources.
    pub media_type: Option<&'a str>,
    pub credential: Option<&'a Credential>,
    pub limits: &'a FetchLimits,
}

/// Context a manager puts on a failed pin resolution at submit: whether a
/// retry can fix it. Transient failures (the registry or git remote could
/// not be reached, timed out, or answered 5xx or 429) make the submit
/// `Unavailable`; the rest (a tag that does not exist, refused credentials,
/// a malformed reference) stay `InvalidArgument`. The app reconciler backs
/// off and retries the first kind, and parks on the second
/// (docs/apps.md, "How an app stays running").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinResolveFailure {
    pub transient: bool,
}

impl fmt::Display for PinResolveFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.transient {
            write!(f, "the source could not be reached (a retry may succeed)")
        } else {
            write!(f, "the source refused the reference")
        }
    }
}

impl PinResolveFailure {
    /// `err` with this context on it, classified.
    pub fn classify(err: anyhow::Error) -> anyhow::Error {
        let transient = is_transient_fetch_error(&err);
        err.context(PinResolveFailure { transient })
    }

    /// Whether `err` carries a transient pin-resolution failure.
    pub fn transient_in(err: &anyhow::Error) -> bool {
        err.downcast_ref::<PinResolveFailure>()
            .is_some_and(|failure| failure.transient)
    }
}

/// Whether a fetch or resolve error is one a retry can fix: the network,
/// not the reference.
pub fn is_transient_fetch_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(oci) = cause.downcast_ref::<oci_client::errors::OciDistributionError>() {
            use oci_client::errors::OciDistributionError as Oci;
            return match oci {
                Oci::RequestError(request) => !request.is_builder() && !request.is_decode(),
                Oci::IoError(_) => true,
                Oci::ServerError { code, .. } => *code >= 500 || *code == 429,
                _ => false,
            };
        }
        if let Some(request) = cause.downcast_ref::<reqwest::Error>() {
            return request.is_connect()
                || request.is_timeout()
                || request
                    .status()
                    .is_some_and(|status| status.is_server_error() || status.as_u16() == 429);
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind;
            if matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::TimedOut
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }
        }
    }
    // git reports through the last line of its stderr.
    let text = format!("{err:#}").to_ascii_lowercase();
    [
        "could not resolve host",
        "temporary failure in name resolution",
        "connection refused",
        "connection reset",
        "connection timed out",
        "operation timed out",
        "network is unreachable",
        "no route to host",
        "the requested url returned error: 5",
        "the requested url returned error: 429",
        "early eof",
        "rpc failed",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

/// A mutable reference resolved to a stable identity without downloading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIdentity {
    /// The source rewritten to its pinned form (OCI `@sha256:…`, git
    /// `?ref=<sha>`).
    pub pinned_source: String,
    /// The digest/SHA the reference resolved to.
    pub digest: String,
}

/// What a fetcher produced inside the sink.
#[derive(Debug)]
pub enum FetchedPayload {
    /// A single file (the common http/s3 case). `digest` is the sha256 of
    /// the bytes, computed while streaming.
    File {
        path: std::path::PathBuf,
        digest: String,
    },
    /// A directory tree (git clone, file:// directory copy).
    Tree { dir: std::path::PathBuf },
    /// An OCI image laid out as an on-disk OCI layout directory.
    Image {
        oci_layout: std::path::PathBuf,
        reference: String,
        digest: String,
    },
    /// mediaType-selected ORAS layers.
    Layers {
        files: Vec<std::path::PathBuf>,
        media_type: String,
        digest: String,
    },
}

/// Result of a successful fetch.
#[derive(Debug)]
pub struct FetchedArtifact {
    pub payload: FetchedPayload,
    pub bytes: u64,
}

/// One fetcher per scheme (artifact system plan §3).
pub trait ArtifactFetcher: Send + Sync {
    /// URL schemes this fetcher serves (for example `["http", "https"]`).
    fn schemes(&self) -> &'static [&'static str];

    /// Resolve a mutable reference to a stable identity without downloading:
    /// OCI tag -> manifest digest, git ref -> commit SHA. Used by the manager
    /// for `resolve: "pin"` at submit and by the cache plan for revalidation.
    /// Returns `None` when the source has no cheap probe.
    fn resolve(&self, req: &FetchRequest<'_>) -> R<Option<ResolvedIdentity>> {
        let _ = req;
        Ok(None)
    }

    /// Download the artifact into `sink`. The sink hashes as it writes, so
    /// checksum verification and digest computation share one pass.
    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact>;
}

struct LoadedFetcher {
    fetcher: Arc<dyn ArtifactFetcher>,
    /// Keeps the dylib mapped for as long as the registry (and therefore any
    /// outstanding `Arc<dyn ArtifactFetcher>` clone) lives. Entries are never
    /// removed.
    _lib: Option<Library>,
}

impl fmt::Debug for FetcherRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetcherRegistry")
            .field("schemes", &self.by_scheme.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Scheme-keyed fetcher registry. Built-ins register first; dynamic fetcher
/// plugins (`plugins/artifacts/<scheme>/`) are consulted only for schemes no
/// built-in serves. Lookups hand out `Arc` clones so concurrent staging
/// threads fetch without holding the registry lock.
#[derive(Default)]
pub struct FetcherRegistry {
    fetchers: Vec<LoadedFetcher>,
    by_scheme: HashMap<String, usize>,
}

impl FetcherRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry with every built-in fetcher registered.
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();
        registry.register(Arc::new(crate::artifacts::fetchers::FileFetcher::new()));
        registry.register(Arc::new(crate::artifacts::fetchers::HttpFetcher::new()));
        registry.register(Arc::new(crate::artifacts::fetchers::GitFetcher::new()));
        registry.register(Arc::new(crate::artifacts::fetchers::S3Fetcher::new()));
        registry.register(Arc::new(crate::artifacts::fetchers::OciFetcher::new()));
        registry
    }

    pub fn register(&mut self, fetcher: Arc<dyn ArtifactFetcher>) {
        let index = self.fetchers.len();
        let schemes = fetcher.schemes();
        self.fetchers.push(LoadedFetcher {
            fetcher,
            _lib: None,
        });
        for scheme in schemes {
            self.by_scheme.entry(scheme.to_string()).or_insert(index);
        }
    }

    /// Fetcher for a scheme, falling back to a dynamic fetcher plugin dylib
    /// (`plugins/artifacts/<scheme>/`) when no built-in serves it.
    pub fn fetcher(&mut self, scheme: &str) -> R<Arc<dyn ArtifactFetcher>> {
        if !self.by_scheme.contains_key(scheme) {
            self.load_dynamic(scheme)
                .with_context(|| format!("no artifact fetcher registered for scheme {scheme}"))?;
        }
        let index = self
            .by_scheme
            .get(scheme)
            .context("artifact fetcher registry inconsistent")?;
        Ok(self.fetchers[*index].fetcher.clone())
    }

    pub fn supports_scheme(&self, scheme: &str) -> bool {
        self.by_scheme.contains_key(scheme)
    }

    /// Loads `plugins/artifacts/<scheme>/` as a dylib exporting
    /// `get() -> Box<dyn ArtifactFetcher>`, mirroring task-runtime plugins.
    fn load_dynamic(&mut self, scheme: &str) -> R<()> {
        let plugin_root = plugin_root_dir();
        let paths = plugin_paths("artifacts", scheme, &plugin_root);
        validate_manifest("artifacts", scheme, &paths.root)
            .with_context(|| format!("invalid artifact fetcher plugin manifest for {scheme}"))?;
        // SAFETY: identical dylib-loading contract as task-runtime plugins; the
        // plugin must export `get() -> Box<dyn ArtifactFetcher>`.
        unsafe {
            let lib =
                crate::plugin_runtime::open_plugin_library("artifacts", scheme, &paths.dylib)?;
            let fetcher: Arc<dyn ArtifactFetcher> = {
                let func: Symbol<unsafe extern "C" fn() -> Box<dyn ArtifactFetcher>> =
                    lib.get(b"get")?;
                Arc::from(func())
            };
            let index = self.fetchers.len();
            let schemes = fetcher.schemes();
            if !schemes.contains(&scheme) {
                bail!(
                    "artifact fetcher plugin {scheme} reports schemes {schemes:?}, which do \
                     not include {scheme}"
                );
            }
            self.fetchers.push(LoadedFetcher {
                fetcher,
                _lib: Some(lib),
            });
            for plugin_scheme in schemes {
                self.by_scheme
                    .entry(plugin_scheme.to_string())
                    .or_insert(index);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry that cannot be reached is a failure a retry can fix; a
    /// tag that does not exist, or refused credentials, are not (hardening
    /// after the adversarial review, M4). The classification survives the
    /// context the submit puts on top.
    #[test]
    fn pin_failures_are_classified_by_whether_a_retry_can_fix_them() {
        let refused = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ))
        .context("resolve manifest digest for oci://registry.example/app:1");
        let classified = PinResolveFailure::classify(refused).context("resolve artifact failed");
        assert!(PinResolveFailure::transient_in(&classified));

        let server = anyhow::Error::new(oci_client::errors::OciDistributionError::ServerError {
            code: 503,
            url: "https://registry.example/v2/".into(),
            message: "unavailable".into(),
        });
        assert!(is_transient_fetch_error(&server));
        let missing = anyhow::Error::new(oci_client::errors::OciDistributionError::ServerError {
            code: 404,
            url: "https://registry.example/v2/".into(),
            message: "manifest unknown".into(),
        });
        assert!(!is_transient_fetch_error(&missing));
        let auth = anyhow::Error::new(
            oci_client::errors::OciDistributionError::AuthenticationFailure("denied".into()),
        );
        let classified = PinResolveFailure::classify(auth).context("resolve artifact failed");
        assert!(!PinResolveFailure::transient_in(&classified));

        let git = anyhow!(
            "git ls-remote failed for git::https://git.example/app: fatal: unable to access \
             'https://git.example/app/': Could not resolve host: git.example"
        );
        assert!(is_transient_fetch_error(&git));
        let no_ref = anyhow!("git ls-remote found no ref \"v9\" at git::https://git.example/app");
        assert!(!is_transient_fetch_error(&no_ref));
        // An error with no classification is not transient.
        assert!(!PinResolveFailure::transient_in(&anyhow!("bad reference")));
    }

    #[test]
    fn credential_json_shapes_parse() -> R<()> {
        let basic = Credential::from_json(r#"{"type":"basic","username":"u","password":"p"}"#)?;
        assert_eq!(
            basic,
            Credential::Basic {
                username: "u".into(),
                password: "p".into()
            }
        );

        let bearer = Credential::from_json(r#"{"type":"bearer","token":"t"}"#)?;
        assert!(matches!(bearer, Credential::Bearer { .. }));

        let aws = Credential::from_json(
            r#"{"type":"aws","accessKeyId":"AK","secretAccessKey":"SK",
                "region":"us-east-1","endpoint":"https://minio.internal:9000"}"#,
        )?;
        match &aws {
            Credential::Aws {
                region, endpoint, ..
            } => {
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(endpoint.as_deref(), Some("https://minio.internal:9000"));
            }
            other => panic!("expected aws credential, got {other:?}"),
        }

        let headers = Credential::from_json(r#"{"type":"headers","headers":{"X-Api-Key":"k"}}"#)?;
        assert!(matches!(headers, Credential::Headers { .. }));

        assert!(Credential::from_json(r#"{"type":"unknown"}"#).is_err());
        assert!(Credential::from_json(r#"{"username":"u"}"#).is_err());
        Ok(())
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credential = Credential::Basic {
            username: "user".into(),
            password: "hunter2".into(),
        };
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("redacted"), "{rendered}");

        let bearer = Credential::Bearer {
            token: "secret-token".into(),
        };
        let rendered = format!("{bearer:?}");
        assert!(!rendered.contains("secret-token"), "{rendered}");
    }

    #[test]
    fn registry_resolves_builtin_schemes() -> R<()> {
        let mut registry = FetcherRegistry::with_builtins();
        for scheme in ["http", "https", "file", "git", "s3", "oci"] {
            assert!(registry.supports_scheme(scheme), "{scheme}");
            registry.fetcher(scheme)?;
        }
        assert!(registry.fetcher("doesnotexist").is_err());
        Ok(())
    }

    /// Phase 6 acceptance: the sample out-of-tree fetcher dylib
    /// (`plugins/artifacts/demo/`) resolves its custom scheme end to end.
    /// Gated on the dylib being built (LEGION_BUILD_PLUGINS=1), mirroring the
    /// noop task-plugin test.
    #[test]
    fn demo_fetcher_plugin_resolves_custom_scheme_when_built() -> R<()> {
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_ok() {
            return Ok(());
        }
        let plugin_root = crate::util::plugin_root_dir();
        let paths = crate::plugin_runtime::plugin_paths("artifacts", "demo", &plugin_root);
        if !paths.dylib.exists() {
            return Ok(());
        }

        let mut registry = FetcherRegistry::with_builtins();
        let fetcher = registry.fetcher("demo")?;

        let staging = tempfile::tempdir()?;
        let mut sink =
            crate::artifacts::ArtifactSink::new(staging.path().join("sink"), 1024 * 1024)?;
        let source = "demo://example/hello.txt?content=hi-from-plugin";
        let parsed = crate::structs::parse_artifact_source(source)?;
        let limits = FetchLimits::default();
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };
        let fetched = fetcher.fetch(&request, &mut sink)?;
        match fetched.payload {
            FetchedPayload::File { path, .. } => {
                assert_eq!(std::fs::read_to_string(path)?, "hi-from-plugin");
            }
            other => bail!("expected file payload, got {other:?}"),
        }
        Ok(())
    }
}
