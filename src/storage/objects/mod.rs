//! The object-storage layer: the one place a store URL and a credential turn
//! into an `object_store` client.
//!
//! Every object store Legion reads or writes is opened here: a worker's
//! `contentStore` (sealed checkpoint parts, archive manifests and session
//! manifests), `s3://` artifact fetches, the manager's archive reads, and
//! streaming persistence (the WAL replica, the restore a new primary runs,
//! and the lease). Each of those used to build its own S3 client, and the
//! differences were bugs: the lease store was built without conditional puts,
//! so it could never be acquired on real S3, and restore fetches did not sign
//! with the node's `contentStore` credential.
//!
//! - [`Scheme`] is the one table of URL schemes: `s3://` and `file://`.
//! - [`StoreUrl`] parses a store URL into its backend (a bucket and key
//!   prefix, or a directory) and owns the layout under it: the object key
//!   ([`StoreUrl::key`]), the object URL ([`StoreUrl::object_url`]), and
//!   whether another URL is under it ([`StoreUrl::covers`]).
//! - [`ObjectCredential`] is who a store signs as before resolution;
//!   [`resolve_principal`] resolves it through the one order every consumer
//!   shares (explicit keys, `AWS_*` keys, web identity, an ECS task role,
//!   instance metadata) and refuses the configurations that would silently
//!   change the principal.
//! - [`open`] and [`open_s3`] build the client as `Arc<dyn ObjectStore>`, so
//!   no consumer names a backend type. [`OpenOptions`] carries what differs
//!   between consumers: conditional puts, the region default, and whether the
//!   process's other `AWS_*` settings apply.
//!
//! `file://` stores are `object_store`'s `LocalFileSystem`, keyed so an
//! object named `a/b` is the file `<root>/a/b` exactly. Two consumers keep
//! their own filesystem code at the boundary, because `LocalFileSystem`
//! cannot do what they need: session manifests' compare-and-swap
//! (`PutMode::Update` is not implemented for local files, so they keep an
//! `flock`-based backend) and rustyriver's file replica (whose on-disk layout
//! is rustyriver's own). The `file://` artifact fetcher also stays on plain
//! `std::fs`: an artifact source is any local path, directories included.
//!
//! Adding a backend (`gs://`, `az://`) is an `object_store` Cargo feature, a
//! [`Scheme`] arm, a [`Backend`] variant that [`open`] builds, and an
//! [`ObjectCredential`] variant for its key shape.

mod aws;

pub use aws::{
    environment_region, resolve_from_process_env, resolve_principal, AwsKeys, Principal,
    S3Location, FALLBACK_REGION,
};

use std::path::{Path, PathBuf};

use object_store::local::LocalFileSystem;
use object_store::{path::Path as ObjectPath, ObjectStore};
use url::Url;

use crate::artifacts::Credential;
use crate::prelude::*;

/// The URL schemes an object store can be named by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// An S3-compatible bucket (AWS, R2, MinIO, …).
    S3,
    /// A local directory: a shared mount, or a test's temp dir.
    File,
}

impl Scheme {
    /// Every supported scheme, in the order error messages list them.
    pub const ALL: [Scheme; 2] = [Scheme::S3, Scheme::File];

    /// The scheme a URL names, ignoring case as URL schemes do.
    pub fn from_name(name: &str) -> Option<Scheme> {
        match name.to_ascii_lowercase().as_str() {
            "s3" => Some(Scheme::S3),
            "file" => Some(Scheme::File),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::S3 => "s3",
            Scheme::File => "file",
        }
    }
}

fn unsupported_scheme(name: &str) -> anyhow::Error {
    let supported: Vec<String> = Scheme::ALL
        .iter()
        .map(|scheme| format!("{}://", scheme.as_str()))
        .collect();
    anyhow!(
        "scheme {name:?} is not supported; use {}",
        supported.join(" or ")
    )
}

/// What a [`StoreUrl`] points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// A bucket. Object keys are `<prefix>/<name>`; `prefix` is the URL path
    /// without its leading and trailing slashes, as written.
    S3 {
        location: S3Location,
        prefix: String,
    },
    /// A directory. The object `<name>` is the file `<root>/<name>`.
    Local { root: PathBuf },
}

/// A parsed object-store URL: `s3://bucket/optional/prefix?region=…&endpoint=…`
/// or `file:///absolute/dir`. The `region` and `endpoint` query parameters
/// are the only ones an `s3://` URL reads; a `file://` URL takes none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreUrl {
    url: Url,
    backend: Backend,
}

impl StoreUrl {
    /// Parse and validate a store URL, saying what is wrong with it: an
    /// unsupported scheme, an `s3://` URL with no bucket, a `file://` URL
    /// that is not an absolute path or carries a query.
    pub fn parse(raw: &str) -> R<Self> {
        let url = Url::parse(raw.trim()).with_context(|| format!("{raw:?} is not a URL"))?;
        Self::from_url(url)
    }

    /// [`StoreUrl::parse`] for an already-parsed URL.
    pub fn from_url(url: Url) -> R<Self> {
        let scheme =
            Scheme::from_name(url.scheme()).ok_or_else(|| unsupported_scheme(url.scheme()))?;
        let backend = match scheme {
            Scheme::S3 => {
                let bucket = url.host_str().unwrap_or_default();
                if bucket.is_empty() {
                    bail!("{url} has no bucket");
                }
                let mut location = S3Location {
                    bucket: bucket.to_string(),
                    ..S3Location::default()
                };
                // The last of a repeated parameter wins.
                for (name, value) in url.query_pairs() {
                    match name.as_ref() {
                        "region" => location.region = Some(value.into_owned()),
                        "endpoint" => location.endpoint = Some(value.into_owned()),
                        _ => {}
                    }
                }
                Backend::S3 {
                    location,
                    prefix: url.path().trim_matches('/').to_string(),
                }
            }
            Scheme::File => {
                if url.query().is_some_and(|query| !query.is_empty()) {
                    bail!("{url}: file URLs take no query");
                }
                let root = url
                    .to_file_path()
                    .map_err(|_| anyhow!("{url} is not an absolute file path"))?;
                Backend::Local { root }
            }
        };
        Ok(Self { url, backend })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn scheme(&self) -> Scheme {
        match self.backend {
            Backend::S3 { .. } => Scheme::S3,
            Backend::Local { .. } => Scheme::File,
        }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// The directory a `file://` store is, for the consumers that need
    /// filesystem semantics `object_store` does not offer.
    pub fn local_root(&self) -> Option<&Path> {
        match &self.backend {
            Backend::Local { root } => Some(root),
            Backend::S3 { .. } => None,
        }
    }

    /// The `endpoint` an `s3://` URL names. Part of a store's identity: the
    /// same bucket name behind another endpoint is another store.
    pub fn endpoint(&self) -> Option<&str> {
        match &self.backend {
            Backend::S3 { location, .. } => location.endpoint.as_deref(),
            Backend::Local { .. } => None,
        }
    }

    /// Where the object `name` (relative to this URL) lives in the store
    /// [`open`] returns; `""` is the URL itself. An `s3://` key encodes each
    /// segment as `object_store` does (`ObjectPath::from`). A `file://` key
    /// keeps the name as written, so the object `a/b` is the file
    /// `<root>/a/b`; a name with an empty, `.` or `..` segment is refused.
    pub fn key(&self, name: &str) -> R<ObjectPath> {
        match &self.backend {
            Backend::S3 { prefix, .. } => Ok(ObjectPath::from(join(prefix, name))),
            Backend::Local { root } => {
                let base = ObjectPath::from_absolute_path(root)
                    .with_context(|| format!("{} is not a usable directory", root.display()))?;
                let full = join(base.as_ref(), name.trim_start_matches('/'));
                ObjectPath::parse(&full)
                    .with_context(|| format!("{name:?} is not a valid object name"))
            }
        }
    }

    /// The name relative to this URL of a location the store listed: the
    /// inverse of [`StoreUrl::key`]. A location outside the prefix comes back
    /// whole.
    pub fn name_of(&self, location: &ObjectPath) -> String {
        let full = location.as_ref();
        let Ok(base) = self.key("") else {
            return full.to_string();
        };
        if base.as_ref().is_empty() {
            return full.to_string();
        }
        full.strip_prefix(base.as_ref())
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(full)
            .to_string()
    }

    /// Whether a listing comes back in key order, so a paged caller may
    /// stop reading once it has enough. S3 lists in order; a directory walk
    /// does not, so a local listing has to be read whole and sorted.
    pub fn lists_in_order(&self) -> bool {
        matches!(self.backend, Backend::S3 { .. })
    }

    /// The URL of the object `name` under this one. An `s3://` URL keeps its
    /// query (`region`, `endpoint`), so a reader reaches the same endpoint.
    pub fn object_url(&self, name: &str) -> String {
        object_url(&self.url, name)
    }

    /// Whether `other` is this URL or an object under it: the same scheme,
    /// bucket (or directory) and `endpoint`, and a path under this one's at
    /// a `/` boundary. A sibling prefix (`content-old/` beside `content/`)
    /// or another endpoint serving the same bucket name is not covered.
    pub fn covers(&self, other: &StoreUrl) -> bool {
        let prefix = self.url.path().trim_end_matches('/');
        self.scheme() == other.scheme()
            && self.url.host_str() == other.url.host_str()
            && self.endpoint() == other.endpoint()
            && other
                .url
                .path()
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

/// `<prefix>/<name>`, or either alone when the other is empty.
fn join(prefix: &str, name: &str) -> String {
    match (prefix.is_empty(), name.is_empty()) {
        (true, _) => name.to_string(),
        (_, true) => prefix.to_string(),
        _ => format!("{prefix}/{name}"),
    }
}

/// The URL of the object `name` under the store URL `root`, without
/// validating `root` ([`StoreUrl::object_url`] for one that is). An `s3://`
/// root keeps its query; a `file://` root that is not a path yields a path
/// of `name` alone.
pub fn object_url(root: &Url, name: &str) -> String {
    match Scheme::from_name(root.scheme()) {
        Some(Scheme::File) => {
            let path = root
                .to_file_path()
                .map(|path| path.join(name))
                .unwrap_or_else(|_| PathBuf::from(name));
            local_url(&path)
        }
        _ => {
            let mut url = root.clone();
            url.set_path(&format!("/{}", join(root.path().trim_matches('/'), name)));
            url.to_string()
        }
    }
}

/// The `file://` URL of a local path.
pub fn local_url(path: &Path) -> String {
    Url::from_file_path(path)
        .map(|url| url.to_string())
        .unwrap_or_else(|_| format!("file://{}", path.display()))
}

/// A configured location that is a store URL or, for settings that have
/// always taken one, a bare local path (`persistence.streaming.replicaUrl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UrlOrPath<'a> {
    /// An `s3://` URL, as written: the consumer's own dialect parser reads
    /// its details (rustyriver's accepts access-point ARNs and
    /// `forcePathStyle`, which a [`StoreUrl`] does not).
    S3(&'a str),
    /// `file://<path>`, with the prefix stripped as text so a relative path
    /// stays relative, or a bare path.
    Local(PathBuf),
}

/// Classify a URL-or-path setting. Anything of the form `<scheme>://…` must
/// name a supported scheme; anything else is a local path.
pub fn url_or_path(raw: &str) -> R<UrlOrPath<'_>> {
    let raw = raw.trim();
    let is_scheme = |name: &str| {
        name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    };
    match raw.split_once("://") {
        Some((name, rest)) if is_scheme(name) => match Scheme::from_name(name) {
            Some(Scheme::S3) => Ok(UrlOrPath::S3(raw)),
            Some(Scheme::File) => Ok(UrlOrPath::Local(PathBuf::from(rest))),
            None => Err(unsupported_scheme(name)),
        },
        _ => Ok(UrlOrPath::Local(PathBuf::from(raw))),
    }
}

/// Who a store signs as, before [`resolve_principal`] resolves it.
#[derive(Clone, PartialEq, Eq)]
pub enum ObjectCredential {
    /// Unsigned requests: a public bucket.
    Anonymous,
    /// The process's own chain: `AWS_*` keys, web identity, an ECS task
    /// role, then instance metadata.
    Ambient,
    /// Literal AWS keys.
    Aws(AwsCredential),
}

/// Literal AWS keys, and the region and endpoint the credential names (a KV
/// `aws` credential may carry both; the store URL's own win).
#[derive(Clone, PartialEq, Eq)]
pub struct AwsCredential {
    pub keys: AwsKeys,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// Where the keys came from, for error messages:
    /// `persistence.streaming.credentials`, `kv legion/content-store`.
    pub origin: String,
}

impl ObjectCredential {
    /// An artifact credential (a KV `aws` credential, or a node's ambient
    /// chain) as an object-store one. Any other kind is refused, naming
    /// `origin` and never the secret.
    pub fn from_artifact(credential: &Credential, origin: &str) -> R<Self> {
        match credential {
            Credential::Aws {
                access_key_id,
                secret_access_key,
                session_token,
                region,
                endpoint,
            } => Ok(ObjectCredential::Aws(AwsCredential {
                keys: AwsKeys {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    session_token: session_token.clone(),
                },
                region: region.clone(),
                endpoint: endpoint.clone(),
                origin: origin.to_string(),
            })),
            Credential::AwsAmbient => Ok(ObjectCredential::Ambient),
            other => bail!("{origin} requires an aws credential, got {other:?}"),
        }
    }
}

/// Keys are withheld; the origin is an operator-facing name.
impl std::fmt::Debug for ObjectCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectCredential::Anonymous => f.write_str("anonymous"),
            ObjectCredential::Ambient => f.write_str("ambient"),
            ObjectCredential::Aws(aws) => write!(f, "aws({}, redacted)", aws.origin),
        }
    }
}

/// The region a store signs for when neither its URL nor its credential
/// names one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionDefault {
    /// `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `us-east-1`: a node's
    /// own stores, configured by the node's environment.
    Environment,
    /// Always this region, whatever the node's environment says: a jobspec
    /// artifact resolves the same on every node.
    Fixed(&'static str),
}

/// What differs between the consumers of one store type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    /// Send `If-None-Match` / `If-Match` on conditional puts. The lease and
    /// session manifests need it: without it `object_store` answers
    /// `PutMode::Create` and `PutMode::Update` with `NotImplemented` on S3.
    pub conditional_put: bool,
    pub region_default: RegionDefault,
    /// Apply the process's other `AWS_*` store settings — `AWS_ENDPOINT`,
    /// `AWS_ALLOW_HTTP`, server-side encryption, proxy and IMDS settings —
    /// the way `AmazonS3Builder::from_env` does. The principal and the region
    /// never come from here. Streaming persistence turns it off: its config
    /// describes its stores completely.
    pub aws_env_settings: bool,
}

impl Default for OpenOptions {
    /// A node's own store: no conditional puts, the environment's region and
    /// settings.
    fn default() -> Self {
        OpenOptions {
            conditional_put: false,
            region_default: RegionDefault::Environment,
            aws_env_settings: true,
        }
    }
}

/// Open the store `url` names, signing as `credential`. Keys are resolved
/// now, so a caller that opens per operation picks up a rotated credential.
/// A `file://` store takes no credential beyond the process's own access.
pub fn open(
    url: &StoreUrl,
    credential: &ObjectCredential,
    options: &OpenOptions,
) -> R<Arc<dyn ObjectStore>> {
    match &url.backend {
        Backend::S3 { location, .. } => open_s3(location, credential, options),
        Backend::Local { .. } => {
            if let ObjectCredential::Aws(aws) = credential {
                bail!(
                    "{} applies to s3:// stores only, not {}",
                    aws.origin,
                    url.url
                );
            }
            // Rooted at `/`, so the directory need not exist until the
            // first write creates it.
            Ok(Arc::new(LocalFileSystem::new()))
        }
    }
}

/// Open an S3 bucket described by its parts rather than a [`StoreUrl`]: the
/// streaming replica (whose URL dialect is rustyriver's) and the lease (from
/// `lease.objectStorage`).
pub fn open_s3(
    location: &S3Location,
    credential: &ObjectCredential,
    options: &OpenOptions,
) -> R<Arc<dyn ObjectStore>> {
    let principal = aws::resolve_from_process_env(credential)?;
    let decided = aws::plan(
        location,
        credential,
        principal,
        options,
        &aws::process_env,
        aws::process_env_vars(),
    )?;
    Ok(Arc::new(aws::build(decided)?))
}

#[cfg(test)]
mod objects_test {
    use super::*;

    fn s3(raw: &str) -> (S3Location, String) {
        match StoreUrl::parse(raw).unwrap().backend {
            Backend::S3 { location, prefix } => (location, prefix),
            other => panic!("{raw} parsed as {other:?}"),
        }
    }

    #[test]
    fn s3_urls_parse_into_bucket_prefix_and_query_test() {
        let (location, prefix) = s3("s3://bucket");
        assert_eq!(location.bucket, "bucket");
        assert_eq!(prefix, "");
        assert_eq!(location.region, None);
        assert_eq!(location.endpoint, None);

        assert_eq!(s3("s3://bucket/").1, "");
        assert_eq!(s3("s3://bucket/a/b").1, "a/b");
        // Trailing and repeated-edge slashes do not reach the prefix.
        assert_eq!(s3("s3://bucket/a/b/").1, "a/b");
        // The scheme is case-insensitive, as URL schemes are.
        assert_eq!(s3("S3://bucket/x").0.bucket, "bucket");
        assert_eq!(s3("  s3://bucket/x  ").1, "x");

        let (location, prefix) =
            s3("s3://legion-content/usw2?endpoint=http://minio:9000&region=auto&other=1");
        assert_eq!(prefix, "usw2");
        assert_eq!(location.region.as_deref(), Some("auto"));
        assert_eq!(location.endpoint.as_deref(), Some("http://minio:9000"));
        // Only the store's own parameters are read; path style and the like
        // are left to object_store's defaults.
        assert_eq!(location.force_path_style, None);
    }

    #[test]
    fn file_urls_parse_into_a_directory_test() {
        let url = StoreUrl::parse("file:///mnt/store").unwrap();
        assert_eq!(url.scheme(), Scheme::File);
        assert_eq!(url.local_root(), Some(Path::new("/mnt/store")));
        let trailing = StoreUrl::parse("file:///mnt/store/").unwrap();
        assert_eq!(
            trailing.key("sealed/x").unwrap(),
            url.key("sealed/x").unwrap()
        );
        assert_eq!(url.endpoint(), None);
        assert!(!url.lists_in_order());
    }

    #[test]
    fn bad_urls_say_what_is_wrong_test() {
        for (raw, needle) in [
            ("https://x/y", "not supported; use s3:// or file://"),
            ("gs://bucket/x", "\"gs\" is not supported"),
            ("s3:///key", "has no bucket"),
            ("s3://", "has no bucket"),
            ("file://otherhost/x", "not an absolute file path"),
            ("file:///x?y=1", "file URLs take no query"),
            ("relative/path", "is not a URL"),
            ("", "is not a URL"),
        ] {
            let err = StoreUrl::parse(raw).unwrap_err();
            assert!(format!("{err:#}").contains(needle), "{raw}: {err:#}");
        }
    }

    #[test]
    fn keys_and_urls_follow_the_prefix_test() -> R<()> {
        let root = StoreUrl::parse("s3://bucket/content/?endpoint=http://minio:9000")?;
        assert_eq!(root.key("sealed/abc")?.as_ref(), "content/sealed/abc");
        assert_eq!(root.key("")?.as_ref(), "content");
        assert_eq!(
            root.object_url("sealed/abc"),
            "s3://bucket/content/sealed/abc?endpoint=http://minio:9000"
        );
        assert_eq!(
            root.name_of(&root.key("manifests/a/b.json")?),
            "manifests/a/b.json"
        );
        assert!(root.lists_in_order());

        let bucket = StoreUrl::parse("s3://bucket")?;
        assert_eq!(bucket.key("sealed/abc")?.as_ref(), "sealed/abc");
        assert_eq!(bucket.object_url("sealed/abc"), "s3://bucket/sealed/abc");
        assert_eq!(
            bucket.name_of(&ObjectPath::from("sealed/abc")),
            "sealed/abc"
        );

        let dir = StoreUrl::parse("file:///mnt/store")?;
        assert_eq!(dir.key("sealed/abc")?.as_ref(), "mnt/store/sealed/abc");
        assert_eq!(dir.object_url("sealed/abc"), "file:///mnt/store/sealed/abc");
        assert_eq!(
            dir.name_of(&dir.key("sessions/j/ab/x.json")?),
            "sessions/j/ab/x.json"
        );
        // A file key is the name as written, so the on-disk layout is too.
        assert_eq!(dir.key("a~b/c#d")?.as_ref(), "mnt/store/a~b/c#d");
        assert!(dir.key("../escape").is_err());
        // An unvalidated root still yields a URL.
        assert_eq!(
            object_url(&Url::parse("s3://b/p?region=auto")?, "x"),
            "s3://b/p/x?region=auto"
        );
        Ok(())
    }

    #[test]
    fn covers_matches_only_this_store_test() -> R<()> {
        let covers = |root: &str, other: &str| {
            StoreUrl::parse(root)
                .unwrap()
                .covers(&StoreUrl::parse(other).unwrap())
        };
        assert!(covers(
            "s3://bucket/content/",
            "s3://bucket/content/sealed/abc"
        ));
        assert!(covers("s3://bucket/content", "s3://bucket/content"));
        assert!(!covers(
            "s3://bucket/content",
            "s3://bucket/content-old/sealed/abc"
        ));
        assert!(!covers("s3://bucket/content", "s3://bucket/sealed/abc"));
        assert!(!covers(
            "s3://bucket/content",
            "s3://other/content/sealed/abc"
        ));
        assert!(!covers("s3://bucket/content", "file:///content/sealed/abc"));
        assert!(covers(
            "s3://bucket?endpoint=http://minio:9000",
            "s3://bucket/sealed/abc?endpoint=http://minio:9000"
        ));
        assert!(!covers(
            "s3://bucket?endpoint=http://minio:9000",
            "s3://bucket/sealed/abc"
        ));
        assert!(covers("file:///mnt/store", "file:///mnt/store/sealed/abc"));
        assert!(!covers(
            "file:///mnt/store",
            "file:///mnt/storage/sealed/abc"
        ));
        Ok(())
    }

    #[test]
    fn url_or_path_classifies_settings_that_take_paths_test() -> R<()> {
        assert_eq!(
            url_or_path("s3://bucket/prefix")?,
            UrlOrPath::S3("s3://bucket/prefix")
        );
        assert_eq!(
            url_or_path(" S3://bucket/prefix")?,
            UrlOrPath::S3("S3://bucket/prefix")
        );
        // rustyriver's access-point form is passed through untouched.
        let arn = "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/ap/db";
        assert_eq!(url_or_path(arn)?, UrlOrPath::S3(arn));
        assert_eq!(
            url_or_path("file:///var/replica")?,
            UrlOrPath::Local(PathBuf::from("/var/replica"))
        );
        assert_eq!(
            url_or_path("/var/replica")?,
            UrlOrPath::Local(PathBuf::from("/var/replica"))
        );
        assert_eq!(
            url_or_path("replica:dir")?,
            UrlOrPath::Local(PathBuf::from("replica:dir"))
        );
        assert_eq!(url_or_path("")?, UrlOrPath::Local(PathBuf::new()));
        let err = url_or_path("gs://bucket/db").unwrap_err();
        assert!(err.to_string().contains("not supported"), "{err}");
        Ok(())
    }

    #[test]
    fn artifact_credentials_map_onto_object_credentials_test() -> R<()> {
        let kv = Credential::Aws {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            region: Some("auto".into()),
            endpoint: Some("http://minio:9000".into()),
        };
        match ObjectCredential::from_artifact(&kv, "kv legion/content")? {
            ObjectCredential::Aws(aws) => {
                assert_eq!(aws.keys.access_key_id, "AKIA");
                assert_eq!(aws.region.as_deref(), Some("auto"));
                assert_eq!(aws.endpoint.as_deref(), Some("http://minio:9000"));
                assert_eq!(aws.origin, "kv legion/content");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            ObjectCredential::from_artifact(&Credential::AwsAmbient, "x")?,
            ObjectCredential::Ambient
        );
        let bearer = Credential::Bearer {
            token: "tok-secret".into(),
        };
        let err = ObjectCredential::from_artifact(&bearer, "s3 artifact s3://b/k").unwrap_err();
        assert!(err.to_string().contains("aws credential"), "{err}");
        assert!(!format!("{err:#}").contains("tok-secret"), "{err:#}");
        Ok(())
    }

    /// A `file://` store round-trips through `open`, and its objects are the
    /// files the name says, so the layout matches what was there before.
    #[tokio::test]
    async fn file_store_round_trips_through_open_test() -> R<()> {
        use futures::TryStreamExt;

        let dir = tempfile::tempdir()?;
        // The root does not exist yet: the first write creates it.
        let root = dir.path().join("shared");
        let url = StoreUrl::parse(&local_url(&root))?;
        let store = open(&url, &ObjectCredential::Ambient, &OpenOptions::default())?;

        let key = url.key("manifests/agents/g/t/t-1.json")?;
        assert!(matches!(
            store.get(&key).await,
            Err(object_store::Error::NotFound { .. })
        ));
        store.put(&key, b"{}".to_vec().into()).await?;
        assert_eq!(
            std::fs::read(root.join("manifests/agents/g/t/t-1.json"))?,
            b"{}"
        );
        assert_eq!(store.get(&key).await?.bytes().await?.as_ref(), b"{}");
        // A name with characters object_store would otherwise escape lands
        // at the same name on disk.
        let odd = url.key("odd/a~b.json")?;
        store.put(&odd, b"x".to_vec().into()).await?;
        assert!(root.join("odd/a~b.json").exists());

        let listed: Vec<String> = store
            .list(Some(&url.key("manifests")?))
            .map_ok(|meta| url.name_of(&meta.location))
            .try_collect()
            .await?;
        assert_eq!(listed, vec!["manifests/agents/g/t/t-1.json".to_string()]);
        // Listing a prefix nothing was written under is empty, not an error.
        let empty: Vec<_> = store
            .list(Some(&url.key("nothing-here")?))
            .try_collect()
            .await?;
        assert!(empty.is_empty());

        store.delete(&key).await?;
        assert!(!root.join("manifests/agents/g/t/t-1.json").exists());
        assert!(matches!(
            store.delete(&key).await,
            Err(object_store::Error::NotFound { .. })
        ));

        // Explicit keys mean nothing to a directory.
        let keys = ObjectCredential::Aws(AwsCredential {
            keys: AwsKeys {
                access_key_id: "AKIA".into(),
                secret_access_key: "s".into(),
                session_token: None,
            },
            region: None,
            endpoint: None,
            origin: "contentStore.auth.kv".into(),
        });
        let err = open(&url, &keys, &OpenOptions::default())
            .err()
            .expect("refused");
        assert!(err.to_string().contains("s3:// stores only"), "{err}");
        Ok(())
    }

    /// Opening an `s3://` store resolves the credential first, so a broken
    /// ambient chain fails the open instead of signing as someone else.
    #[test]
    fn s3_open_builds_without_network_test() -> R<()> {
        let url = StoreUrl::parse("s3://bucket/prefix?region=auto&endpoint=http://minio:9000")?;
        let store = open(
            &url,
            &ObjectCredential::Anonymous,
            &OpenOptions {
                conditional_put: true,
                ..OpenOptions::default()
            },
        )?;
        let rendered = format!("{store:?}");
        assert!(rendered.contains("minio:9000"), "{rendered}");
        assert!(rendered.contains("ETagMatch"), "{rendered}");
        Ok(())
    }
}
