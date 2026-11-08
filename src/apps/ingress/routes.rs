//! Routes: the `ingress` values jobs declare on their services, compiled
//! into the table every request reads (docs/ingress.md, "Routing").
//!
//! A value arrives over `WatchAppExtensions` as `{ host, path, stripPath }`
//! on one job's task's service. [`RouteSpec::from_value`] checks and
//! normalizes it; [`RouteTable::compile`] turns every active spec into one
//! route per host, resolves conflicts the same way on every replica, and
//! hands each route the [`Backend`] for its service, reused across compiles
//! so endpoint state survives a route change.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::backend::Backend;

/// Most hosts one value may name (the schema's `maxItems`).
pub const MAX_HOSTS: usize = 16;

/// The service a route sends traffic to: one job's task's service.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Target {
    /// The canonical job id (`web`, or `team-a/web`).
    pub job_id: String,
    pub namespace: String,
    pub task: String,
    pub service: String,
}

impl Target {
    /// The name `ResolveService` takes: the bare service in `default`,
    /// `namespace/service` elsewhere.
    pub fn service_ref(&self) -> String {
        if self.namespace.is_empty() || self.namespace == "default" {
            self.service.clone()
        } else {
            format!("{}/{}", self.namespace, self.service)
        }
    }

    /// `job/task/service`, for views and logs.
    pub fn display(&self) -> String {
        format!(
            "{} (task {}, service {})",
            self.job_id, self.task, self.service
        )
    }

    /// The job's bare name.
    pub fn job_name(&self) -> &str {
        crate::structs::namespace::split_job_id(&self.job_id).1
    }
}

/// Where a value sits: what `WatchAppExtensions` keys upserts and deletes
/// by (the job, and the path inside it).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ValueKey {
    pub job_id: String,
    pub group: String,
    pub task: String,
    pub service: String,
}

/// One value, checked and normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    /// Lower-case, no trailing dot; `*.example.com` for a wildcard.
    pub hosts: Vec<String>,
    /// `/`, or a prefix with no trailing slash (`/api`).
    pub path: String,
    pub strip_path: bool,
    pub target: Target,
    /// False for a value its job no longer serves (the job is stopped, or
    /// the value fails the app's current schema): kept for the views, never
    /// routed, and never a claim.
    pub active: bool,
    /// Why the value is not active, or what is wrong with it.
    pub problem: Option<String>,
}

impl RouteSpec {
    /// Read a value's effective JSON (`{ host, path, stripPath }`, defaults
    /// filled) for `target`. Errors name what is wrong, for the validate
    /// call and the views.
    pub fn from_value(value: &serde_json::Value, target: Target) -> Result<RouteSpec, String> {
        let hosts = match value.get("host") {
            Some(serde_json::Value::String(host)) => vec![normalize_host(host)?],
            Some(serde_json::Value::Array(hosts)) => {
                if hosts.is_empty() || hosts.len() > MAX_HOSTS {
                    return Err(format!("host lists 1 to {MAX_HOSTS} names"));
                }
                let mut normalized = Vec::with_capacity(hosts.len());
                for host in hosts {
                    let host = host
                        .as_str()
                        .ok_or_else(|| "every host is a string".to_string())?;
                    let host = normalize_host(host)?;
                    if !normalized.contains(&host) {
                        normalized.push(host);
                    }
                }
                normalized
            }
            _ => return Err("host is required: a name, or a list of names".into()),
        };
        let path = normalize_path(
            value
                .get("path")
                .and_then(|path| path.as_str())
                .unwrap_or("/"),
        )?;
        let strip_path = value
            .get("stripPath")
            .and_then(|strip| strip.as_bool())
            .unwrap_or(false);
        Ok(RouteSpec {
            hosts,
            path,
            strip_path,
            target,
            active: true,
            problem: None,
        })
    }
}

/// Lower-case `host`, drop a trailing dot, and check it is a DNS name or a
/// `*.` wildcard over one.
pub fn normalize_host(host: &str) -> Result<String, String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    let (wildcard, name) = match host.strip_prefix("*.") {
        Some(rest) => (true, rest),
        None => (false, host.as_str()),
    };
    let valid = !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        });
    if !valid || (wildcard && !name.contains('.')) {
        return Err(format!(
            "host {host:?} is not a DNS name (a wildcard is `*.` over a name with a dot, such as *.example.com)"
        ));
    }
    Ok(host)
}

/// `/` or a `/`-led prefix without a trailing slash, query, or fragment.
pub fn normalize_path(path: &str) -> Result<String, String> {
    if !path.starts_with('/') || path.contains(['?', '#']) || path.chars().any(char::is_whitespace)
    {
        return Err(format!(
            "path {path:?} must start with / and hold no query, fragment, or spaces"
        ));
    }
    let trimmed = path.trim_end_matches('/');
    Ok(if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    })
}

/// Whether a request for `path` falls under route prefix `prefix`, on
/// segment boundaries.
pub fn path_matches(prefix: &str, path: &str) -> bool {
    prefix == "/"
        || path == prefix
        || (path.len() > prefix.len()
            && path.starts_with(prefix)
            && path.as_bytes()[prefix.len()] == b'/')
}

/// What reaches the service when `strip` is set: `path` without `prefix`,
/// always `/`-led.
pub fn strip_prefix<'a>(prefix: &str, path: &'a str) -> std::borrow::Cow<'a, str> {
    if prefix == "/" {
        return std::borrow::Cow::Borrowed(path);
    }
    match path.strip_prefix(prefix) {
        Some("") => std::borrow::Cow::Borrowed("/"),
        Some(rest) if rest.starts_with('/') => std::borrow::Cow::Borrowed(rest),
        _ => std::borrow::Cow::Borrowed(path),
    }
}

/// Whether a public CA could issue a certificate for `host`: a name with a
/// dot, not an IP literal, outside the names reserved for local and test
/// use. The ingress asks an ACME Issuer only for these; the others are
/// served over HTTP, from an operator's bundle, or by a private-CA Issuer.
pub fn publicly_issuable(host: &str) -> bool {
    const RESERVED: [&str; 9] = [
        "localhost",
        "local",
        "internal",
        "test",
        "example",
        "invalid",
        "home.arpa",
        "lan",
        "localdomain",
    ];
    if host.starts_with("*.") || !host.contains('.') || host.parse::<std::net::IpAddr>().is_ok() {
        return false;
    }
    !RESERVED
        .iter()
        .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

/// One host's route to one backend.
#[derive(Debug)]
pub struct Route {
    pub host: String,
    pub path: String,
    pub strip_path: bool,
    pub backend: Arc<Backend>,
}

/// One row of what the table knows, for the views and the validate call:
/// every host of every value, active or not.
#[derive(Debug, Clone)]
pub struct RouteRow {
    pub host: String,
    pub path: String,
    pub strip_path: bool,
    pub target: Target,
    /// Served: active and the winner of its host and path.
    pub served: bool,
    pub problem: Option<String>,
}

/// The compiled routes, swapped whole on every change.
#[derive(Debug, Default)]
pub struct RouteTable {
    /// Exact host to its routes, longest path first.
    exact: HashMap<String, Vec<Arc<Route>>>,
    /// `*.parent` routes, keyed by `parent`.
    wildcard: HashMap<String, Vec<Arc<Route>>>,
    /// Every value's hosts, for the views and validate.
    pub rows: Vec<RouteRow>,
}

impl RouteTable {
    /// Compile `specs`. A `(host, path)` claimed by several active values
    /// goes to the lowest target (job, task, service), the same choice on
    /// every replica; the others are reported. `backends` keeps one
    /// [`Backend`] per target across compiles: targets no route uses any
    /// more are dropped from it.
    pub fn compile<'a>(
        specs: impl IntoIterator<Item = &'a RouteSpec>,
        backends: &mut HashMap<Target, Arc<Backend>>,
    ) -> RouteTable {
        // (host, path) -> every claim on it, lowest target first.
        let mut claims: BTreeMap<(String, String), Vec<&RouteSpec>> = BTreeMap::new();
        let mut rows = Vec::new();
        for spec in specs {
            for host in &spec.hosts {
                if spec.active {
                    claims
                        .entry((host.clone(), spec.path.clone()))
                        .or_default()
                        .push(spec);
                } else {
                    rows.push(RouteRow {
                        host: host.clone(),
                        path: spec.path.clone(),
                        strip_path: spec.strip_path,
                        target: spec.target.clone(),
                        served: false,
                        problem: spec.problem.clone(),
                    });
                }
            }
        }
        let mut table = RouteTable::default();
        let mut used = std::collections::HashSet::new();
        for ((host, path), mut specs) in claims {
            specs.sort_by(|a, b| a.target.cmp(&b.target));
            let winner = specs[0];
            for loser in &specs[1..] {
                rows.push(RouteRow {
                    host: host.clone(),
                    path: path.clone(),
                    strip_path: loser.strip_path,
                    target: loser.target.clone(),
                    served: false,
                    problem: Some(format!(
                        "{host}{path} is already routed to {}",
                        winner.target.display()
                    )),
                });
            }
            let backend = backends
                .entry(winner.target.clone())
                .or_insert_with(|| Arc::new(Backend::new(winner.target.clone())))
                .clone();
            used.insert(winner.target.clone());
            let route = Arc::new(Route {
                host: host.clone(),
                path: path.clone(),
                strip_path: winner.strip_path,
                backend,
            });
            rows.push(RouteRow {
                host: host.clone(),
                path: path.clone(),
                strip_path: winner.strip_path,
                target: winner.target.clone(),
                served: true,
                problem: winner.problem.clone(),
            });
            let (map, key) = match host.strip_prefix("*.") {
                Some(parent) => (&mut table.wildcard, parent.to_string()),
                None => (&mut table.exact, host),
            };
            map.entry(key).or_default().push(route);
        }
        for routes in table.exact.values_mut().chain(table.wildcard.values_mut()) {
            // Longest prefix first; `/` last.
            routes.sort_by_key(|route| std::cmp::Reverse(route.path.len()));
        }
        backends.retain(|target, _| used.contains(target));
        rows.sort_by(|a, b| {
            (a.host.as_str(), a.path.as_str(), !a.served, &a.target).cmp(&(
                b.host.as_str(),
                b.path.as_str(),
                !b.served,
                &b.target,
            ))
        });
        table.rows = rows;
        table
    }

    /// The route for a request to `host` (lower-case, no port) and `path`:
    /// the exact host's longest matching prefix, else its `*.` parent's.
    pub fn find(&self, host: &str, path: &str) -> Option<&Arc<Route>> {
        fn matching<'t>(routes: &'t [Arc<Route>], path: &str) -> Option<&'t Arc<Route>> {
            routes.iter().find(|route| path_matches(&route.path, path))
        }
        if let Some(route) = self
            .exact
            .get(host)
            .and_then(|routes| matching(routes, path))
        {
            return Some(route);
        }
        let (_, parent) = host.split_once('.')?;
        self.wildcard
            .get(parent)
            .and_then(|routes| matching(routes, path))
    }

    /// Every route's backend (a backend shared by several routes repeats).
    pub fn backends(&self) -> impl Iterator<Item = &Arc<Backend>> {
        self.exact
            .values()
            .chain(self.wildcard.values())
            .flatten()
            .map(|route| &route.backend)
    }

    /// Whether any route names `host`, exactly or by wildcard.
    pub fn knows_host(&self, host: &str) -> bool {
        self.exact.contains_key(host)
            || host
                .split_once('.')
                .is_some_and(|(_, parent)| self.wildcard.contains_key(parent))
    }

    /// Every served host, exact names and `*.` wildcards, once each: what
    /// the ingress asks the cert manager to keep certificates for.
    pub fn served_hosts(&self) -> Vec<String> {
        let mut hosts: Vec<String> = self
            .exact
            .keys()
            .cloned()
            .chain(self.wildcard.keys().map(|parent| format!("*.{parent}")))
            .collect();
        hosts.sort();
        hosts
    }

    /// The served route on `(host, path)`, for the validate call.
    pub fn owner(&self, host: &str, path: &str) -> Option<&RouteRow> {
        self.rows
            .iter()
            .find(|row| row.served && row.host == host && row.path == path)
    }

    /// How many routes are served.
    pub fn served(&self) -> usize {
        self.rows.iter().filter(|row| row.served).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target(job: &str) -> Target {
        Target {
            job_id: job.to_string(),
            namespace: crate::structs::namespace::job_namespace(job).to_string(),
            task: "web".to_string(),
            service: "web".to_string(),
        }
    }

    fn spec(job: &str, value: serde_json::Value) -> RouteSpec {
        RouteSpec::from_value(&value, target(job)).expect("valid")
    }

    #[test]
    fn values_are_normalized_and_checked() {
        let spec = spec(
            "shop",
            json!({ "host": ["Shop.Example.com.", "shop.example.com", "*.Example.com"], "path": "/api/" }),
        );
        assert_eq!(spec.hosts, vec!["shop.example.com", "*.example.com"]);
        assert_eq!(spec.path, "/api");
        assert!(!spec.strip_path);

        let bad =
            |value: serde_json::Value| RouteSpec::from_value(&value, target("x")).unwrap_err();
        assert!(bad(json!({})).contains("host is required"));
        assert!(bad(json!({ "host": "bad_host.com" })).contains("not a DNS name"));
        assert!(bad(json!({ "host": "*.com" })).contains("not a DNS name"));
        assert!(bad(json!({ "host": "-a.com" })).contains("not a DNS name"));
        assert!(bad(json!({ "host": "a.com", "path": "api" })).contains("must start with /"));
        assert!(bad(json!({ "host": "a.com", "path": "/a?b" })).contains("query"));
        assert!(bad(json!({ "host": [] })).contains("1 to 16"));
    }

    #[test]
    fn paths_match_on_segment_boundaries_and_strip_cleanly() {
        assert!(path_matches("/", "/anything"));
        assert!(path_matches("/api", "/api"));
        assert!(path_matches("/api", "/api/users"));
        assert!(!path_matches("/api", "/apix"));
        assert!(!path_matches("/api", "/"));
        assert_eq!(strip_prefix("/api", "/api/users"), "/users");
        assert_eq!(strip_prefix("/api", "/api"), "/");
        assert_eq!(strip_prefix("/", "/x"), "/x");
    }

    #[test]
    fn the_longest_prefix_wins_and_wildcards_fill_in() {
        let specs = vec![
            spec("site", json!({ "host": "example.com" })),
            spec("api", json!({ "host": "example.com", "path": "/api" })),
            spec("tenants", json!({ "host": "*.example.com" })),
        ];
        let mut backends = HashMap::new();
        let table = RouteTable::compile(&specs, &mut backends);
        let job = |host: &str, path: &str| {
            table
                .find(host, path)
                .map(|route| route.backend.target.job_id.clone())
        };
        assert_eq!(job("example.com", "/api/v1"), Some("api".into()));
        assert_eq!(job("example.com", "/apiary"), Some("site".into()));
        assert_eq!(job("example.com", "/"), Some("site".into()));
        assert_eq!(job("acme.example.com", "/"), Some("tenants".into()));
        assert_eq!(
            job("a.b.example.com", "/"),
            None,
            "a wildcard matches one label"
        );
        assert_eq!(job("other.com", "/"), None);
        assert_eq!(backends.len(), 3);
        assert_eq!(
            table.served_hosts(),
            vec!["*.example.com".to_string(), "example.com".to_string()]
        );
        assert!(table.knows_host("acme.example.com"));
    }

    #[test]
    fn a_conflict_goes_to_the_lowest_target_on_every_replica() {
        let specs = vec![
            spec("zeta", json!({ "host": "a.example.com" })),
            spec("alpha", json!({ "host": "a.example.com" })),
        ];
        let mut backends = HashMap::new();
        let table = RouteTable::compile(&specs, &mut backends);
        let route = table.find("a.example.com", "/").expect("routed");
        assert_eq!(route.backend.target.job_id, "alpha");
        let loser = table
            .rows
            .iter()
            .find(|row| !row.served)
            .expect("the loser is reported");
        assert_eq!(loser.target.job_id, "zeta");
        assert!(loser
            .problem
            .as_deref()
            .unwrap()
            .contains("already routed to alpha"));
        assert_eq!(backends.len(), 1, "the loser gets no backend");
        assert_eq!(
            table.owner("a.example.com", "/").unwrap().target.job_id,
            "alpha"
        );
    }

    #[test]
    fn inactive_values_are_listed_and_never_claim() {
        let mut stopped = spec("stopped", json!({ "host": "a.example.com" }));
        stopped.active = false;
        stopped.problem = Some("the job is stopped".into());
        let specs = vec![stopped, spec("live", json!({ "host": "a.example.com" }))];
        let mut backends = HashMap::new();
        let table = RouteTable::compile(&specs, &mut backends);
        assert_eq!(
            table
                .find("a.example.com", "/")
                .unwrap()
                .backend
                .target
                .job_id,
            "live"
        );
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.served(), 1);
    }

    #[test]
    fn backends_survive_recompiles_and_go_with_their_last_route() {
        let mut backends = HashMap::new();
        let one = vec![spec("shop", json!({ "host": "a.example.com" }))];
        let first = RouteTable::compile(&one, &mut backends);
        let backend = first.find("a.example.com", "/").unwrap().backend.clone();
        let two = vec![spec(
            "shop",
            json!({ "host": ["a.example.com", "b.example.com"] }),
        )];
        let second = RouteTable::compile(&two, &mut backends);
        assert!(Arc::ptr_eq(
            &backend,
            &second.find("b.example.com", "/").unwrap().backend
        ));
        RouteTable::compile(&Vec::<RouteSpec>::new(), &mut backends);
        assert!(backends.is_empty());
    }

    #[test]
    fn only_public_names_are_issuable() {
        for host in ["shop.example.com", "a.b.co.uk", "x.dev"] {
            assert!(publicly_issuable(host), "{host}");
        }
        for host in [
            "localhost",
            "app.localhost",
            "intranet",
            "svc.internal",
            "shop.test",
            "10.0.0.1",
            "*.example.com",
            "printer.local",
            "router.home.arpa",
            "example",
        ] {
            assert!(!publicly_issuable(host), "{host}");
        }
    }

    #[test]
    fn the_service_ref_names_the_namespace_outside_default() {
        assert_eq!(target("shop").service_ref(), "web");
        assert_eq!(target("team-a/shop").service_ref(), "team-a/web");
        assert_eq!(target("team-a/shop").job_name(), "shop");
    }
}
