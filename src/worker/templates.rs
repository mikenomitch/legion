//! Config-file rendering from the connect datapath (docs/service-discovery.md,
//! "Templates"): `connect.templates` entries render a file into the task
//! directory from the task's upstreams and their live endpoint feed, and
//! re-render on every heartbeat that changes the result — signalling or
//! restarting the task if the entry asks for it.
//!
//! The language is deliberately tiny — three functions and one loop — so a
//! template is legible to anyone who has seen consul-template without
//! anyone having to learn it:
//!
//! ```text
//! db: {{ service "db" }}                     # host:port the task dials
//! host: {{ service "db" "host" }}            # just the host (a VIP or 127.0.0.1)
//! {{ range endpoints "db" }}
//! server {{ .address }}:{{ .port }} # node={{ .node }} tags={{ .tags }}
//! {{ end }}
//! token: {{ env "LEGION_ALLOC_ID" }}
//! ```
//!
//! `service` renders the upstream address the datapath serves — every
//! connection still goes through the node's forwarder, so policy, handlers
//! and the graph apply. `endpoints` iterates the healthy endpoints the
//! manager last fed for the upstream (fail-static: a manager outage leaves
//! them, and the file, as they were). A template naming an upstream the
//! task does not declare fails at submit.
//!
//! Change handling per entry: `coalesce` waits for the feed to settle before
//! acting, `splay` adds a random delay so a fleet does not reload in
//! lockstep, and `changeMode` is `noop` (rewrite only), `signal`
//! (`changeSignal`), or `restart`.

use crate::prelude::*;
use crate::structs::netpolicy::parse_duration;
use crate::structs::{AllocNetwork, ConnectTemplate};
use crate::worker::balance::Endpoint;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// What a template can see.
pub struct TemplateContext<'a> {
    /// The allocation's assigned network (upstream addresses).
    pub network: Option<&'a AllocNetwork>,
    /// Endpoint feed per upstream key.
    pub endpoints: &'a BTreeMap<String, Vec<Endpoint>>,
    /// The task's environment (`LEGION_*` and declared env).
    pub env: &'a BTreeMap<String, String>,
}

/// Upstream keys a template refers to (`service "x"`, `endpoints "x"`), for
/// admission-time validation.
pub fn referenced_upstreams(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in tokens(source) {
        if let Token::Action(action) = token {
            let words = split_words(&action);
            match words.first().map(String::as_str) {
                Some("service") | Some("endpoints") => {
                    if let Some(key) = words.get(1) {
                        out.push(key.clone());
                    }
                }
                Some("range") if words.get(1).map(String::as_str) == Some("endpoints") => {
                    if let Some(key) = words.get(2) {
                        out.push(key.clone());
                    }
                }
                _ => {}
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Every action a template uses, so validation can reject unknown ones
/// with the line in the message.
pub fn validate_source(source: &str) -> R<()> {
    let mut depth = 0usize;
    for token in tokens(source) {
        let Token::Action(action) = token else {
            continue;
        };
        let words = split_words(&action);
        match words.first().map(String::as_str) {
            Some("service") => {
                if words.len() < 2 || words.len() > 3 {
                    bail!("`{{{{ {action} }}}}`: service takes an upstream key and an optional part (host|port)");
                }
                if let Some(part) = words.get(2) {
                    if part != "host" && part != "port" {
                        bail!("`{{{{ {action} }}}}`: the part must be host or port, got {part:?}");
                    }
                }
            }
            Some("env") => {
                if words.len() != 2 {
                    bail!("`{{{{ {action} }}}}`: env takes exactly one name");
                }
            }
            Some("range") => {
                if words.get(1).map(String::as_str) != Some("endpoints") || words.len() != 3 {
                    bail!("`{{{{ {action} }}}}`: only `range endpoints \"<upstream>\"` is supported");
                }
                depth += 1;
            }
            Some("end") => {
                if depth == 0 {
                    bail!("`{{{{ end }}}}` without a matching `{{{{ range ... }}}}`");
                }
                depth -= 1;
            }
            Some(".address") | Some(".port") | Some(".node") | Some(".tags") | Some(".allocation") => {
                if depth == 0 {
                    bail!("`{{{{ {action} }}}}` is only valid inside `{{{{ range endpoints ... }}}}`");
                }
            }
            Some(other) => bail!(
                "`{{{{ {action} }}}}`: unknown template action {other:?}; use service, endpoints (in a range), env"
            ),
            None => bail!("empty `{{{{ }}}}` action"),
        }
    }
    if depth != 0 {
        bail!("`{{{{ range ... }}}}` without a matching `{{{{ end }}}}`");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Text(String),
    Action(String),
}

fn tokens(source: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        if open > 0 {
            out.push(Token::Text(rest[..open].to_string()));
        }
        let after = &rest[open + 2..];
        match after.find("}}") {
            Some(close) => {
                out.push(Token::Action(after[..close].trim().to_string()));
                rest = &after[close + 2..];
            }
            None => {
                out.push(Token::Text(rest[open..].to_string()));
                rest = "";
                break;
            }
        }
    }
    if !rest.is_empty() {
        out.push(Token::Text(rest.to_string()));
    }
    out
}

/// Split an action into words, honouring double quotes.
fn split_words(action: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for ch in action.chars() {
        match ch {
            '"' => quoted = !quoted,
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// The address a task dials for an upstream: [`AllocUpstream::dial`], the
/// same function `LEGION_UPSTREAM_<KEY>` is built from, so the two cannot
/// disagree.
///
/// [`AllocUpstream::dial`]: crate::structs::AllocUpstream::dial
fn service_address(network: Option<&AllocNetwork>, key: &str) -> Option<(String, u16)> {
    network?.upstream(key)?.dial()
}

/// Render `source` against the context.
pub fn render(source: &str, context: &TemplateContext<'_>) -> R<String> {
    let tokens = tokens(source);
    let mut out = String::with_capacity(source.len());
    let mut index = 0;
    while index < tokens.len() {
        match &tokens[index] {
            Token::Text(text) => out.push_str(text),
            Token::Action(action) => {
                let words = split_words(action);
                match words.first().map(String::as_str) {
                    Some("service") => {
                        let key = words.get(1).context("service needs an upstream key")?;
                        let (host, port) =
                            service_address(context.network, key).with_context(|| {
                                format!("upstream {key:?} is not declared on this task")
                            })?;
                        match words.get(2).map(String::as_str) {
                            Some("host") => out.push_str(&host),
                            Some("port") => out.push_str(&port.to_string()),
                            _ => out.push_str(&format!("{host}:{port}")),
                        }
                    }
                    Some("env") => {
                        let name = words.get(1).context("env needs a name")?;
                        out.push_str(context.env.get(name).map(String::as_str).unwrap_or(""));
                    }
                    Some("range") => {
                        let key = words
                            .get(2)
                            .context("range endpoints needs an upstream key")?;
                        // Collect the body up to the matching end.
                        let mut body = Vec::new();
                        let mut depth = 1;
                        let mut cursor = index + 1;
                        while cursor < tokens.len() {
                            match &tokens[cursor] {
                                Token::Action(a) if a.starts_with("range ") => depth += 1,
                                Token::Action(a) if a == "end" => {
                                    depth -= 1;
                                    if depth == 0 {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                            body.push(tokens[cursor].clone());
                            cursor += 1;
                        }
                        if depth != 0 {
                            bail!("`{{{{ range endpoints \"{key}\" }}}}` without a matching `{{{{ end }}}}`");
                        }
                        let endpoints = context.endpoints.get(key).cloned().unwrap_or_default();
                        for endpoint in &endpoints {
                            out.push_str(&render_body(&body, endpoint)?);
                        }
                        index = cursor;
                    }
                    Some(other) => bail!("unknown template action {other:?}"),
                    None => {}
                }
            }
        }
        index += 1;
    }
    Ok(out)
}

fn render_body(body: &[Token], endpoint: &Endpoint) -> R<String> {
    let (address, port) = match endpoint.addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (endpoint.addr.clone(), String::new()),
    };
    let mut out = String::new();
    for token in body {
        match token {
            Token::Text(text) => out.push_str(text),
            Token::Action(action) => match action.as_str() {
                ".address" => out.push_str(&address),
                ".port" => out.push_str(&port),
                ".node" => out.push_str(&endpoint.node_id),
                ".allocation" => out.push_str(&endpoint.allocation_id.to_string()),
                ".tags" => out.push_str(&endpoint.tags.join(",")),
                other => bail!("unknown action {other:?} inside range"),
            },
        }
    }
    Ok(out)
}

/// One rendered file's state.
#[derive(Debug, Clone)]
struct Rendered {
    hash: u64,
    last_action: Option<Instant>,
    /// A change seen while inside `coalesce`; acted on once it elapses.
    pending: bool,
}

/// What the engine decided to do after rendering, for the daemon to apply
/// (the engine never holds the task manager lock itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeAction {
    Signal { signal: String, splay: Duration },
    Restart { splay: Duration },
}

/// Per-allocation template state on a worker.
#[derive(Default)]
pub struct TemplateEngine {
    rendered: Mutex<HashMap<(i64, String), Rendered>>,
    problems: Mutex<HashMap<i64, Vec<String>>>,
}

fn hash_of(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

/// Where a template lands on the host: relative to the task directory, or
/// an absolute path inside the runtime's task-dir mount.
pub fn destination_path(
    destination: &str,
    in_task_root: Option<&str>,
    host_task_dir: &Path,
) -> R<PathBuf> {
    let relative = match in_task_root {
        Some(root) => crate::task_plugins::resolve_task_dir_destination(root, destination)
            .map_err(|err| anyhow::anyhow!("{err}"))?,
        None => {
            let path = Path::new(destination.trim());
            if path.is_absolute() {
                return Ok(path.to_path_buf());
            }
            if path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                bail!("template destination {destination:?} climbs out of the task directory");
            }
            path.to_path_buf()
        }
    };
    Ok(host_task_dir.join(relative))
}

impl TemplateEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Render every template of an allocation, write the ones whose content
    /// changed, and return the change actions to apply. `first` marks the
    /// render before the task starts: files are written, nothing is
    /// signalled.
    #[allow(clippy::too_many_arguments)]
    pub fn render_allocation(
        &self,
        allocation_id: i64,
        templates: &[ConnectTemplate],
        context: &TemplateContext<'_>,
        in_task_root: Option<&str>,
        host_task_dir: &Path,
        first: bool,
    ) -> Vec<ChangeAction> {
        let mut actions = Vec::new();
        let mut problems = Vec::new();
        for template in templates {
            let key = (allocation_id, template.name.clone());
            let content = match render(&template.source, context) {
                Ok(content) => content,
                Err(err) => {
                    problems.push(format!("template {}: {err:#}", template.name));
                    continue;
                }
            };
            let path = match destination_path(&template.destination, in_task_root, host_task_dir) {
                Ok(path) => path,
                Err(err) => {
                    problems.push(format!("template {}: {err:#}", template.name));
                    continue;
                }
            };
            let hash = hash_of(&content);
            let mut rendered = self.rendered.lock().unwrap_or_else(|p| p.into_inner());
            let entry = rendered.entry(key).or_insert(Rendered {
                hash: 0,
                last_action: None,
                pending: false,
            });
            let changed = entry.hash != hash || first;
            if !changed && !entry.pending {
                continue;
            }
            if changed {
                if let Err(err) = write_rendered(host_task_dir, &path, &content) {
                    problems.push(format!(
                        "template {}: write {}: {err:#}",
                        template.name,
                        path.display()
                    ));
                    continue;
                }
                entry.hash = hash;
            }
            if first {
                entry.last_action = Some(Instant::now());
                entry.pending = false;
                continue;
            }
            // Coalesce: act once the feed has been quiet for the window.
            let coalesce = template
                .coalesce
                .as_deref()
                .and_then(|c| parse_duration(c).ok())
                .unwrap_or(Duration::ZERO);
            let due = entry
                .last_action
                .map(|last| last.elapsed() >= coalesce)
                .unwrap_or(true);
            if !due {
                entry.pending = true;
                continue;
            }
            entry.pending = false;
            entry.last_action = Some(Instant::now());
            let splay = template
                .splay
                .as_deref()
                .and_then(|s| parse_duration(s).ok())
                .unwrap_or(Duration::ZERO);
            match template.change_mode.as_deref().unwrap_or("noop") {
                "signal" => actions.push(ChangeAction::Signal {
                    signal: template
                        .change_signal
                        .clone()
                        .unwrap_or_else(|| "SIGHUP".to_string()),
                    splay,
                }),
                "restart" => actions.push(ChangeAction::Restart { splay }),
                _ => {}
            }
        }
        let mut all = self.problems.lock().unwrap_or_else(|p| p.into_inner());
        if problems.is_empty() {
            all.remove(&allocation_id);
        } else {
            all.insert(allocation_id, problems);
        }
        actions
    }

    /// Render problems for the datapath status report.
    pub fn problems(&self, allocation_id: i64) -> Vec<String> {
        self.problems
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&allocation_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Forget allocations that left the node.
    pub fn retain(&self, live: &std::collections::HashSet<i64>) {
        self.rendered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(id, _), _| live.contains(id));
        self.problems
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|id, _| live.contains(id));
    }
}

/// Write a rendered template. Below the task directory, which the task can
/// write while the worker re-renders into it, nothing on the way is followed
/// ([`crate::util::write_file_beneath`]); elsewhere (an `exec` task's
/// absolute destination) the path is written as given.
fn write_rendered(host_task_dir: &Path, path: &Path, content: &str) -> R<()> {
    match path.strip_prefix(host_task_dir) {
        Ok(relative) => {
            crate::util::write_file_beneath(host_task_dir, relative, content.as_bytes(), 0o644)
        }
        Err(_) => write_atomic(path, content),
    }
}

/// Write via a temp file and rename, so a reader never sees a torn file.
fn write_atomic(path: &Path, content: &str) -> R<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.legion-tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("template")
    ));
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::AllocUpstream;

    fn network() -> AllocNetwork {
        AllocNetwork {
            upstreams: vec![
                AllocUpstream {
                    key: "db".into(),
                    local_port: Some(20001),
                    balance: "round_robin".into(),
                    to: None,
                    vip: Some("127.42.3.2".into()),
                    canonical_port: Some(5432),
                },
                AllocUpstream {
                    key: "cache".into(),
                    local_port: Some(20002),
                    balance: "round_robin".into(),
                    to: None,
                    vip: None,
                    canonical_port: None,
                },
            ],
            ..Default::default()
        }
    }

    fn endpoints() -> BTreeMap<String, Vec<Endpoint>> {
        let mut out = BTreeMap::new();
        out.insert(
            "db".to_string(),
            vec![
                Endpoint {
                    addr: "10.0.0.1:5432".into(),
                    allocation_id: 1,
                    node_id: "node-a".into(),
                    tags: vec!["primary".into()],
                    mesh_tls: false,
                },
                Endpoint {
                    addr: "10.0.0.2:5432".into(),
                    allocation_id: 2,
                    node_id: "node-b".into(),
                    tags: vec![],
                    mesh_tls: false,
                },
            ],
        );
        out
    }

    #[test]
    fn renders_service_endpoints_and_env() -> R<()> {
        let network = network();
        let endpoints = endpoints();
        let mut env = BTreeMap::new();
        env.insert("LEGION_ALLOC_ID".to_string(), "7".to_string());
        let context = TemplateContext {
            network: Some(&network),
            endpoints: &endpoints,
            env: &env,
        };
        let source = "db={{ service \"db\" }} host={{ service \"db\" \"host\" }} port={{ service \"cache\" \"port\" }}\n\
                      {{ range endpoints \"db\" }}- {{ .address }}:{{ .port }} node={{ .node }} tags={{ .tags }} alloc={{ .allocation }}\n{{ end }}\
                      id={{ env \"LEGION_ALLOC_ID\" }} missing=[{{ env \"NOPE\" }}]";
        let rendered = render(source, &context)?;
        assert_eq!(
            rendered,
            "db=127.42.3.2:5432 host=127.42.3.2 port=20002\n\
             - 10.0.0.1:5432 node=node-a tags=primary alloc=1\n\
             - 10.0.0.2:5432 node=node-b tags= alloc=2\n\
             id=7 missing=[]"
        );
        assert_eq!(
            referenced_upstreams(source),
            vec!["cache".to_string(), "db".to_string()]
        );
        Ok(())
    }

    #[test]
    fn validation_names_the_bad_action() {
        assert!(validate_source("{{ service \"db\" }}").is_ok());
        assert!(validate_source("{{ range endpoints \"db\" }}{{ .address }}{{ end }}").is_ok());
        let err = validate_source("{{ frobnicate }}").unwrap_err().to_string();
        assert!(err.contains("unknown template action"), "{err}");
        let err = validate_source("{{ range endpoints \"db\" }}{{ .address }}")
            .unwrap_err()
            .to_string();
        assert!(err.contains("without a matching"), "{err}");
        let err = validate_source("{{ .address }}").unwrap_err().to_string();
        assert!(err.contains("only valid inside"), "{err}");
        let err = validate_source("{{ service \"db\" \"socket\" }}")
            .unwrap_err()
            .to_string();
        assert!(err.contains("host or port"), "{err}");
    }

    #[test]
    fn undeclared_upstream_is_a_render_error() {
        let network = network();
        let endpoints = endpoints();
        let env = BTreeMap::new();
        let context = TemplateContext {
            network: Some(&network),
            endpoints: &endpoints,
            env: &env,
        };
        let err = render("{{ service \"nope\" }}", &context)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not declared"), "{err}");
    }

    #[test]
    fn engine_writes_on_change_and_honours_coalesce_and_modes() -> R<()> {
        let dir = tempfile::TempDir::new()?;
        let engine = TemplateEngine::new();
        let network = network();
        let mut endpoints = endpoints();
        let env = BTreeMap::new();
        let templates = vec![
            ConnectTemplate {
                name: "haproxy".into(),
                destination: "local/backends.cfg".into(),
                source: "{{ range endpoints \"db\" }}server {{ .address }}\n{{ end }}".into(),
                change_mode: Some("signal".into()),
                change_signal: Some("SIGHUP".into()),
                splay: Some("0s".into()),
                coalesce: Some("10s".into()),
            },
            ConnectTemplate {
                name: "static".into(),
                destination: "local/db.txt".into(),
                source: "{{ service \"db\" }}".into(),
                change_mode: None,
                change_signal: None,
                splay: None,
                coalesce: None,
            },
        ];
        let context = TemplateContext {
            network: Some(&network),
            endpoints: &endpoints,
            env: &env,
        };
        // First render: files exist, nothing is signalled.
        let actions = engine.render_allocation(7, &templates, &context, None, dir.path(), true);
        assert!(actions.is_empty());
        let backends = std::fs::read_to_string(dir.path().join("local/backends.cfg"))?;
        assert_eq!(backends, "server 10.0.0.1\nserver 10.0.0.2\n");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("local/db.txt"))?,
            "127.42.3.2:5432"
        );
        assert!(engine.problems(7).is_empty());

        // Unchanged feed: nothing happens.
        let actions = engine.render_allocation(7, &templates, &context, None, dir.path(), false);
        assert!(actions.is_empty());

        // The feed changes inside the coalesce window: the file is rewritten
        // at once, the signal waits for the window.
        endpoints.get_mut("db").unwrap().pop();
        let context = TemplateContext {
            network: Some(&network),
            endpoints: &endpoints,
            env: &env,
        };
        let actions = engine.render_allocation(7, &templates, &context, None, dir.path(), false);
        assert!(actions.is_empty(), "inside coalesce: {actions:?}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("local/backends.cfg"))?,
            "server 10.0.0.1\n"
        );
        // Force the window closed and the pending change is acted on.
        {
            let mut rendered = engine.rendered.lock().unwrap();
            for entry in rendered.values_mut() {
                entry.last_action = Some(Instant::now() - Duration::from_secs(60));
            }
        }
        let actions = engine.render_allocation(7, &templates, &context, None, dir.path(), false);
        assert_eq!(
            actions,
            vec![ChangeAction::Signal {
                signal: "SIGHUP".into(),
                splay: Duration::ZERO
            }]
        );

        // A destination that climbs out is a problem, not a write.
        let bad = vec![ConnectTemplate {
            name: "evil".into(),
            destination: "../../etc/passwd".into(),
            source: "x".into(),
            ..Default::default()
        }];
        engine.render_allocation(7, &bad, &context, None, dir.path(), true);
        assert!(engine.problems(7)[0].contains("climbs out"));
        Ok(())
    }
}
