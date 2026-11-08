//! The ingress's UI (docs/ingress.md, "Seeing it"): the Ingress page in the
//! sidebar (`routes`) and the panel on a job's page (`job`). Both need
//! `job:read`; the page lists only the namespaces the viewer may read, and
//! only the certificates for hosts it lists.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::certificates::CertState;
use super::certs::LoadedCert;
use super::edge::{DnsCheck, EdgeAddress};
use super::routes::RouteRow;
use super::State;
use crate::apps::ui::{UiRegistry, Viewer};
use crate::structs::app_ui::{
    AppView, Block, Cell, Column, ColumnType, KeyValueBlock, KeyValueItem, NoticeBlock, RichText,
    Stat, StatsBlock, TableBlock, Tone,
};

/// Register the views.
pub fn register(ui: &UiRegistry, state: Arc<State>) {
    let page = state.clone();
    ui.view("routes", move |request| {
        let state = page.clone();
        async move { Ok(routes_view(&state, &request.viewer)) }
    });
    ui.view("job", move |request| {
        let state = state.clone();
        async move {
            let Some(job) = request.context.job_id() else {
                return Ok(AppView::hidden());
            };
            Ok(job_view(&state, &job))
        }
    });
}

fn text(value: impl Into<String>) -> Cell {
    Cell::Text(value.into())
}

fn rich(
    text: impl Into<String>,
    tone: Option<Tone>,
    href: Option<String>,
    route: Option<String>,
) -> Cell {
    Cell::Rich(RichText {
        text: text.into(),
        route,
        href,
        tone,
    })
}

/// The URL a route answers on: https when a certificate is served.
fn url(state: &State, row: &RouteRow) -> String {
    let scheme = if state.certs.serves(&row.host) {
        "https"
    } else {
        "http"
    };
    let path = if row.path == "/" {
        "/"
    } else {
        row.path.as_str()
    };
    format!("{scheme}://{}{path}", row.host)
}

/// A route's certificate, as one cell.
fn certificate_cell(state: &State, host: &str, now: i64) -> Cell {
    if let Some(certified) = state.certs.lookup(host) {
        let (loaded, _) = state.certs.snapshot();
        let cert = loaded
            .iter()
            .find(|cert| Arc::ptr_eq(&cert.certified, &certified));
        return match cert {
            Some(cert) => {
                let days = (cert.not_after - now) / 86_400;
                let tone = if cert.expired(now) {
                    Tone::Danger
                } else if days < 14 {
                    Tone::Warn
                } else {
                    Tone::Ok
                };
                rich(
                    format!("{}, {days}d left", cert.source.label()),
                    Some(tone),
                    None,
                    None,
                )
            }
            None => rich("served", Some(Tone::Ok), None, None),
        };
    }
    if !state.config.certificates {
        return rich("HTTP only (certificates off)", None, None, None);
    }
    match state.cert_states().get(host) {
        Some(CertState::Skipped(_)) => rich("HTTP only", None, None, None),
        Some(CertState::AwaitingIssuer) => rich("waiting for issuer", Some(Tone::Info), None, None),
        Some(CertState::Requested {
            ready: false,
            reason,
            ..
        }) => rich(reason.clone(), Some(Tone::Danger), None, None),
        _ => rich("requested", Some(Tone::Info), None, None),
    }
}

fn endpoints_cell(state: &State, row: &RouteRow) -> Cell {
    if !row.served {
        return text("-");
    }
    let table = state.table();
    let Some(route) = table
        .find(row.host.trim_start_matches("*."), &row.path)
        .filter(|route| route.backend.target == row.target)
    else {
        // A wildcard row: find it by target.
        return match table
            .backends()
            .find(|backend| backend.target == row.target)
        {
            Some(backend) => health_cell(&backend.health()),
            None => text("-"),
        };
    };
    health_cell(&route.backend.health())
}

fn health_cell(health: &super::backend::BackendHealth) -> Cell {
    if !health.refreshed {
        return rich("…", None, None, None);
    }
    let tone = if health.routed == 0 {
        Tone::Danger
    } else if health.routed < health.total {
        Tone::Warn
    } else {
        Tone::Ok
    };
    rich(
        format!("{}/{}", health.routed, health.total),
        Some(tone),
        None,
        None,
    )
}

/// What is wrong with a row, if anything.
fn problem(row: &RouteRow, health: Option<&super::backend::BackendHealth>) -> Option<String> {
    if let Some(problem) = &row.problem {
        return Some(problem.clone());
    }
    let health = health?;
    if health.mesh_only > 0 && health.routed == 0 {
        return Some("the service is on the mesh: give it a second, non-mesh port to route".into());
    }
    if let Some(error) = &health.error {
        return Some(format!("reading endpoints: {error}"));
    }
    if health.refreshed && health.routed == 0 {
        return Some("no healthy endpoint".into());
    }
    None
}

fn route_rows(state: &State, rows: &[&RouteRow], with_target: bool) -> Vec<BTreeMap<String, Cell>> {
    let dns = state.dns();
    let now = chrono::Utc::now().timestamp();
    let table = state.table();
    rows.iter()
        .map(|row| {
            let health = table
                .backends()
                .find(|backend| backend.target == row.target)
                .map(|backend| backend.health());
            let mut cells = BTreeMap::new();
            let address = url(state, row);
            let link = (row.served && !row.host.starts_with("*.")).then(|| address.clone());
            cells.insert("route".into(), rich(address, None, link, None));
            if with_target {
                let (namespace, job) = crate::structs::namespace::split_job_id(&row.target.job_id);
                cells.insert(
                    "target".into(),
                    rich(
                        format!("{} / {}", row.target.job_id, row.target.service),
                        None,
                        None,
                        Some(format!("/jobs/{namespace}/{job}")),
                    ),
                );
            } else {
                cells.insert("target".into(), text(row.target.service.clone()));
            }
            cells.insert("endpoints".into(), endpoints_cell(state, row));
            cells.insert("dns".into(), dns_cell(&dns, &row.host));
            cells.insert(
                "certificate".into(),
                if row.served {
                    certificate_cell(state, &row.host, now)
                } else {
                    text("-")
                },
            );
            cells.insert(
                "problem".into(),
                match problem(row, health.as_ref()) {
                    Some(problem) => rich(problem, Some(Tone::Danger), None, None),
                    None => text(""),
                },
            );
            cells
        })
        .collect()
}

fn route_columns(target_label: &str) -> Vec<Column> {
    vec![
        Column {
            key: "route".into(),
            label: "Route".into(),
            kind: ColumnType::Text,
        },
        Column {
            key: "target".into(),
            label: target_label.into(),
            kind: ColumnType::Text,
        },
        Column {
            key: "endpoints".into(),
            label: "Endpoints".into(),
            kind: ColumnType::Text,
        },
        Column {
            key: "dns".into(),
            label: "DNS".into(),
            kind: ColumnType::Text,
        },
        Column {
            key: "certificate".into(),
            label: "Certificate".into(),
            kind: ColumnType::Text,
        },
        Column {
            key: "problem".into(),
            label: "Problem".into(),
            kind: ColumnType::Text,
        },
    ]
}

/// Where to point DNS: one `A` record per host, at any of these. A node
/// with no public address shows its cluster one, with how to give it one.
fn point_dns_here(edge: &[EdgeAddress]) -> Block {
    if edge.is_empty() {
        return Block::Notice(NoticeBlock {
            tone: Tone::Warn,
            title: Some("Point DNS here".into()),
            text: "No replica of the ingress is running and healthy yet, so there is no address to point DNS at. `legion app status ingress` says why.".into(),
        });
    }
    let targets: Vec<String> = edge
        .iter()
        .map(|replica| format!("`{}`", replica.target()))
        .collect();
    let mut text = format!(
        "Create an `A` (or `AAAA`) record for each host below pointing at {} {}.",
        if edge.len() == 1 {
            "the ingress node,"
        } else {
            "the ingress nodes:"
        },
        targets.join(", ")
    );
    let private: Vec<&str> = edge
        .iter()
        .filter(|replica| replica.public_address.is_none())
        .map(|replica| replica.node_id.as_str())
        .collect();
    if !private.is_empty() {
        text.push_str(&format!(
            " Node {} reports only its cluster address; if it is behind NAT, start its worker with `LEGION_WORKER_PUBLIC_ADDRESS=<public IP>` and its public address shows here.",
            private.join(", ")
        ));
    }
    Block::KeyValue(KeyValueBlock {
        title: Some("Point DNS here".into()),
        items: std::iter::once(KeyValueItem {
            key: "records".into(),
            value: text,
            tone: None,
            route: None,
        })
        .chain(edge.iter().map(|replica| KeyValueItem {
            key: replica.node_id.clone(),
            value: match &replica.public_address {
                Some(public) => format!("{public} (public; cluster {})", replica.address),
                None => format!("{} (cluster)", replica.address),
            },
            tone: None,
            route: Some(format!("/nodes/{}", replica.node_id)),
        }))
        .collect(),
    })
}

/// A route's DNS, as the ingress last resolved its host.
fn dns_cell(dns: &BTreeMap<String, DnsCheck>, host: &str) -> Cell {
    if host.starts_with("*.") {
        return text("-");
    }
    match dns.get(host) {
        Some(DnsCheck::Here) => rich("points here", Some(Tone::Ok), None, None),
        Some(DnsCheck::Elsewhere(ips)) => rich(
            format!(
                "points to {}",
                ips.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Some(Tone::Warn),
            None,
            None,
        ),
        Some(DnsCheck::Missing) => rich("no record", Some(Tone::Warn), None, None),
        None => rich("…", None, None, None),
    }
}

/// The Ingress page.
pub fn routes_view(state: &State, viewer: &Viewer) -> AppView {
    let table = state.table();
    let visible: Vec<&RouteRow> = table
        .rows
        .iter()
        .filter(|row| viewer.sees_namespace(&row.target.namespace))
        .collect();
    let hosts: std::collections::BTreeSet<&str> =
        visible.iter().map(|row| row.host.as_str()).collect();
    let (loaded, broken) = state.certs.snapshot();
    let now = chrono::Utc::now().timestamp();
    let covers = |cert: &LoadedCert| {
        cert.names.iter().any(|name| {
            hosts.iter().any(|host| {
                host == name
                    || name.strip_prefix("*.").is_some_and(|parent| {
                        host.split_once('.').is_some_and(|(_, rest)| rest == parent)
                    })
            })
        })
    };
    let certificates: Vec<&LoadedCert> = loaded.iter().filter(|cert| covers(cert)).collect();
    let problems = visible
        .iter()
        .filter(|row| !row.served || row.problem.is_some())
        .count();

    let mut blocks = Vec::new();
    let watch = state.watch_status();
    if !watch.connected {
        blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Warn,
            title: Some("Not following routes".into()),
            text: format!(
                "The ingress cannot watch its routes ({}); it keeps serving the last ones it had.",
                watch.error.unwrap_or_else(|| "connecting".into())
            ),
        }));
    }
    if state.config.certificates {
        if let Some(issuer) = state.issuer() {
            let problem = if issuer.kind.is_none() {
                Some(format!(
                    "There is no certs/Issuer named {}, so no certificate is issued. {}",
                    issuer.name,
                    if issuer.name == super::certificates::DEFAULT_ISSUER {
                        "The ingress creates it; check that the certs app is enabled (`legion app status certs`)."
                    } else {
                        "Create it, or set the ingress's `issuer` to one that exists."
                    }
                ))
            } else if !issuer.admits_ingress {
                Some(format!(
                    "The certs/Issuer {} does not serve the legion-system namespace, where the ingress keeps its Certificates: add it to the Issuer's `namespaces`.",
                    issuer.name
                ))
            } else if !issuer.ready {
                Some(format!(
                    "The certs/Issuer {} is not ready ({}): {}",
                    issuer.name, issuer.reason, issuer.message
                ))
            } else {
                None
            };
            if let Some(text) = problem {
                blocks.push(Block::Notice(NoticeBlock {
                    tone: Tone::Danger,
                    title: Some("Certificates".into()),
                    text,
                }));
            }
        }
    }
    blocks.push(Block::Stats(StatsBlock {
        title: None,
        items: vec![
            Stat {
                label: "Routes".into(),
                value: visible.iter().filter(|row| row.served).count().to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Hosts".into(),
                value: hosts.len().to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Certificates".into(),
                value: certificates.len().to_string(),
                tone: None,
                hint: Some(if state.config.certificates {
                    format!("issued by certs/Issuer {}", state.config.issuer)
                } else {
                    "certificates off".into()
                }),
            },
            Stat {
                label: "Problems".into(),
                value: problems.to_string(),
                tone: Some(if problems == 0 { Tone::Ok } else { Tone::Warn }),
                hint: None,
            },
        ],
    }));
    blocks.push(point_dns_here(&state.edge()));
    blocks.push(Block::Table(TableBlock {
        title: Some("Routes".into()),
        columns: route_columns("Service"),
        rows: route_rows(state, &visible, true),
        empty: Some(
            "No routes yet. Add `ingress = { host = \"shop.example.com\" }` to a service block and run the job."
                .into(),
        ),
    }));
    let mut cert_rows: Vec<BTreeMap<String, Cell>> = certificates
        .iter()
        .map(|cert| {
            let mut cells = BTreeMap::new();
            cells.insert("names".into(), text(cert.names.join(", ")));
            cells.insert("source".into(), text(cert.source.label()));
            cells.insert("issuer".into(), text(cert.issuer.clone()));
            cells.insert("expires".into(), Cell::Number(cert.not_after as f64));
            cells.insert(
                "state".into(),
                if cert.expired(now) {
                    rich("expired", Some(Tone::Danger), None, None)
                } else if cert.renewal_due(now) {
                    rich("renewing", Some(Tone::Warn), None, None)
                } else {
                    rich("ok", Some(Tone::Ok), None, None)
                },
            );
            cells
        })
        .collect();
    // Bundles that do not load, for whoever can see every namespace.
    if viewer.sees_namespace("default") {
        for (key, error) in broken {
            let mut cells = BTreeMap::new();
            cells.insert("names".into(), text(key));
            cells.insert("source".into(), text("operator"));
            cells.insert("state".into(), rich(error, Some(Tone::Danger), None, None));
            cert_rows.push(cells);
        }
    }
    // The cert manager's word on each host it has not issued for yet, and
    // why the ingress asks for nothing for a host.
    for (host, state) in state.cert_states() {
        if !hosts.contains(host.as_str()) {
            continue;
        }
        let (label, tone) = match state {
            CertState::Requested {
                ready: false,
                reason,
                message,
            } => (format!("{reason}: {message}"), Some(Tone::Danger)),
            CertState::Requested {
                ready: true,
                reason,
                message,
            } if reason != "Issued" => (format!("{reason}: {message}"), Some(Tone::Warn)),
            CertState::Waiting => (
                "requested from the cert manager".to_string(),
                Some(Tone::Info),
            ),
            CertState::AwaitingIssuer => (
                "waiting for the Issuer to register its ACME account".to_string(),
                Some(Tone::Info),
            ),
            CertState::Skipped(why) => (format!("not requested: {why}"), None),
            CertState::Requested { .. } => continue,
        };
        let mut cells = BTreeMap::new();
        cells.insert("names".into(), text(host));
        cells.insert("source".into(), text("cert manager"));
        cells.insert("state".into(), rich(label, tone, None, None));
        cert_rows.push(cells);
    }
    blocks.push(Block::Table(TableBlock {
        title: Some("Certificates".into()),
        columns: vec![
            Column {
                key: "names".into(),
                label: "Names".into(),
                kind: ColumnType::Text,
            },
            Column {
                key: "source".into(),
                label: "Source".into(),
                kind: ColumnType::Text,
            },
            Column {
                key: "issuer".into(),
                label: "Issuer".into(),
                kind: ColumnType::Text,
            },
            Column {
                key: "expires".into(),
                label: "Expires".into(),
                kind: ColumnType::Time,
            },
            Column {
                key: "state".into(),
                label: "State".into(),
                kind: ColumnType::Text,
            },
        ],
        rows: cert_rows,
        empty: Some("No certificates yet.".into()),
    }));
    AppView::new(blocks)
}

/// The panel on a job's page: its routes, with their URLs. Hidden for a job
/// with none.
pub fn job_view(state: &State, job_id: &str) -> AppView {
    let table = state.table();
    let rows: Vec<&RouteRow> = table
        .rows
        .iter()
        .filter(|row| row.target.job_id == job_id)
        .collect();
    if rows.is_empty() {
        return AppView::hidden();
    }
    AppView::new(vec![Block::Table(TableBlock {
        title: None,
        columns: route_columns("Service"),
        rows: route_rows(state, &rows, false),
        empty: None,
    })])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::ingress::routes::{RouteSpec, RouteTable, Target};
    use crate::apps::ingress::IngressConfig;
    use crate::structs::app_ui::{validate_view_value, Slot};
    use serde_json::json;

    /// The view as the manager checks what an app sends.
    fn check(view: &AppView, slot: Slot) {
        validate_view_value(&serde_json::to_value(view).unwrap(), None, Some(slot))
            .expect("a valid legion-view/1 document");
    }

    fn state_with_routes() -> Arc<State> {
        let state = State::new(IngressConfig::from_value(&json!({})).unwrap());
        let target = |job: &str| Target {
            job_id: job.into(),
            namespace: crate::structs::namespace::job_namespace(job).into(),
            task: "web".into(),
            service: "web".into(),
        };
        let specs = vec![
            RouteSpec::from_value(&json!({ "host": "shop.example.com" }), target("shop")).unwrap(),
            RouteSpec::from_value(&json!({ "host": "a.internal" }), target("team-a/admin"))
                .unwrap(),
        ];
        let mut backends = std::collections::HashMap::new();
        state.set_table(RouteTable::compile(&specs, &mut backends));
        state
    }

    fn viewer(namespaces: &[&str]) -> Viewer {
        Viewer {
            principal: "user:alice".into(),
            accessor: None,
            permission: Some("job:read".into()),
            scope: crate::structs::app_ui::AssertionScope::Any,
            namespaces: namespaces.iter().map(|ns| ns.to_string()).collect(),
            delegation: None,
        }
    }

    fn table_rows(view: &AppView, title: &str) -> usize {
        view.blocks
            .iter()
            .find_map(|block| match block {
                Block::Table(table) if table.title.as_deref() == Some(title) => {
                    Some(table.rows.len())
                }
                _ => None,
            })
            .expect("the table is there")
    }

    #[test]
    fn the_page_lists_only_the_viewers_namespaces() {
        let state = state_with_routes();
        assert_eq!(
            table_rows(&routes_view(&state, &viewer(&["*"])), "Routes"),
            2
        );
        assert_eq!(
            table_rows(&routes_view(&state, &viewer(&["default"])), "Routes"),
            1
        );
        assert_eq!(table_rows(&routes_view(&state, &viewer(&[])), "Routes"), 0);
        check(&routes_view(&state, &viewer(&["*"])), Slot::Nav);
    }

    #[test]
    fn the_job_panel_shows_the_jobs_routes_or_hides() {
        let state = state_with_routes();
        let panel = job_view(&state, "team-a/admin");
        assert!(!panel.hidden);
        check(&panel, Slot::Job);
        assert!(job_view(&state, "nothing").hidden);
    }
}
