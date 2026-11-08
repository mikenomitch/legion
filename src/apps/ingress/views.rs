//! The ingress's UI (docs/ingress.md, "Seeing it"): the Ingress page in the
//! sidebar (`routes`) and the panel on a job's page (`job`). Both need
//! `job:read`; the page lists only the namespaces the viewer may read, and
//! only the certificates for hosts it lists.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::certs::LoadedCert;
use super::routes::{publicly_issuable, RouteRow};
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
    if host.starts_with("*.") {
        return rich("needs an operator bundle", Some(Tone::Warn), None, None);
    }
    if !publicly_issuable(host) {
        return rich("HTTP only", None, None, None);
    }
    if !state.config.acme.enabled {
        return rich("HTTP only (acme off)", None, None, None);
    }
    let failed = state
        .issuer_status()
        .and_then(|status| status.hosts.get(host).cloned())
        .and_then(|host| host.error);
    match failed {
        Some(_) => rich("ordering failed", Some(Tone::Danger), None, None),
        None => rich("ordering", Some(Tone::Info), None, None),
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
    if let Some(error) = state.issuer_status().and_then(|status| status.error) {
        blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Danger,
            title: Some("Certificates".into()),
            text: format!("The issuer cannot reach its ACME CA: {error}"),
        }));
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
                hint: Some(if state.config.acme.enabled {
                    "ordered automatically".into()
                } else {
                    "acme off".into()
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
    let replicas = state.replicas();
    blocks.push(Block::KeyValue(KeyValueBlock {
        title: Some("Listening on".into()),
        items: if replicas.is_empty() {
            vec![KeyValueItem {
                key: "replicas".into(),
                value: "none reported yet".into(),
                tone: Some(Tone::Warn),
                route: None,
            }]
        } else {
            replicas
                .iter()
                .map(|(id, record)| KeyValueItem {
                    key: format!("allocation {id}"),
                    value: format!(
                        "{} (http {}, https {})",
                        record.address, record.http_port, record.https_port
                    ),
                    tone: None,
                    route: None,
                })
                .collect()
        },
    }));
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
    let ordering_errors: Vec<(String, String)> = state
        .issuer_status()
        .map(|status| {
            status
                .hosts
                .into_iter()
                .filter(|(host, _)| hosts.contains(host.as_str()))
                .filter_map(|(host, status)| status.error.map(|error| (host, error)))
                .collect()
        })
        .unwrap_or_default();
    for (host, error) in ordering_errors {
        let mut cells = BTreeMap::new();
        cells.insert("names".into(), text(host));
        cells.insert("source".into(), text("acme"));
        cells.insert(
            "state".into(),
            rich(
                format!("ordering failed: {error}"),
                Some(Tone::Danger),
                None,
                None,
            ),
        );
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
