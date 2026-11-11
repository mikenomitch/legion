//! The cert manager's UI (docs/cert-manager.md, "In the UI"): a
//! Certificates page in the sidebar and a card on the cluster overview,
//! drawn from the engine's latest outcome for each object and filtered by
//! the namespaces where the viewer may read objects.

use super::engine::{BoardRow, Engine, CERTIFICATE_KIND, ISSUER_KIND};
use crate::apps::ui::Viewer;
use crate::prelude::*;
use crate::structs::app_ui::{
    AppView, Block, Cell, Column, ColumnType, NoticeBlock, RichText, Stat, StatsBlock, TableBlock,
    Tone,
};
use std::collections::BTreeMap;

/// A certificate that renews within this many seconds counts as "renewing
/// soon".
const SOON: i64 = 14 * 86400;

/// One certificate as the views show it.
#[derive(Debug, Clone)]
struct Row {
    id: String,
    names: String,
    issuer: String,
    state: String,
    tone: Tone,
    detail: String,
    not_after: Option<i64>,
    renewal: Option<i64>,
    failing: bool,
}

fn time_field(fields: &Value, key: &str) -> Option<i64> {
    fields[key]
        .as_str()
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.timestamp())
}

/// A state and its tone from an outcome.
fn state_of(row: Option<&BoardRow>) -> (String, Tone, String, bool) {
    let Some(row) = row else {
        return ("Pending".into(), Tone::Neutral, String::new(), false);
    };
    if !row.accepted {
        return (
            format!("Rejected: {}", row.reason),
            Tone::Danger,
            row.message.clone(),
            true,
        );
    }
    match (row.ready, row.reason.as_str()) {
        (Some(true), "RenewalFailing") => (
            "Ready (renewal failing)".into(),
            Tone::Warn,
            row.message.clone(),
            true,
        ),
        (Some(true), _) => ("Ready".into(), Tone::Ok, row.message.clone(), false),
        (Some(false), reason) => (
            format!("NotReady: {reason}"),
            Tone::Danger,
            row.message.clone(),
            true,
        ),
        (None, _) => ("Accepted".into(), Tone::Info, row.message.clone(), false),
    }
}

fn certificate_rows(engine: &Engine, viewer: &Viewer) -> Vec<Row> {
    let mut rows: Vec<Row> = engine
        .live(CERTIFICATE_KIND)
        .into_iter()
        .filter(|object| viewer.sees_namespace(&object.namespace))
        .map(|object| {
            let board = engine.board_row(object.uid);
            let spec: Value = serde_json::from_str(&object.effective_json).unwrap_or_default();
            let (state, tone, detail, failing) = state_of(board.as_ref());
            let fields = board
                .as_ref()
                .map(|row| row.fields.clone())
                .unwrap_or_default();
            let mut names: Vec<String> = spec["dnsNames"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(spec["ipAddresses"].as_array().into_iter().flatten())
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect();
            if names.len() > 3 {
                let more = names.len() - 3;
                names.truncate(3);
                names.push(format!("+{more}"));
            }
            Row {
                id: object.id.clone(),
                names: names.join(", "),
                issuer: spec["issuer"].as_str().unwrap_or("").to_string(),
                state,
                tone,
                detail,
                not_after: time_field(&fields, "notAfter"),
                renewal: time_field(&fields, "renewalTime"),
                failing,
            }
        })
        .collect();
    rows.sort_by(|a, b| (!a.failing, &a.id).cmp(&(!b.failing, &b.id)));
    rows
}

fn stats(rows: &[Row], now: i64) -> StatsBlock {
    let ready = rows.iter().filter(|row| row.tone == Tone::Ok).count();
    let failing = rows.iter().filter(|row| row.failing).count();
    let soon = rows
        .iter()
        .filter(|row| row.renewal.is_some_and(|at| at - now <= SOON))
        .count();
    let stat = |label: &str, value: usize, tone: Option<Tone>, hint: Option<&str>| Stat {
        label: label.into(),
        value: value.to_string(),
        tone,
        hint: hint.map(str::to_string),
    };
    StatsBlock {
        title: None,
        items: vec![
            stat("Certificates", rows.len(), None, None),
            stat("Ready", ready, (ready > 0).then_some(Tone::Ok), None),
            stat(
                "Renewing within 14 days",
                soon,
                None,
                Some("renewal is automatic"),
            ),
            stat(
                "Failing",
                failing,
                (failing > 0).then_some(Tone::Danger),
                None,
            ),
        ],
    }
}

fn column(key: &str, label: &str, kind: ColumnType) -> Column {
    Column {
        key: key.into(),
        label: label.into(),
        kind,
    }
}

fn rich(text: &str, tone: Option<Tone>) -> Cell {
    Cell::Rich(RichText {
        text: text.into(),
        route: None,
        href: None,
        tone,
    })
}

/// The Certificates page.
pub fn certificates_view(engine: &Engine, viewer: &Viewer, now: i64) -> AppView {
    let rows = certificate_rows(engine, viewer);
    let mut blocks = vec![Block::Stats(stats(&rows, now))];
    blocks.push(Block::Table(TableBlock {
        title: Some("Certificates".into()),
        columns: vec![
            column("certificate", "Certificate", ColumnType::Text),
            column("names", "Names", ColumnType::Text),
            column("issuer", "Issuer", ColumnType::Text),
            column("state", "State", ColumnType::Text),
            column("expires", "Expires", ColumnType::Time),
            column("renews", "Renews", ColumnType::Time),
            column("detail", "Detail", ColumnType::Text),
        ],
        rows: rows
            .iter()
            .map(|row| {
                let mut cells = BTreeMap::new();
                cells.insert("certificate".into(), Cell::Text(row.id.clone()));
                cells.insert("names".into(), Cell::Text(row.names.clone()));
                cells.insert("issuer".into(), Cell::Text(row.issuer.clone()));
                cells.insert("state".into(), rich(&row.state, Some(row.tone)));
                if let Some(at) = row.not_after {
                    cells.insert("expires".into(), Cell::Number(at as f64));
                }
                if let Some(at) = row.renewal {
                    cells.insert("renews".into(), Cell::Number(at as f64));
                }
                cells.insert("detail".into(), Cell::Text(row.detail.clone()));
                cells
            })
            .collect(),
        empty: Some(
            "No certificates. Apply a certs/Certificate (docs/cert-manager.md) to get one.".into(),
        ),
    }));
    // Issuers are cluster-scoped: shown to viewers who read objects in
    // every namespace.
    if viewer.sees_namespace("*") {
        let mut issuers: Vec<(String, Option<BoardRow>, Value)> = engine
            .live(ISSUER_KIND)
            .into_iter()
            .map(|object| {
                let spec: Value = serde_json::from_str(&object.effective_json).unwrap_or_default();
                (object.name.clone(), engine.board_row(object.uid), spec)
            })
            .collect();
        issuers.sort_by(|a, b| a.0.cmp(&b.0));
        blocks.push(Block::Table(TableBlock {
            title: Some("Issuers".into()),
            columns: vec![
                column("issuer", "Issuer", ColumnType::Text),
                column("type", "Type", ColumnType::Text),
                column("state", "State", ColumnType::Text),
                column("detail", "Detail", ColumnType::Text),
            ],
            rows: issuers
                .into_iter()
                .map(|(name, board, spec)| {
                    let kind = ["acme", "ca", "selfSigned"]
                        .into_iter()
                        .find(|kind| spec.get(*kind).is_some())
                        .unwrap_or("");
                    let (state, tone, detail, _) = state_of(board.as_ref());
                    let mut cells = BTreeMap::new();
                    cells.insert("issuer".into(), Cell::Text(name));
                    cells.insert("type".into(), Cell::Text(kind.into()));
                    cells.insert("state".into(), rich(&state, Some(tone)));
                    cells.insert("detail".into(), Cell::Text(detail));
                    cells
                })
                .collect(),
            empty: Some("No issuers. Apply a certs/Issuer first.".into()),
        }));
    }
    AppView::new(blocks)
}

/// The overview card.
pub fn summary_view(engine: &Engine, viewer: &Viewer, now: i64) -> AppView {
    let rows = certificate_rows(engine, viewer);
    if rows.is_empty() {
        return AppView::hidden();
    }
    let mut blocks = vec![Block::Stats(stats(&rows, now))];
    let failing: Vec<&Row> = rows.iter().filter(|row| row.failing).collect();
    if !failing.is_empty() {
        let names: Vec<&str> = failing.iter().take(5).map(|row| row.id.as_str()).collect();
        blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Warn,
            title: None,
            text: format!(
                "{} certificate(s) need attention: {}{}",
                failing.len(),
                names.join(", "),
                if failing.len() > 5 { ", ..." } else { "" }
            ),
        }));
    }
    AppView::new(blocks)
}

/// Register the views on the app.
pub fn register(context: &crate::apps::AppContext, engine: Arc<Engine>) {
    let page = engine.clone();
    context.view("certificates", move |request| {
        let engine = page.clone();
        async move {
            Ok(certificates_view(
                &engine,
                &request.viewer,
                chrono::Utc::now().timestamp(),
            ))
        }
    });
    context.view("summary", move |request| {
        let engine = engine.clone();
        async move {
            Ok(summary_view(
                &engine,
                &request.viewer,
                chrono::Utc::now().timestamp(),
            ))
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::certs::engine::{EngineOptions, MemoryPlane, APP};
    use crate::structs::app_ui::AssertionScope;
    use crate::structs::AppObject;

    fn viewer(namespaces: &[&str]) -> Viewer {
        Viewer {
            principal: "alice".into(),
            accessor: None,
            permission: Some("object:read".into()),
            scope: AssertionScope::Any,
            namespaces: namespaces.iter().map(|ns| ns.to_string()).collect(),
            delegation: None,
        }
    }

    fn object(kind: &str, uid: u64, namespace: &str, name: &str, spec: Value) -> AppObject {
        AppObject {
            app: APP.into(),
            kind: kind.into(),
            qualified_kind: format!("{APP}/{kind}"),
            namespace: namespace.into(),
            name: name.into(),
            id: if namespace.is_empty() {
                name.into()
            } else {
                format!("{namespace}/{name}")
            },
            uid,
            generation: 1,
            spec_version: 1,
            effective_json: spec.to_string(),
            state: "valid".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn the_page_lists_what_the_viewer_may_read_test() -> R<()> {
        let engine = Engine::new(Arc::new(MemoryPlane::default()), EngineOptions::default());
        let issuer = object(ISSUER_KIND, 1, "", "self", json!({ "selfSigned": {} }));
        let a = object(
            CERTIFICATE_KIND,
            2,
            "team-a",
            "web",
            json!({ "issuer": "self", "kv": "certs/a", "dnsNames": ["a.test"] }),
        );
        let b = object(
            CERTIFICATE_KIND,
            3,
            "team-b",
            "web",
            json!({ "issuer": "missing", "kv": "certs/b", "dnsNames": ["b.test"] }),
        );
        engine
            .objects()
            .write()
            .unwrap()
            .apply(crate::structs::ObjectEvent {
                snapshot: true,
                snapshot_done: true,
                upserts: vec![issuer.clone(), a.clone(), b.clone()],
                ..Default::default()
            });
        engine.reconcile_issuer(&issuer).await;
        engine.reconcile_certificate(&a).await;
        engine.reconcile_certificate(&b).await;

        let now = chrono::Utc::now().timestamp();
        let everyone = certificates_view(&engine, &viewer(&["*"]), now);
        crate::structs::app_ui::validate_view_value(&serde_json::to_value(&everyone)?, None, None)
            .map_err(|err| anyhow!("{err}"))?;
        let text = serde_json::to_string(&everyone)?;
        assert!(text.contains("team-a/web") && text.contains("team-b/web"));
        assert!(text.contains("IssuerNotFound"));
        assert!(text.contains("Issuers"));

        let team_a = certificates_view(&engine, &viewer(&["team-a"]), now);
        let text = serde_json::to_string(&team_a)?;
        assert!(text.contains("team-a/web"));
        assert!(!text.contains("team-b/web"));
        assert!(!text.contains("\"Issuers\""), "issuers are cluster-wide");

        let card = summary_view(&engine, &viewer(&["*"]), now);
        let text = serde_json::to_string(&card)?;
        assert!(text.contains("need attention: team-b/web"), "{text}");
        assert!(summary_view(&engine, &viewer(&["nobody"]), now).hidden);
        Ok(())
    }
}
