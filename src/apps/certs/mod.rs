//! The bundled cert manager, `certs` (docs/cert-manager.md;
//! internal_docs/plans/cert-manager-plan.md).
//!
//! Two object kinds: `certs/Issuer` (cluster-scoped: a self-signed issuer,
//! a private CA kept in KV, or an ACME CA such as Let's Encrypt) and
//! `certs/Certificate` (namespaced: names, an Issuer, and the KV key the
//! certificate goes to, as the PEM bundle `ports[].tls.cert = { kv }`
//! serves). The app issues each Certificate, renews it before it expires,
//! and reports on both kinds; everything it reads or writes for an object
//! it does acting for that object, so a Certificate can only write where
//! its writer could.
//!
//! - [`spec`]: the kinds' specs, as the app parses them.
//! - [`issue`]: keys, private CAs, signing, the bundle format.
//! - [`acme`]: the RFC 8555 client; [`dns01`]: DNS-01 providers;
//!   [`http01`]: the stateless HTTP-01 responder (`legion cert http01`).
//! - [`engine`]: what reconciling an Issuer or a Certificate does.
//! - [`views`]: the Certificates page and the overview card.
//! - [`test_support`]: a fake ACME CA and an HTTP stand-in, for tests.

pub mod acme;
pub mod dns01;
pub mod engine;
pub mod http01;
pub mod issue;
pub mod spec;
pub mod test_support;
pub mod views;

use crate::apps::objects::{run_object_watch, Reconciler, ShardSelection};
use crate::apps::{AppContext, AppFuture};
use crate::cmd::helpers::ManagerConnection;
use crate::prelude::*;
use engine::{Engine, EngineOptions, LegionPlane, APP, CERTIFICATE_KIND, ISSUER_KIND};
use std::time::Duration;

/// How often every object is looked at again when nothing changed: an
/// Issuer re-reads its CA from KV, a Certificate re-reads its key.
pub const RESYNC: Duration = Duration::from_secs(300);
/// How many objects of each kind are reconciled at once. An ACME issuance
/// can take a minute (the DNS-01 propagation wait), so a few run side by
/// side.
pub const CONCURRENCY: usize = 4;

/// The bundled app's body.
pub fn run(context: AppContext) -> AppFuture {
    Box::pin(body(context))
}

async fn body(mut context: AppContext) -> R<()> {
    let connection = match context.connection() {
        Ok(connection) => connection,
        Err(err) => {
            warn!("The cert manager cannot reach Legion: {err:#}");
            context.set_health(true, format!("idle: cannot reach Legion: {err:#}"));
            context.stopped().await;
            return Ok(());
        }
    };
    let engine = Engine::new(
        Arc::new(LegionPlane::new(connection.clone())),
        EngineOptions::default(),
    );
    views::register(&context, engine.clone());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let running = start(engine, connection, context.name().to_string(), stop_rx);
    context.set_health(true, "running");
    context.stopped().await;
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    Ok(())
}

/// Run the engine over `connection` as app `app` until `stop` turns true:
/// watch every object of the app's kinds, and once the first snapshot is
/// in (so a Certificate never reads its Issuer as missing because the
/// watch had not caught up), reconcile Issuers and Certificates.
pub fn start(
    engine: Arc<Engine>,
    connection: ManagerConnection,
    app: String,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = connection.client();
        let (selection_tx, selection_rx) = tokio::sync::watch::channel(ShardSelection::All);
        let watch = tokio::spawn(run_object_watch(
            client.clone(),
            app.clone(),
            Vec::new(),
            selection_rx,
            engine.objects(),
        ));
        loop {
            if engine.objects().read().is_ok_and(|cache| cache.synced()) {
                break;
            }
            if *stop.borrow() {
                watch.abort();
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = stop.changed() => {}
            }
        }
        let issuers = {
            let engine = engine.clone();
            Reconciler::new(&app, ISSUER_KIND)
                .connection(connection.clone())
                .resync(RESYNC)
                .concurrency(CONCURRENCY)
                .run(client.clone(), stop.clone(), move |request| {
                    let engine = engine.clone();
                    async move { Ok(engine.reconcile_issuer(&request.object).await) }
                })
        };
        let certificates = {
            let engine = engine.clone();
            Reconciler::new(&app, CERTIFICATE_KIND)
                .connection(connection.clone())
                .resync(RESYNC)
                .concurrency(CONCURRENCY)
                .run(client, stop.clone(), move |request| {
                    let engine = engine.clone();
                    async move { Ok(engine.reconcile_certificate(&request.object).await) }
                })
        };
        // Forget what the engine remembers about objects that are gone.
        let pruning = {
            let engine = engine.clone();
            let mut stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(RESYNC) => engine.prune().await,
                        _ = stop.changed() => if *stop.borrow() { return },
                    }
                }
            })
        };
        let _ = tokio::join!(issuers, certificates);
        pruning.abort();
        watch.abort();
        drop(selection_tx);
        info!(app = %APP, "The cert manager stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::bundled_manifest;
    use crate::structs::app_object::KindScope;

    /// The manifest's grant is exactly what the app calls: its own KV for
    /// ACME account keys, and every other key (another app's private prefix
    /// included) only for one of its objects.
    #[test]
    fn the_certs_manifest_asks_for_what_it_calls_test() {
        let manifest = bundled_manifest("certs").expect("bundled");
        let rules = serde_json::to_value(&manifest.permissions).expect("rules");
        assert_eq!(
            rules,
            json!([
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["legion/apps/certs/*"] },
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["*", "legion/apps/*"], "via": "objects" },
            ])
        );
        let decls = crate::structs::app_object::manifest_kinds(manifest).expect("kinds parse");
        let kinds: Vec<(&str, KindScope)> = decls
            .iter()
            .map(|kind| (kind.name.as_str(), kind.scope))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (ISSUER_KIND, KindScope::Cluster),
                (CERTIFICATE_KIND, KindScope::Namespaced)
            ]
        );
        assert_eq!(manifest.placement, crate::structs::app::Placement::Managers);
        assert_eq!(manifest.replicas, crate::structs::app::Replicas::Count(1));
        assert!(crate::apps::is_bundled("certs"));
        let ui = manifest.ui_spec().expect("the ui block parses");
        assert_eq!(
            ui.slots(),
            vec![
                crate::structs::app_ui::Slot::Nav,
                crate::structs::app_ui::Slot::Overview
            ]
        );
    }

    /// Every example in the manifest parses into the app's own types and
    /// passes its checks: the schema the manager enforces and the parser
    /// the app runs agree.
    #[test]
    fn the_manifest_examples_parse_as_the_app_reads_them_test() {
        let manifest = bundled_manifest("certs").expect("bundled");
        let decls = crate::structs::app_object::manifest_kinds(manifest).expect("kinds parse");
        for kind in &decls {
            assert!(!kind.examples.is_empty(), "{} has examples", kind.name);
            for example in &kind.examples {
                match kind.name.as_str() {
                    ISSUER_KIND => {
                        let spec: spec::IssuerSpec = serde_json::from_value(example.clone())
                            .unwrap_or_else(|err| panic!("{example}: {err}"));
                        spec.issuer_type()
                            .unwrap_or_else(|err| panic!("{example}: {err}"));
                    }
                    CERTIFICATE_KIND => {
                        let spec: spec::CertificateSpec = serde_json::from_value(example.clone())
                            .unwrap_or_else(|err| panic!("{example}: {err}"));
                        spec.desired()
                            .unwrap_or_else(|err| panic!("{example}: {err}"));
                    }
                    other => panic!("unexpected kind {other}"),
                }
            }
        }
    }
}
