//! `legion cert`: the bundled cert manager's commands
//! (docs/cert-manager.md). Certificates and Issuers are app objects, so
//! `legion object` reads, applies, and deletes them; these are the two
//! things it cannot do.
//!
//! ```console
//! $ legion cert renew shop -n default --wait
//! $ legion cert http01 --issuer letsencrypt --listen 0.0.0.0:80
//! ```

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::apps::certs::engine::{APP, CERTIFICATE_KIND, ISSUER_KIND, RENEW_LABEL};
use crate::apps::certs::http01;
use crate::cmd::helpers::{active_namespace, connect_manager_client, json_flag, namespace_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{AppObject, ApplyObjectRequest, GetObjectRequest};

/// How long `renew --wait` waits by default.
const WAIT_DEFAULT: Duration = Duration::from_secs(120);
/// How often the responder re-reads its Issuer's thumbprint once known.
const THUMBPRINT_REFRESH: Duration = Duration::from_secs(60);

/// `legion cert`.
pub fn config() -> Command {
    Command::new("cert")
        .about("The cert manager's certificates: renew one now, or answer ACME HTTP-01 challenges (docs/cert-manager.md)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("renew")
                .about("Re-issue a certificate now, whatever its renewal time")
                .long_about(
                    "Sets the certs/renew label on the certs/Certificate to the current time, \
                     which the certs app honors once by issuing a new certificate (and a new \
                     key). An ordinary apply: it needs what writing the Certificate needs. A \
                     renewal request is not held back by a failing certificate's backoff.",
                )
                .arg(
                    Arg::new("name")
                        .required(true)
                        .value_name("NAME")
                        .help("The Certificate's name, or namespace/name"),
                )
                .arg(namespace_flag())
                .arg(
                    Arg::new("wait")
                        .long("wait")
                        .action(ArgAction::SetTrue)
                        .help("Wait until the certs app has re-issued it, or says why it could not"),
                )
                .arg(
                    Arg::new("timeout")
                        .long("timeout")
                        .value_name("DURATION")
                        .requires("wait")
                        .help("How long --wait waits (default 2m)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("http01")
                .about("Answer ACME HTTP-01 challenges for an Issuer, and redirect everything else to HTTPS (a long-running responder: run it as a job on the nodes your names point at)")
                .long_about(
                    "Answers GET /.well-known/acme-challenge/<token> with <token>.<thumbprint>, \
                     the Issuer's ACME account thumbprint, which is public: the responder needs \
                     no secret and no list of pending challenges. Every other request gets a \
                     308 redirect to https://<host><path> (--no-redirect: 404). With --issuer \
                     it reads the thumbprint from the Issuer's status (object:read on \
                     certs/Issuer) and keeps it current.",
                )
                .arg(
                    Arg::new("issuer")
                        .long("issuer")
                        .value_name("ISSUER")
                        .required_unless_present("thumbprint")
                        .conflicts_with("thumbprint")
                        .help("The certs/Issuer whose challenges to answer"),
                )
                .arg(
                    Arg::new("thumbprint")
                        .long("thumbprint")
                        .value_name("THUMBPRINT")
                        .help("Answer with this thumbprint instead of reading an Issuer's"),
                )
                .arg(
                    Arg::new("listen")
                        .long("listen")
                        .value_name("ADDR")
                        .help("Where to listen (default 0.0.0.0:$LEGION_PORT_HTTP, else 0.0.0.0:80)"),
                )
                .arg(
                    Arg::new("no-redirect")
                        .long("no-redirect")
                        .action(ArgAction::SetTrue)
                        .help("Answer 404 instead of redirecting other requests to HTTPS"),
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("renew", sub)) => renew(sub).await,
        Some(("http01", sub)) => serve_http01(sub).await,
        _ => unreachable!(),
    }
}

fn certificate_kind() -> String {
    format!("{APP}/{CERTIFICATE_KIND}")
}

fn status_fields(object: &AppObject) -> Value {
    object
        .status
        .as_ref()
        .and_then(|status| serde_json::from_str(&status.fields_json).ok())
        .unwrap_or(Value::Null)
}

/// The Certificate's `Ready` condition: (status, reason, message).
fn ready(object: &AppObject) -> Option<(String, String, String)> {
    object.status.as_ref().and_then(|status| {
        status
            .conditions
            .iter()
            .find(|condition| condition.r#type == "Ready")
            .map(|c| (c.status.clone(), c.reason.clone(), c.message.clone()))
    })
}

/// `legion cert renew`.
async fn renew(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let timeout = match matches.get_one::<String>("timeout") {
        None => WAIT_DEFAULT,
        Some(text) => humantime::parse_duration(text.trim()).map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("--timeout {text:?}: {err}"),
                    Some("write it like 2m"),
                ),
            )
        })?,
    };
    let mut client = connect_manager_client().await?;
    let read = output::resolve(
        format,
        "failed to read the certificate",
        client
            .get_object(GetObjectRequest {
                kind: certificate_kind(),
                name: name.clone(),
                namespace: active_namespace(matches).unwrap_or_default(),
            })
            .await,
    )?
    .into_inner();
    let Some(object) = read.object else {
        return Err(output::fail_local(
            format,
            CliError::local("NotFound", format!("no certs/Certificate {name}"), None),
        ));
    };
    // Milliseconds, so two requests in one second are two renewals.
    let requested = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    let mut labels: BTreeMap<String, String> = object.labels.clone().into_iter().collect();
    labels.insert(RENEW_LABEL.to_string(), requested.clone());
    let spec: Value = serde_json::from_str(&object.spec_json).unwrap_or(Value::Null);
    let document = json!({
        "kind": object.qualified_kind,
        "name": object.name,
        "namespace": object.namespace,
        "labels": labels,
        "spec": spec,
    });
    let applied = output::resolve(
        format,
        "failed to request the renewal",
        client
            .apply_object(ApplyObjectRequest {
                object_json: document.to_string(),
                namespace: object.namespace.clone(),
                dry_run: false,
                // Over the version just read: never undo someone's change.
                expected_version: Some(object.spec_version),
                idempotency_key: None,
                force: false,
            })
            .await,
    )?
    .into_inner();
    if !format.is_json() {
        println!(
            "{} {}: renewal requested ({RENEW_LABEL}={requested}); the certs app re-issues it within seconds",
            object.qualified_kind, object.id
        );
    }
    let mut renewed = None;
    if matches.get_flag("wait") {
        match wait_for_renewal(&mut client, &object, &requested, timeout).await {
            Ok(done) => {
                if !format.is_json() {
                    let fields = status_fields(&done);
                    println!(
                        "{} {}: renewed (serial {}, valid until {})",
                        done.qualified_kind,
                        done.id,
                        fields["serial"].as_str().unwrap_or("?"),
                        fields["notAfter"].as_str().unwrap_or("?")
                    );
                }
                renewed = Some(done);
            }
            Err(err) => {
                return Err(output::fail_local(
                    format,
                    CliError::local(
                        "FailedPrecondition",
                        err,
                        Some("legion object get certificate <name> shows the status"),
                    ),
                ))
            }
        }
    }
    if format.is_json() {
        output::print_success(&json!({
            "requested": requested,
            "object": applied.object,
            "renewed": renewed,
        }));
    }
    Ok(())
}

/// Poll until the Certificate's status records this renew request, or a
/// failure after it.
async fn wait_for_renewal(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    object: &AppObject,
    requested: &str,
    timeout: Duration,
) -> Result<AppObject, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let current = client
            .get_object(GetObjectRequest {
                kind: object.qualified_kind.clone(),
                name: object.name.clone(),
                namespace: object.namespace.clone(),
            })
            .await
            .map_err(|status| status.message().to_string())?
            .into_inner()
            .object
            .ok_or_else(|| format!("{} {} is gone", object.qualified_kind, object.id))?;
        let fields = status_fields(&current);
        if fields["renewRequest"].as_str() == Some(requested) {
            return Ok(current);
        }
        let failed_after = fields["lastFailureTime"]
            .as_str()
            .is_some_and(|at| at >= &requested[..19]);
        if let Some((status, reason, message)) = ready(&current) {
            if failed_after && (status == "false" || reason == "RenewalFailing") {
                return Err(format!(
                    "{} {}: the renewal failed: {message}",
                    current.qualified_kind, current.id
                ));
            }
        }
        if current.summary.starts_with("Rejected") || current.summary == "Stale" {
            return Err(format!(
                "{} {}: {} {}",
                current.qualified_kind, current.id, current.summary, current.summary_detail
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} {} was not renewed within {}s (`legion app status certs` says whether the app runs)",
                object.qualified_kind,
                object.id,
                timeout.as_secs()
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The responder's listen address.
fn listen_addr(matches: &ArgMatches) -> R<std::net::SocketAddr> {
    let raw = matches
        .get_one::<String>("listen")
        .cloned()
        .or_else(|| {
            std::env::var("LEGION_PORT_HTTP")
                .ok()
                .filter(|port| !port.trim().is_empty())
                .map(|port| format!("0.0.0.0:{}", port.trim()))
        })
        .unwrap_or_else(|| "0.0.0.0:80".to_string());
    raw.trim()
        .parse()
        .with_context(|| format!("invalid listen address {raw:?}"))
}

/// `legion cert http01`: serve until SIGTERM or SIGINT.
async fn serve_http01(matches: &ArgMatches) -> R<()> {
    let addr = listen_addr(matches)?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to listen on {addr}"))?;
    let thumbprint: http01::SharedThumbprint = Arc::new(RwLock::new(
        matches.get_one::<String>("thumbprint").cloned(),
    ));
    let issuer = matches.get_one::<String>("issuer").cloned();
    if let Some(issuer) = &issuer {
        tokio::spawn(follow_thumbprint(issuer.clone(), thumbprint.clone()));
    }
    info!(
        listen = %listener.local_addr()?,
        issuer = issuer.as_deref().unwrap_or("-"),
        "Answering ACME HTTP-01 challenges"
    );
    let redirect = !matches.get_flag("no-redirect");
    tokio::select! {
        served = http01::serve(listener, thumbprint, redirect) => served,
        _ = tokio::signal::ctrl_c() => Ok(()),
        _ = terminated() => Ok(()),
    }
}

async fn terminated() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            term.recv().await;
            return;
        }
    }
    std::future::pending::<()>().await
}

/// Keep `thumbprint` at the Issuer's, read from its status: every 5 seconds
/// until it is known, then every minute. A failed read keeps the last one.
async fn follow_thumbprint(issuer: String, thumbprint: http01::SharedThumbprint) {
    let kind = format!("{APP}/{ISSUER_KIND}");
    loop {
        let read = async {
            let mut client = connect_manager_client().await?;
            let object = client
                .get_object(GetObjectRequest {
                    kind: kind.clone(),
                    name: issuer.clone(),
                    namespace: String::new(),
                })
                .await
                .map_err(|status| anyhow!("{}", status.message()))?
                .into_inner()
                .object
                .ok_or_else(|| anyhow!("no certs/Issuer {issuer}"))?;
            status_fields(&object)["thumbprint"]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    anyhow!(
                        "certs/Issuer {issuer} has no ACME account thumbprint yet (is it an acme \
                         Issuer, and Ready?)"
                    )
                })
        };
        match read.await {
            Ok(value) => {
                let changed = thumbprint
                    .read()
                    .map(|current| current.as_deref() != Some(value.as_str()))
                    .unwrap_or(true);
                if changed {
                    info!(%issuer, "Answering with the issuer's account thumbprint");
                    if let Ok(mut current) = thumbprint.write() {
                        *current = Some(value);
                    }
                }
            }
            Err(err) => {
                warn!(%issuer, error = %format!("{err:#}"), "Could not read the issuer's thumbprint")
            }
        }
        let known = thumbprint.read().map(|t| t.is_some()).unwrap_or(false);
        tokio::time::sleep(if known {
            THUMBPRINT_REFRESH
        } else {
            Duration::from_secs(5)
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_responder_listens_where_it_is_told_test() {
        let matches = config()
            .try_get_matches_from([
                "cert",
                "http01",
                "--thumbprint",
                "T",
                "--listen",
                "127.0.0.1:8080",
            ])
            .unwrap();
        let (_, sub) = matches.subcommand().unwrap();
        assert_eq!(listen_addr(sub).unwrap().port(), 8080);
        // One of --issuer and --thumbprint.
        assert!(config().try_get_matches_from(["cert", "http01"]).is_err());
        assert!(config()
            .try_get_matches_from(["cert", "http01", "--issuer", "a", "--thumbprint", "b"])
            .is_err());
    }
}
