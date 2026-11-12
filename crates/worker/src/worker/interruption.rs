//! The node's spot interruption watcher (docs/spot-interruption.md).
//!
//! A cloud that reclaims spot capacity says so a little ahead of time: EC2
//! publishes an `instance-action` in the instance metadata service two
//! minutes before it stops or terminates the instance. A worker whose type
//! sets `spotInterruption = { provider = "aws" }` polls for that notice and
//! reports it on every heartbeat (`HeartbeatRequest.interruption`) while it
//! stands. The managers record it on the node, stop placing gangs there, and
//! move the gang members it runs before the node goes.
//!
//! The watcher only reads: it never acts on the node itself. A metadata
//! service that cannot be reached leaves the last answer in place (no notice
//! at all, until one is read), so an IMDS hiccup never invents or retracts
//! an interruption.

use crate::prelude::*;
use crate::structs::{NodeInterruptionNotice, WorkerSpotInterruptionConfig};
use std::sync::Mutex;

/// The notice this node reports, if any.
static NOTICE: Mutex<Option<NodeInterruptionNotice>> = Mutex::new(None);

/// The notice to put on the next heartbeat: the last one the watcher read,
/// or `None` when there is none (or no watcher runs).
pub fn current_notice() -> Option<NodeInterruptionNotice> {
    NOTICE.lock().unwrap_or_else(|p| p.into_inner()).clone()
}

fn set_notice(notice: Option<NodeInterruptionNotice>) {
    let mut current = NOTICE.lock().unwrap_or_else(|p| p.into_inner());
    if *current != notice {
        match &notice {
            Some(notice) => warn!(
                deadline_unix = notice.deadline_unix,
                detail = %notice.detail,
                "Spot interruption notice: this node is about to be reclaimed"
            ),
            None => info!("Spot interruption notice withdrawn"),
        }
    }
    *current = notice;
}

/// Parse the body of EC2's `spot/instance-action`:
/// `{"action": "terminate", "time": "2026-11-12T08:22:00Z"}`.
pub fn parse_instance_action(body: &str) -> R<NodeInterruptionNotice> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("spot instance-action is not JSON")?;
    let action = value
        .get("action")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("terminate")
        .to_string();
    let deadline_unix = match value.get("time").and_then(serde_json::Value::as_str) {
        Some(time) => {
            time::OffsetDateTime::parse(time, &time::format_description::well_known::Rfc3339)
                .with_context(|| format!("spot instance-action time {time:?} is not RFC 3339"))?
                .unix_timestamp()
        }
        None => 0,
    };
    Ok(NodeInterruptionNotice {
        deadline_unix,
        source: "aws-imds".to_string(),
        detail: format!("spot instance-action: {action}"),
    })
}

/// One look at the metadata service: `Ok(Some)` for a notice, `Ok(None)`
/// for none (404), `Err` when the service could not be asked.
async fn poll_aws(client: &reqwest::Client, endpoint: &str) -> R<Option<NodeInterruptionNotice>> {
    // IMDSv2 first; an instance that only allows v1 answers without a token.
    let token = client
        .put(format!("{endpoint}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "300")
        .send()
        .await
        .ok()
        .filter(|response| response.status().is_success());
    let token = match token {
        Some(response) => response.text().await.ok(),
        None => None,
    };
    let mut request = client.get(format!("{endpoint}/latest/meta-data/spot/instance-action"));
    if let Some(token) = token.as_deref() {
        request = request.header("X-aws-ec2-metadata-token", token);
    }
    let response = request
        .send()
        .await
        .context("instance metadata unreachable")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        bail!("instance metadata answered {}", response.status());
    }
    let body = response.text().await?;
    parse_instance_action(&body).map(Some)
}

/// Start the watcher for `config` (validated by the manager at startup).
pub fn spawn_watcher(config: WorkerSpotInterruptionConfig) {
    let every = config
        .poll_every()
        .unwrap_or(crate::structs::DEFAULT_SPOT_POLL_EVERY);
    let endpoint = config.endpoint();
    info!(
        provider = %config.provider,
        endpoint = %endpoint,
        every_secs = every.as_secs(),
        "Watching for spot interruption notices"
    );
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .no_proxy()
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                warn!(error = %err, "Spot interruption watcher could not start");
                return;
            }
        };
        loop {
            match poll_aws(&client, &endpoint).await {
                Ok(notice) => set_notice(notice),
                Err(err) => debug!(error = %format!("{err:#}"), "Spot interruption poll failed"),
            }
            tokio::time::sleep(every).await;
        }
    });
}

#[cfg(test)]
mod interruption_test {
    use super::*;

    #[test]
    fn an_instance_action_becomes_a_notice() -> R<()> {
        let notice =
            parse_instance_action(r#"{"action": "terminate", "time": "2026-11-12T08:22:00Z"}"#)?;
        assert_eq!(notice.deadline_unix, 1_794_471_720);
        assert_eq!(notice.source, "aws-imds");
        assert_eq!(notice.detail, "spot instance-action: terminate");
        assert!(parse_instance_action("not json").is_err());
        assert!(parse_instance_action(r#"{"action": "stop", "time": "soon"}"#).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn the_watcher_reads_a_notice_and_its_withdrawal() -> R<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let body = r#"{"action":"stop","time":"2026-11-12T08:22:00Z"}"#;
        let answers = std::sync::Arc::new(Mutex::new(vec![
            "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n".to_string(),
            format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            ),
        ]));
        let served = answers.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buffer = [0u8; 2048];
                let read = stream.read(&mut buffer).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                let reply =
                    if request.starts_with("PUT") {
                        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\ntoken".to_string()
                    } else {
                        served.lock().unwrap().pop().unwrap_or_else(|| {
                            "HTTP/1.1 500 Oops\r\ncontent-length: 0\r\n\r\n".into()
                        })
                    };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
        });
        let client = reqwest::Client::builder().no_proxy().build()?;
        let notice = poll_aws(&client, &endpoint).await?.context("a notice")?;
        assert_eq!(notice.detail, "spot instance-action: stop");
        assert!(poll_aws(&client, &endpoint).await?.is_none(), "404: none");
        assert!(poll_aws(&client, &endpoint).await.is_err(), "500: unknown");
        Ok(())
    }
}
