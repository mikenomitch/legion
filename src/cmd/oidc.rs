//! OIDC browser login for the CLI (`legion login --method <oidc-method>`).
//!
//! Standard authorization-code flow with PKCE for a public client:
//!
//! 1. Discover the IdP's endpoints from the issuer
//!    (`/.well-known/openid-configuration`).
//! 2. Start a loopback callback listener on `127.0.0.1:<port>` and open the
//!    browser at the authorization URL (PKCE challenge + anti-CSRF state).
//! 3. Receive the authorization code on the callback, exchange it at the
//!    token endpoint with the PKCE verifier (no client secret), and take the
//!    `id_token`.
//! 4. Hand the ID token to the manager's `Login` RPC, which validates it
//!    against the configured auth method and mints a Legion ACL token.
//!
//! The redirect URI (`http://127.0.0.1:<port>/oidc/callback`) must be
//! registered with the identity provider for the configured client ID.

use crate::prelude::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Default loopback port for the CLI's OIDC callback listener.
pub const DEFAULT_CALLBACK_PORT: u16 = 4749;

/// PKCE verifier/challenge pair plus the anti-CSRF state value.
#[derive(Debug, Clone)]
pub struct PkceFlow {
    pub verifier: String,
    pub challenge: String,
    pub state: String,
}

impl PkceFlow {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 48];
        rand::thread_rng().fill_bytes(&mut bytes);
        let verifier = URL_SAFE_NO_PAD.encode(bytes);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut state_bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut state_bytes);
        PkceFlow {
            verifier,
            challenge,
            state: URL_SAFE_NO_PAD.encode(state_bytes),
        }
    }
}

/// The IdP endpoints needed for the code flow.
#[derive(Debug, Clone)]
pub struct OidcEndpoints {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

/// Fetches the OIDC discovery document from the issuer.
pub async fn discover(issuer: &str) -> R<OidcEndpoints> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let discovery: Value = reqwest::get(&url)
        .await
        .with_context(|| format!("failed to fetch {url}"))?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("invalid discovery document at {url}"))?;
    Ok(OidcEndpoints {
        authorization_endpoint: discovery["authorization_endpoint"]
            .as_str()
            .context("discovery document missing authorization_endpoint")?
            .to_string(),
        token_endpoint: discovery["token_endpoint"]
            .as_str()
            .context("discovery document missing token_endpoint")?
            .to_string(),
    })
}

pub fn redirect_uri(port: u16) -> String {
    format!("http://127.0.0.1:{port}/oidc/callback")
}

/// Builds the browser authorization URL.
pub fn authorize_url(
    authorization_endpoint: &str,
    client_id: &str,
    scopes: &[String],
    pkce: &PkceFlow,
    port: u16,
) -> R<String> {
    let mut url = url::Url::parse(authorization_endpoint)
        .with_context(|| format!("invalid authorization endpoint {authorization_endpoint}"))?;
    let scope = if scopes.is_empty() {
        "openid".to_string()
    } else {
        scopes.join(" ")
    };
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri(port))
        .append_pair("scope", &scope)
        .append_pair("state", &pkce.state)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256");
    Ok(url.to_string())
}

/// Extracts `code` from the callback request line, verifying `state`.
///
/// `request_line` is the first line of the HTTP request, for example
/// `GET /oidc/callback?code=abc&state=xyz HTTP/1.1`.
pub fn parse_callback(request_line: &str, expected_state: &str) -> R<String> {
    let path = request_line
        .split_whitespace()
        .nth(1)
        .context("malformed callback request")?;
    let url = url::Url::parse(&format!("http://localhost{path}"))
        .context("malformed callback request path")?;
    if url.path() != "/oidc/callback" {
        bail!("unexpected callback path {}", url.path());
    }
    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.to_string()),
            "state" => state = Some(value.to_string()),
            "error" => error = Some(value.to_string()),
            _ => {}
        }
    }
    if let Some(error) = error {
        bail!("identity provider returned an error: {error}");
    }
    if state.as_deref() != Some(expected_state) {
        bail!("callback state mismatch (possible CSRF); aborting login");
    }
    code.context("callback is missing the authorization code")
}

/// Waits for exactly one callback on the loopback listener and returns the
/// authorization code.
pub async fn wait_for_callback(port: u16, expected_state: &str) -> R<String> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| {
            format!("failed to bind the OIDC callback listener on 127.0.0.1:{port} (use --callback-port to change it)")
        })?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut buf = vec![0u8; 8192];
        let read = socket.read(&mut buf).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..read]);
        let Some(request_line) = request.lines().next() else {
            continue;
        };
        // Browsers also fetch /favicon.ico; only handle the callback path.
        if !request_line.contains("/oidc/callback") {
            let _ = socket
                .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n")
                .await;
            continue;
        }
        let result = parse_callback(request_line, expected_state);
        let body = match &result {
            Ok(_) => {
                "<html><body><h2>Logged in to Legion.</h2>You can close this tab.</body></html>"
            }
            Err(_) => {
                "<html><body><h2>Login failed.</h2>Check the terminal for details.</body></html>"
            }
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
        return result;
    }
}

/// Exchanges the authorization code for tokens and returns the `id_token`.
pub async fn exchange_code(
    token_endpoint: &str,
    client_id: &str,
    code: &str,
    verifier: &str,
    port: u16,
) -> R<String> {
    let response = reqwest::Client::new()
        .post(token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &redirect_uri(port)),
            ("client_id", client_id),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .with_context(|| format!("failed to reach token endpoint {token_endpoint}"))?;
    let status = response.status();
    let payload: Value = response
        .json()
        .await
        .context("token endpoint returned invalid JSON")?;
    if !status.is_success() {
        bail!(
            "token endpoint returned {status}: {}",
            payload["error_description"]
                .as_str()
                .or(payload["error"].as_str())
                .unwrap_or("unknown error")
        );
    }
    payload["id_token"]
        .as_str()
        .map(str::to_string)
        .context("token endpoint response is missing id_token (is `openid` in the scopes?)")
}

/// Opens the system browser, best effort. The URL is always printed too.
pub fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let command = "open";
    #[cfg(not(target_os = "macos"))]
    let command = "xdg-open";
    let _ = std::process::Command::new(command)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

#[cfg(test)]
mod oidc_test {
    use super::*;

    #[test]
    fn pkce_challenge_is_s256_of_verifier() {
        let pkce = PkceFlow::generate();
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(pkce.verifier.as_bytes()));
        assert_eq!(pkce.challenge, expected);
        // Verifier length within RFC 7636 bounds (43..=128).
        assert!((43..=128).contains(&pkce.verifier.len()));
        // Two flows never share state/verifier.
        let other = PkceFlow::generate();
        assert_ne!(pkce.verifier, other.verifier);
        assert_ne!(pkce.state, other.state);
    }

    #[test]
    fn authorize_url_carries_pkce_and_state() -> R<()> {
        let pkce = PkceFlow::generate();
        let url = authorize_url(
            "https://idp.test/authorize",
            "legion-cli",
            &["openid".into(), "email".into()],
            &pkce,
            4749,
        )?;
        assert!(url.starts_with("https://idp.test/authorize?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=legion-cli"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains(&format!("code_challenge={}", pkce.challenge)));
        assert!(url.contains(&format!("state={}", pkce.state)));
        assert!(url.contains("scope=openid+email"));
        assert!(url.contains("127.0.0.1%3A4749%2Foidc%2Fcallback"));
        Ok(())
    }

    #[test]
    fn parse_callback_validates_state_and_errors() {
        let code = parse_callback("GET /oidc/callback?code=abc&state=xyz HTTP/1.1", "xyz")
            .expect("valid callback parses");
        assert_eq!(code, "abc");

        // State mismatch is rejected (CSRF protection).
        assert!(parse_callback("GET /oidc/callback?code=abc&state=evil HTTP/1.1", "xyz").is_err());
        // IdP-reported errors surface.
        let err = parse_callback(
            "GET /oidc/callback?error=access_denied&state=xyz HTTP/1.1",
            "xyz",
        )
        .unwrap_err();
        assert!(err.to_string().contains("access_denied"));
        // Missing code is an error.
        assert!(parse_callback("GET /oidc/callback?state=xyz HTTP/1.1", "xyz").is_err());
    }

    #[tokio::test]
    async fn exchange_code_posts_form_and_returns_id_token() -> R<()> {
        // Stub token endpoint asserting the form fields arrive.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 8192];
            let mut request = Vec::new();
            loop {
                let Ok(read) = socket.read(&mut buf).await else {
                    return;
                };
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                let text = String::from_utf8_lossy(&request);
                if let Some((headers, body)) = text.split_once("\r\n\r\n") {
                    let content_length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= content_length {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&request);
            let ok = text.contains("grant_type=authorization_code")
                && text.contains("code=the-code")
                && text.contains("code_verifier=the-verifier");
            let body = if ok {
                r#"{"id_token": "the-id-token", "access_token": "at"}"#
            } else {
                r#"{"error": "invalid_request"}"#
            };
            let status = if ok { "200 OK" } else { "400 Bad Request" };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });

        let id_token = exchange_code(
            &format!("http://{addr}/token"),
            "legion-cli",
            "the-code",
            "the-verifier",
            4749,
        )
        .await?;
        assert_eq!(id_token, "the-id-token");
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_callback_serves_browser_and_returns_code() -> R<()> {
        // Bind an ephemeral port by probing: use port 0 trick via std listener.
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = probe.local_addr()?.port();
        drop(probe);

        let waiter = tokio::spawn(async move { wait_for_callback(port, "xyz").await });
        // Give the listener a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let body = reqwest::get(format!(
            "http://127.0.0.1:{port}/oidc/callback?code=abc&state=xyz"
        ))
        .await?
        .text()
        .await?;
        assert!(body.contains("Logged in"));
        let code = waiter.await??;
        assert_eq!(code, "abc");
        Ok(())
    }
}
