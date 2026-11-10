//! `http://` / `https://` fetcher: streaming downloads (no whole-body
//! buffering) with bearer/basic/header credentials, redirect and size
//! limits. Replaces the old getter's buffer-everything behavior.

use reqwest::blocking::Client;
use reqwest::redirect::Policy;

use crate::artifacts::fetcher::{
    ArtifactFetcher, Credential, FetchRequest, FetchedArtifact, FetchedPayload,
};
use crate::artifacts::fetchers::file::copy_with_deadline;
use crate::artifacts::stage::ArtifactSink;
use crate::prelude::*;

const MAX_REDIRECTS: usize = 5;

#[derive(Debug, Default)]
pub struct HttpFetcher;

impl HttpFetcher {
    pub fn new() -> Self {
        Self
    }

    /// A blocking client for one request to `url`, following no redirects
    /// (the fetch follows them itself, judging each hop). Built per request:
    /// `reqwest::blocking` clients may not be constructed inside an async
    /// runtime, and fetchers always run on staging threads while the
    /// registry itself is built wherever (worker startup, manager admission).
    ///
    /// With a host guard, the host is resolved here, every address judged,
    /// and the client pinned to the ones that pass, so the connection goes
    /// where the guard looked (a second lookup cannot answer differently).
    fn client_for(
        &self,
        url: &reqwest::Url,
        guard: Option<&crate::structs::host_guard::HostGuard>,
        redacted_source: &str,
    ) -> R<Client> {
        let mut builder = Client::builder().redirect(Policy::none());
        if let Some(guard) = guard {
            let host = url
                .host()
                .with_context(|| format!("{redacted_source} names no host"))?;
            let port = url
                .port_or_known_default()
                .with_context(|| format!("{redacted_source} names no port"))?;
            let allowed = judged_addrs(guard, &host, port, redacted_source)?;
            if let url::Host::Domain(domain) = host {
                builder = builder.resolve_to_addrs(domain, &allowed);
            }
        }
        builder.build().context("build http artifact client")
    }
}

/// The addresses `host:port` resolves to that the worker's host guard lets
/// an artifact fetch reach. Fails, naming the guard's reason, when it
/// refuses every one.
pub(crate) fn judged_addrs(
    guard: &crate::structs::host_guard::HostGuard,
    host: &url::Host<&str>,
    port: u16,
    redacted_source: &str,
) -> R<Vec<std::net::SocketAddr>> {
    let addrs: Vec<std::net::SocketAddr> = match host {
        url::Host::Ipv4(ip) => vec![(*ip, port).into()],
        url::Host::Ipv6(ip) => vec![(*ip, port).into()],
        url::Host::Domain(domain) => {
            use std::net::ToSocketAddrs;
            (*domain, port)
                .to_socket_addrs()
                .with_context(|| format!("resolve {domain} for {redacted_source}"))?
                .collect()
        }
    };
    let mut denial = None;
    let allowed: Vec<std::net::SocketAddr> = addrs
        .into_iter()
        .filter(|addr| match guard.judge(addr.ip(), addr.port()) {
            Ok(()) => true,
            Err(denied) => {
                denial.get_or_insert(denied);
                false
            }
        })
        .collect();
    if allowed.is_empty() {
        match denial {
            Some(denied) => bail!(
                "{redacted_source}: the host guard refuses {} ({}); an artifact may not be \
                 fetched from it unless the worker's hostGuard.allow opens {}:{port}",
                denied.ip,
                denied.class.short(),
                denied.ip
            ),
            None => bail!("{redacted_source} resolved to no address"),
        }
    }
    Ok(allowed)
}

/// The next URL of a redirect `response` to `from`, or `None` when it is not
/// a redirect. Only `http(s)` destinations, and never from `https` down to
/// `http`.
fn redirect_target(
    from: &reqwest::Url,
    response: &reqwest::blocking::Response,
    redacted_source: &str,
) -> R<Option<reqwest::Url>> {
    if !response.status().is_redirection() {
        return Ok(None);
    }
    let Some(location) = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    let next = from
        .join(location)
        .with_context(|| format!("{redacted_source} redirected to an invalid location"))?;
    match (from.scheme(), next.scheme()) {
        (_, "https") | ("http", "http") => Ok(Some(next)),
        ("https", "http") => bail!("{redacted_source} redirected from https to plain http"),
        (_, other) => bail!("{redacted_source} redirected to an unsupported scheme {other}"),
    }
}

impl ArtifactFetcher for HttpFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["http", "https"]
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let origin = req.url.origin();
        let mut url = req.url.clone();
        let mut redirects = 0;
        let response = loop {
            let mut request = self
                .client_for(&url, req.limits.egress.as_deref(), req.redacted_source)?
                .get(url.clone());
            // Credentials go to the source's own origin only, never along a
            // redirect to another host (a header credential included).
            if url.origin() == origin {
                request = match req.credential {
                    None => request,
                    Some(Credential::Basic { username, password }) => {
                        request.basic_auth(username, Some(password))
                    }
                    Some(Credential::Bearer { token }) => request.bearer_auth(token),
                    Some(Credential::Headers { headers }) => {
                        headers.iter().fold(request, |request, (name, value)| {
                            request.header(name, value)
                        })
                    }
                    Some(Credential::Aws { .. } | Credential::AwsAmbient) => {
                        bail!(
                            "aws credentials are not valid for {} (use them with s3:// sources)",
                            req.redacted_source
                        );
                    }
                };
            }
            // reqwest errors render the full URL (query included); strip it so
            // presigned-URL secrets and tokens never reach error strings.
            let response = request
                .send()
                .map_err(reqwest::Error::without_url)
                .with_context(|| format!("request {}", req.redacted_source))?;
            match redirect_target(&url, &response, req.redacted_source)? {
                Some(next) if redirects < MAX_REDIRECTS => {
                    redirects += 1;
                    url = next;
                }
                Some(_) => bail!(
                    "{} redirected more than {MAX_REDIRECTS} times",
                    req.redacted_source
                ),
                None => break response,
            }
        };
        let response = response
            .error_for_status()
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("request {} returned error status", req.redacted_source))?;

        if let Some(length) = response.content_length() {
            if length > req.limits.max_bytes {
                bail!(
                    "{} is {} bytes, over the {}-byte artifact size limit",
                    req.redacted_source,
                    length,
                    req.limits.max_bytes
                );
            }
            sink.set_bytes_total(Some(length));
        }

        let file_name = req
            .url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|segment| !segment.is_empty())
            .unwrap_or("download")
            .to_string();
        let mut writer = sink.file_writer(&file_name)?;
        let mut response = response;
        copy_with_deadline(&mut response, &mut writer, req)?;
        let (path, digest, bytes) = writer.finish()?;
        Ok(FetchedArtifact {
            bytes,
            payload: FetchedPayload::File { path, digest },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::fetcher::FetchLimits;
    use crate::structs::parse_artifact_source;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use tempfile::tempdir;

    /// Minimal single-shot HTTP server for fetcher tests. Returns the bound
    /// address and a join handle that serves exactly `responses` requests.
    pub(crate) fn serve_http(
        responses: Vec<(String, Vec<u8>)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test http server");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let mut seen_requests = Vec::new();
            for (status_line, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = [0u8; 8192];
                let read = stream.read(&mut buf).unwrap_or(0);
                seen_requests.push(String::from_utf8_lossy(&buf[..read]).into_owned());
                let header = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
            }
            seen_requests
        });
        (format!("http://{addr}"), handle)
    }

    fn fetch(
        source: &str,
        credential: Option<Credential>,
    ) -> R<(FetchedArtifact, tempfile::TempDir)> {
        let staging = tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().join("sink"), 10 * 1024 * 1024)?;
        let parsed = parse_artifact_source(source)?;
        let limits = FetchLimits::default();
        let redacted = crate::structs::redact_source(source);
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: &redacted,
            checksum: None,
            media_type: None,
            credential: credential.as_ref(),
            limits: &limits,
        };
        let fetched = HttpFetcher::new().fetch(&request, &mut sink)?;
        Ok((fetched, staging))
    }

    #[test]
    fn downloads_and_hashes_body() -> R<()> {
        let (base, server) = serve_http(vec![("200 OK".into(), b"artifact bytes".to_vec())]);
        let (fetched, _staging) = fetch(&format!("{base}/tool.bin"), None)?;
        match fetched.payload {
            FetchedPayload::File { path, digest } => {
                assert_eq!(std::fs::read(path)?, b"artifact bytes");
                let expected = {
                    use sha2::{Digest, Sha256};
                    format!("sha256:{:x}", Sha256::digest(b"artifact bytes"))
                };
                assert_eq!(digest, expected);
            }
            other => panic!("expected file payload, got {other:?}"),
        }
        server.join().expect("server");
        Ok(())
    }

    /// A credential goes to the source's own origin: a redirect to another
    /// one (here, another port) is followed without it.
    #[test]
    fn credentials_do_not_follow_a_redirect_to_another_origin_test() -> R<()> {
        let (elsewhere, other) = serve_http(vec![("200 OK".into(), b"moved".to_vec())]);
        let (base, origin) = serve_http(vec![(
            format!("302 Found\r\nLocation: {elsewhere}/tool.bin"),
            Vec::new(),
        )]);
        let (fetched, _staging) = fetch(
            &format!("{base}/tool.bin"),
            Some(Credential::Headers {
                headers: [("X-Api-Key".to_string(), "secret-key".to_string())]
                    .into_iter()
                    .collect(),
            }),
        )?;
        assert!(matches!(fetched.payload, FetchedPayload::File { .. }));
        let first = origin.join().expect("origin");
        let second = other.join().expect("other");
        assert!(
            first[0].to_lowercase().contains("x-api-key: secret-key"),
            "{}",
            first[0]
        );
        assert!(
            !second[0].to_lowercase().contains("secret-key"),
            "{}",
            second[0]
        );
        Ok(())
    }

    /// With the worker's host guard, a source on a guarded address (here
    /// loopback) is refused before anything is sent, and an allow entry for
    /// that address and port opens it.
    #[test]
    fn the_host_guard_judges_artifact_sources_test() -> R<()> {
        use crate::structs::host_guard::{HostGuard, HostGuardConfig};
        let fetch_guarded = |source: &str, guard: HostGuard| -> R<FetchedArtifact> {
            let staging = tempdir()?;
            let mut sink = ArtifactSink::new(staging.path().join("sink"), 1024 * 1024)?;
            let parsed = parse_artifact_source(source)?;
            let limits = FetchLimits {
                egress: Some(std::sync::Arc::new(guard)),
                ..FetchLimits::default()
            };
            let redacted = crate::structs::redact_source(source);
            HttpFetcher::new().fetch(
                &FetchRequest {
                    scheme: &parsed.scheme,
                    url: &parsed.url,
                    redacted_source: &redacted,
                    checksum: None,
                    media_type: None,
                    credential: None,
                    limits: &limits,
                },
                &mut sink,
            )
        };
        let err = fetch_guarded(
            "http://127.0.0.1:9/tool.bin",
            HostGuard::new(&HostGuardConfig::default())?,
        )
        .expect_err("loopback is guarded");
        assert!(format!("{err:#}").contains("host guard refuses"), "{err:#}");
        let err = fetch_guarded(
            "http://169.254.169.254/latest/meta-data/",
            HostGuard::new(&HostGuardConfig::default())?,
        )
        .expect_err("the metadata service is guarded");
        assert!(format!("{err:#}").contains("host guard refuses"), "{err:#}");

        let (base, server) = serve_http(vec![("200 OK".into(), b"ok".to_vec())]);
        let port = base.rsplit(':').next().unwrap_or_default().to_string();
        let open = HostGuard::new(&HostGuardConfig {
            allow: vec![format!("127.0.0.1:{port}")],
            ..HostGuardConfig::default()
        })?;
        fetch_guarded(&format!("{base}/tool.bin"), open)?;
        server.join().expect("server");
        Ok(())
    }

    #[test]
    fn sends_bearer_and_basic_credentials() -> R<()> {
        let (base, server) = serve_http(vec![
            ("200 OK".into(), b"a".to_vec()),
            ("200 OK".into(), b"b".to_vec()),
        ]);
        fetch(
            &format!("{base}/with-bearer"),
            Some(Credential::Bearer {
                token: "tok-123".into(),
            }),
        )?;
        fetch(
            &format!("{base}/with-basic"),
            Some(Credential::Basic {
                username: "u".into(),
                password: "p".into(),
            }),
        )?;
        let requests = server.join().expect("server");
        assert!(
            requests[0].contains("authorization: Bearer tok-123")
                || requests[0].contains("Authorization: Bearer tok-123"),
            "{}",
            requests[0]
        );
        assert!(
            requests[1].to_lowercase().contains("authorization: basic"),
            "{}",
            requests[1]
        );
        Ok(())
    }

    #[test]
    fn http_error_status_fails_with_redacted_source() {
        let (base, server) = serve_http(vec![("404 Not Found".into(), Vec::new())]);
        let err = fetch(&format!("{base}/missing?token=secret"), None).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("error status"), "{rendered}");
        assert!(!rendered.contains("token=secret"), "{rendered}");
        server.join().expect("server");
    }
}
