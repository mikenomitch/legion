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

    /// Builds the blocking client lazily per fetch: `reqwest::blocking`
    /// clients may not be constructed inside an async runtime, and fetchers
    /// always run on staging threads while the registry itself is built
    /// wherever (worker startup, manager admission).
    fn client(&self) -> R<Client> {
        Client::builder()
            .redirect(Policy::limited(MAX_REDIRECTS))
            .build()
            .context("build http artifact client")
    }
}

impl ArtifactFetcher for HttpFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["http", "https"]
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let mut request = self.client()?.get(req.url.clone());
        match req.credential {
            None => {}
            Some(Credential::Basic { username, password }) => {
                request = request.basic_auth(username, Some(password));
            }
            Some(Credential::Bearer { token }) => {
                request = request.bearer_auth(token);
            }
            Some(Credential::Headers { headers }) => {
                for (name, value) in headers {
                    request = request.header(name, value);
                }
            }
            Some(Credential::Aws { .. } | Credential::AwsAmbient) => {
                bail!(
                    "aws credentials are not valid for {} (use them with s3:// sources)",
                    req.redacted_source
                );
            }
        }

        // reqwest errors render the full URL (query included); strip it so
        // presigned-URL secrets and tokens never reach error strings.
        let response = request
            .send()
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("request {}", req.redacted_source))?
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
