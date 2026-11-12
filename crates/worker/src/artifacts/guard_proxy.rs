//! The guarded forward proxy: the connection pin for HTTP clients Legion does
//! not build itself (docs/artifacts.md, "What the host guard checks").
//!
//! The `http(s)` and Git fetchers resolve a host once, judge every address
//! with the worker's [`HostGuard`], and pin their connection to the judged
//! addresses. `oci-client` and `object_store` build their own `reqwest`
//! clients with no resolver hook, re-resolve the host, and follow redirects
//! (a registry's blob CDN, its token `realm`, an S3 `307`) on their own. A
//! fetch that judged the registry's name up front could still be sent
//! somewhere else.
//!
//! Both clients take an HTTP proxy, so they get this one: a listener on
//! loopback that speaks `CONNECT` (for `https`) and absolute-form requests
//! (for plain `http`). Each new connection, the first and every redirect's,
//! is resolved here, judged here, and dialed only to an address the guard
//! lets through. A refused destination gets `403` with `Legion-Denied-By:
//! host-guard`, and the proxy remembers the reason so the fetch can report it.
//!
//! The listener sits on `127.0.0.1`. It takes no secret: it only ever
//! narrows what a caller could dial on its own, since everything it forwards
//! to is an address a process in the host's network namespace reaches
//! directly, and a gated allocation cannot reach it at all (loopback and the
//! node's addresses are guarded classes). The `Proxy-Authorization` user a
//! client sends is a label the proxy files its refusals under. reqwest drops
//! that header on a cross-origin redirect of a plain-`http` request, so such a
//! redirect is still judged, but its refusal is filed under no label.
//!
//! One proxy serves one guard (one per worker in practice), on a small
//! runtime of its own, and shuts down once the guard is dropped.

use crate::prelude::*;
use crate::structs::host_guard::HostGuard;
use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The longest request head the proxy reads.
const MAX_HEAD: usize = 32 * 1024;
/// How long a client has to send its request head, and the proxy to dial.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// How many refusals the proxy remembers for [`GuardProxy::denials_since`].
const MAX_DENIALS: usize = 256;
/// How often an idle listener checks that its guard still exists.
const LIVENESS_CHECK: Duration = Duration::from_secs(30);

/// One destination the proxy refused.
#[derive(Debug, Clone)]
pub struct ProxyDenial {
    /// The proxy user the client connected as (see [`GuardProxy::url`]).
    pub user: String,
    /// The host the client asked for, as it asked.
    pub host: String,
    pub port: u16,
    /// The guard's reason, worded as the `http(s)` fetcher words it.
    pub reason: String,
    pub at: Instant,
}

/// A running guarded proxy. Cheap to clone the `Arc` of; the listener lives
/// as long as its guard.
#[derive(Debug)]
pub struct GuardProxy {
    addr: SocketAddr,
    denials: Mutex<VecDeque<ProxyDenial>>,
}

/// The proxies' runtime, built on first use.
fn runtime() -> R<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<std::result::Result<tokio::runtime::Runtime, String>> =
        OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("legion-guard-proxy")
                .enable_all()
                .build()
                .map_err(|err| err.to_string())
        })
        .as_ref()
        .map_err(|err| anyhow!("start the guarded proxy's runtime: {err}"))
}

/// Whether the worker's environment names an HTTP proxy for its outbound
/// requests (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, either case). The
/// clients would otherwise send through it, and it resolves and connects on
/// its own, so a fetch through an operator's proxy is judged by name only.
pub fn system_proxy_configured() -> bool {
    [
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]
    .iter()
    .any(|name| std::env::var(name).is_ok_and(|value| !value.trim().is_empty()))
}

impl GuardProxy {
    /// The proxy for `guard`, started on first use and shared after.
    pub fn for_guard(guard: &Arc<HostGuard>) -> R<Arc<GuardProxy>> {
        type Entry = (Weak<HostGuard>, Arc<GuardProxy>);
        static PROXIES: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();
        let mut proxies = PROXIES
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        proxies.retain(|(weak, _)| weak.strong_count() > 0);
        if let Some((_, proxy)) = proxies
            .iter()
            .find(|(weak, _)| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(guard)))
        {
            return Ok(proxy.clone());
        }
        let proxy = Self::start(Arc::downgrade(guard))?;
        proxies.push((Arc::downgrade(guard), proxy.clone()));
        Ok(proxy)
    }

    fn start(guard: Weak<HostGuard>) -> R<Arc<GuardProxy>> {
        let runtime = runtime()?;
        let std_listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .context("bind the guarded proxy on loopback")?;
        std_listener.set_nonblocking(true)?;
        let addr = std_listener.local_addr()?;
        let proxy = Arc::new(GuardProxy {
            addr,
            denials: Mutex::new(VecDeque::new()),
        });
        let listener = {
            let _entered = runtime.enter();
            TcpListener::from_std(std_listener)?
        };
        let weak_proxy = Arc::downgrade(&proxy);
        runtime.spawn(accept_loop(listener, guard, weak_proxy));
        debug!(%addr, "guarded proxy started");
        Ok(proxy)
    }

    /// The proxy URL to hand a client, connecting as `user`: a label the
    /// proxy files its refusals under, so a fetch reads back only its own
    /// client's ([`Self::denials_since`]).
    pub fn url(&self, user: &str) -> String {
        format!("http://{user}:legion@{}", self.addr)
    }

    /// The loopback address the proxy listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The refusals made for `user` at or after `since`, oldest first.
    pub fn denials_since(&self, user: &str, since: Instant) -> Vec<ProxyDenial> {
        self.denials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|denial| denial.user == user && denial.at >= since)
            .cloned()
            .collect()
    }

    fn record(&self, denial: ProxyDenial) {
        let mut denials = self.denials.lock().unwrap_or_else(|p| p.into_inner());
        if denials.len() >= MAX_DENIALS {
            denials.pop_front();
        }
        denials.push_back(denial);
    }
}

/// The label a `Proxy-Authorization: Basic` value carries, or `""`.
fn proxy_user(value: Option<&str>) -> String {
    use base64::Engine;
    value
        .and_then(|value| value.trim().strip_prefix("Basic "))
        .and_then(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()
        })
        .and_then(|decoded| String::from_utf8(decoded).ok())
        .map(|decoded| {
            decoded
                .split_once(':')
                .map_or(decoded.as_str(), |(user, _)| user)
                .to_string()
        })
        .unwrap_or_default()
}

/// Add the refusals `proxy` made for `user` since `since` to a failed
/// fetch's error, so the job sees the guard's reason rather than the
/// client's "unsuccessful tunnel".
pub fn explain(
    err: anyhow::Error,
    proxy: Option<&(Arc<GuardProxy>, String)>,
    since: Instant,
) -> anyhow::Error {
    let Some((proxy, user)) = proxy else {
        return err;
    };
    let denials = proxy.denials_since(user, since);
    let Some(last) = denials.last() else {
        return err;
    };
    err.context(format!(
        "the host guard refused {}:{}: {}; it is opened only by the worker's \
         hostGuard.allow",
        last.host, last.port, last.reason
    ))
}

async fn accept_loop(listener: TcpListener, guard: Weak<HostGuard>, proxy: Weak<GuardProxy>) {
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(accepted),
            _ = tokio::time::sleep(LIVENESS_CHECK) => None,
        };
        if guard.strong_count() == 0 || proxy.strong_count() == 0 {
            debug!("guarded proxy stopped: its guard is gone");
            return;
        }
        let Some(accepted) = accepted else {
            continue;
        };
        match accepted {
            Ok((conn, _)) => {
                let (guard, proxy) = (guard.clone(), proxy.clone());
                tokio::spawn(async move {
                    let (Some(guard), Some(proxy)) = (guard.upgrade(), proxy.upgrade()) else {
                        return;
                    };
                    if let Err(err) = serve(conn, &guard, &proxy).await {
                        debug!(error = %format!("{err:#}"), "guarded proxy connection ended");
                    }
                });
            }
            Err(err) => {
                warn!(error = %err, "guarded proxy accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// A parsed request head.
struct Head {
    method: String,
    target: String,
    version: String,
    headers: Vec<(String, String)>,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

fn parse_head(raw: &[u8]) -> Option<Head> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut lines = text.split("\r\n");
    let mut request = lines.next()?.split(' ');
    let (method, target, version) = (request.next()?, request.next()?, request.next()?);
    if request.next().is_some() || !version.starts_with("HTTP/1.") {
        return None;
    }
    let mut headers = Vec::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':')?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Some(Head {
        method: method.to_string(),
        target: target.to_string(),
        version: version.to_string(),
        headers,
    })
}

async fn respond(conn: &mut TcpStream, status: &str, body: &str, denied: bool) -> R<()> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if denied {
        response.push_str(&format!(
            "Legion-Denied-By: {}\r\n",
            crate::structs::host_guard::HOST_GUARD_SOURCE
        ));
    }
    response.push_str("\r\n");
    response.push_str(body);
    conn.write_all(response.as_bytes()).await?;
    conn.shutdown().await.ok();
    Ok(())
}

/// `host:port` from a `CONNECT` target (`[v6]:port` for IPv6).
fn connect_target(target: &str) -> Option<(String, u16)> {
    let (host, port) = target.rsplit_once(':')?;
    let port: u16 = port.parse().ok().filter(|port| *port > 0)?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    (!host.is_empty()).then(|| (host.to_string(), port))
}

/// What a resolution and judgement came to.
enum Judged {
    Allowed(Vec<SocketAddr>),
    Denied(String),
    Unresolved(String),
}

async fn resolve_and_judge(guard: &HostGuard, host: &str, port: u16) -> Judged {
    let addrs: Vec<SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => {
            match tokio::time::timeout(IO_TIMEOUT, tokio::net::lookup_host((host, port))).await {
                Ok(Ok(addrs)) => addrs.collect(),
                Ok(Err(err)) => return Judged::Unresolved(format!("resolve {host}: {err}")),
                Err(_) => return Judged::Unresolved(format!("resolve {host}: timed out")),
            }
        }
    };
    let mut denial = None;
    let allowed: Vec<SocketAddr> = addrs
        .into_iter()
        .filter(|addr| match guard.judge(addr.ip(), addr.port()) {
            Ok(()) => true,
            Err(denied) => {
                denial.get_or_insert(denied);
                false
            }
        })
        .collect();
    match (allowed.is_empty(), denial) {
        (false, _) => Judged::Allowed(allowed),
        (true, Some(denied)) => Judged::Denied(denied.reason(Some(host))),
        (true, None) => Judged::Unresolved(format!("{host} resolved to no address")),
    }
}

async fn dial(addrs: &[SocketAddr]) -> R<TcpStream> {
    let mut last = None;
    for addr in addrs {
        match tokio::time::timeout(IO_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(err)) => last = Some(anyhow!("connect {addr}: {err}")),
            Err(_) => last = Some(anyhow!("connect {addr}: timed out")),
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("no address to dial")))
}

async fn serve(mut conn: TcpStream, guard: &HostGuard, proxy: &GuardProxy) -> R<()> {
    let mut buf = Vec::with_capacity(4096);
    let head_end = loop {
        if let Some(end) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        if buf.len() > MAX_HEAD {
            return respond(&mut conn, "431 Request Header Fields Too Large", "", false).await;
        }
        let read = tokio::time::timeout(IO_TIMEOUT, conn.read_buf(&mut buf))
            .await
            .map_err(|_| anyhow!("request head timed out"))??;
        if read == 0 {
            return Ok(());
        }
    };
    let Some(head) = parse_head(&buf[..head_end - 4]) else {
        return respond(&mut conn, "400 Bad Request", "malformed request", false).await;
    };
    let leftover = buf[head_end..].to_vec();
    let user = proxy_user(head.header("proxy-authorization"));

    let connect = head.method.eq_ignore_ascii_case("CONNECT");
    let (host, port, origin_form) = if connect {
        match connect_target(&head.target) {
            Some((host, port)) => (host, port, None),
            None => {
                return respond(&mut conn, "400 Bad Request", "bad CONNECT target", false).await
            }
        }
    } else {
        let Ok(url) = url::Url::parse(&head.target) else {
            return respond(
                &mut conn,
                "400 Bad Request",
                "absolute-form URL required",
                false,
            )
            .await;
        };
        if url.scheme() != "http" {
            return respond(
                &mut conn,
                "400 Bad Request",
                "only http:// is forwarded",
                false,
            )
            .await;
        }
        let Some(host) = url.host_str() else {
            return respond(&mut conn, "400 Bad Request", "URL has no host", false).await;
        };
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = url.port_or_known_default().unwrap_or(80);
        let mut origin = url.path().to_string();
        if let Some(query) = url.query() {
            origin.push('?');
            origin.push_str(query);
        }
        (host, port, Some(origin))
    };

    let addrs = match resolve_and_judge(guard, &host, port).await {
        Judged::Allowed(addrs) => addrs,
        Judged::Denied(reason) => {
            debug!(host = %host, port, %reason, "guarded proxy refused a destination");
            proxy.record(ProxyDenial {
                user,
                host: host.clone(),
                port,
                reason: reason.clone(),
                at: Instant::now(),
            });
            return respond(&mut conn, "403 Forbidden", &reason, true).await;
        }
        Judged::Unresolved(reason) => {
            return respond(&mut conn, "502 Bad Gateway", &reason, false).await;
        }
    };
    let mut upstream = match dial(&addrs).await {
        Ok(upstream) => upstream,
        Err(err) => return respond(&mut conn, "502 Bad Gateway", &format!("{err:#}"), false).await,
    };

    match origin_form {
        None => {
            conn.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await?;
        }
        Some(origin) => {
            let mut forwarded = format!("{} {origin} {}\r\n", head.method, head.version);
            let mut has_host = false;
            for (name, value) in &head.headers {
                let lower = name.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "proxy-authorization" | "proxy-connection" | "connection" | "keep-alive"
                ) {
                    continue;
                }
                has_host |= lower == "host";
                forwarded.push_str(&format!("{name}: {value}\r\n"));
            }
            if !has_host {
                forwarded.push_str(&format!("Host: {host}:{port}\r\n"));
            }
            // One request per upstream connection: a client reusing this
            // connection for another host would otherwise reach it unjudged.
            forwarded.push_str("Connection: close\r\n\r\n");
            upstream.write_all(forwarded.as_bytes()).await?;
        }
    }
    if !leftover.is_empty() {
        upstream.write_all(&leftover).await?;
    }
    tokio::io::copy_bidirectional(&mut conn, &mut upstream).await?;
    Ok(())
}

#[cfg(test)]
mod guard_proxy_test {
    use super::*;
    use crate::structs::host_guard::HostGuardConfig;
    use std::io::{Read, Write};

    /// A one-shot HTTP server on loopback answering every request with
    /// `body`.
    fn origin(body: &'static str) -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { return };
                let mut buf = [0u8; 4096];
                let _ = conn.read(&mut buf);
                let _ = write!(
                    conn,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        addr
    }

    fn client(proxy: &GuardProxy, user: &str) -> reqwest::blocking::Client {
        reqwest::blocking::Client::builder()
            .proxy(reqwest::Proxy::all(proxy.url(user)).expect("proxy"))
            .build()
            .expect("client")
    }

    #[test]
    fn a_guarded_destination_is_refused_and_explained_test() -> R<()> {
        let target = origin("secret");
        let guard = Arc::new(HostGuard::new(&HostGuardConfig::default())?);
        let proxy = GuardProxy::for_guard(&guard)?;
        let since = Instant::now();
        let response = client(&proxy, "job-a")
            .get(format!("http://{target}/latest/meta-data"))
            .send()?;
        assert_eq!(response.status(), 403);
        assert_eq!(
            response
                .headers()
                .get("legion-denied-by")
                .and_then(|v| v.to_str().ok()),
            Some("host-guard")
        );
        let denials = proxy.denials_since("job-a", since);
        assert_eq!(denials.len(), 1, "{denials:?}");
        assert!(denials[0].reason.contains("loopback"), "{denials:?}");
        assert!(
            proxy.denials_since("job-b", since).is_empty(),
            "a refusal is filed under its own client"
        );
        // CONNECT (an https client's path) is judged the same way.
        let err = client(&proxy, "job-a")
            .get(format!("https://{target}/"))
            .send()
            .expect_err("tunnel refused");
        let explained = explain(err.into(), Some(&(proxy.clone(), "job-a".into())), since);
        assert!(
            format!("{explained:#}").contains("the host guard refused 127.0.0.1"),
            "{explained:#}"
        );
        Ok(())
    }

    #[test]
    fn an_allowed_destination_is_forwarded_test() -> R<()> {
        let target = origin("hello");
        let guard = Arc::new(HostGuard::new(&HostGuardConfig {
            allow: vec![format!("127.0.0.1:{}", target.port())],
            ..HostGuardConfig::default()
        })?);
        let proxy = GuardProxy::for_guard(&guard)?;
        assert!(Arc::ptr_eq(&proxy, &GuardProxy::for_guard(&guard)?));
        let body = client(&proxy, "job")
            .get(format!("http://{target}/x?y=1"))
            .send()?
            .text()?;
        assert_eq!(body, "hello");
        Ok(())
    }

    #[test]
    fn a_redirect_is_judged_on_its_own_test() -> R<()> {
        // An allowed server that redirects to a guarded one.
        let secret = origin("metadata");
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let redirector = listener.local_addr()?;
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { return };
                let mut buf = [0u8; 4096];
                let _ = conn.read(&mut buf);
                let _ = write!(
                    conn,
                    "HTTP/1.1 302 Found\r\nLocation: http://{secret}/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });
        let guard = Arc::new(HostGuard::new(&HostGuardConfig {
            allow: vec![format!("127.0.0.1:{}", redirector.port())],
            ..HostGuardConfig::default()
        })?);
        let proxy = GuardProxy::for_guard(&guard)?;
        let response = client(&proxy, "job")
            .get(format!("http://{redirector}/"))
            .send()?;
        assert_eq!(response.status(), 403, "the redirect's target is refused");
        Ok(())
    }

    #[test]
    fn a_request_without_a_label_is_still_judged_test() -> R<()> {
        let guard = Arc::new(HostGuard::new(&HostGuardConfig::default())?);
        let proxy = GuardProxy::for_guard(&guard)?;
        let mut conn = std::net::TcpStream::connect(proxy.addr())?;
        write!(
            conn,
            "CONNECT 127.0.0.1:443 HTTP/1.1\r\nHost: 127.0.0.1:443\r\n\r\n"
        )?;
        let mut response = String::new();
        conn.read_to_string(&mut response)?;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response.contains("Legion-Denied-By: host-guard"),
            "{response}"
        );
        assert_eq!(proxy_user(None), "");
        assert_eq!(proxy_user(Some("Basic b2NpLTE6bGVnaW9u")), "oci-1");
        Ok(())
    }

    #[test]
    fn connect_targets_parse_test() {
        assert_eq!(
            connect_target("registry.example:443"),
            Some(("registry.example".into(), 443))
        );
        assert_eq!(connect_target("[::1]:5000"), Some(("::1".into(), 5000)));
        assert_eq!(connect_target("no-port"), None);
        assert_eq!(connect_target(":443"), None);
        assert_eq!(connect_target("host:0"), None);
    }
}
