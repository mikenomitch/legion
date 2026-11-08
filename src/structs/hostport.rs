//! `host:port` for every address family (mesh-control-path plan §6.5,
//! review NET5): Go's `net.JoinHostPort` / `net.SplitHostPort` rules. An
//! IPv6 literal is written in brackets (`[fd00::7]:5432`) so the last `:`
//! is the port's; a bare IPv6 literal with no port is never split.

/// `host:port`, bracketing an IPv6 literal. A host already in brackets is
/// left as it is.
pub fn join(host: &str, port: u16) -> String {
    let host = host.trim();
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Split `host:port` or `[v6]:port` into the host (without brackets) and
/// the port. `None` for anything else, including a bare IPv6 literal, whose
/// last `:` is not a port separator.
pub fn split(address: &str) -> Option<(&str, u16)> {
    let address = address.trim();
    if let Some(rest) = address.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?.parse().ok()?;
        return Some((host, port));
    }
    let (host, port) = address.rsplit_once(':')?;
    if host.contains(':') || host.is_empty() {
        return None;
    }
    Some((host, port.parse().ok()?))
}

/// The host of an authority, with or without a port: `[::1]:80` and `::1`
/// are `::1`, `10.0.0.5:4848` is `10.0.0.5`, `db.internal` is itself.
pub fn host_of(authority: &str) -> &str {
    let authority = authority.trim();
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split_once(']').map(|(host, _)| host).unwrap_or(rest);
    }
    match authority.matches(':').count() {
        // `host:port`.
        1 => authority
            .split_once(':')
            .map(|(host, _)| host)
            .unwrap_or(authority),
        // `host`, or a bare IPv6 literal.
        _ => authority,
    }
}

/// The host of a URL-ish address (`http://[fd00::5]:4848/x` → `fd00::5`,
/// `10.0.0.5:4848` → `10.0.0.5`), or `None` when it is empty.
pub fn url_host(address: &str) -> Option<&str> {
    let without_scheme = address
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(address);
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    // Userinfo is never part of the host.
    let authority = authority
        .rsplit_once('@')
        .map(|(_, a)| a)
        .unwrap_or(authority);
    let host = host_of(authority);
    (!host.is_empty()).then_some(host)
}

#[cfg(test)]
mod hostport_test {
    use super::*;

    #[test]
    fn join_brackets_ipv6_only() {
        assert_eq!(join("10.0.0.5", 5432), "10.0.0.5:5432");
        assert_eq!(join("db.internal", 80), "db.internal:80");
        assert_eq!(join("fd00::7", 5432), "[fd00::7]:5432");
        assert_eq!(join("::1", 80), "[::1]:80");
        assert_eq!(join("[::1]", 80), "[::1]:80", "already bracketed");
    }

    #[test]
    fn split_follows_join() {
        assert_eq!(split("10.0.0.5:5432"), Some(("10.0.0.5", 5432)));
        assert_eq!(split("[fd00::7]:5432"), Some(("fd00::7", 5432)));
        assert_eq!(split("db.internal:80"), Some(("db.internal", 80)));
        assert_eq!(split("fd00::7"), None, "a bare v6 literal has no port");
        assert_eq!(split("fd00::7:80"), None, "ambiguous without brackets");
        assert_eq!(split("[fd00::7]"), None);
        assert_eq!(split("host:notaport"), None);
        for (host, port) in [("10.0.0.5", 1), ("fd00::7", 65535), ("::1", 80)] {
            assert_eq!(split(&join(host, port)), Some((host, port)));
        }
        // What a socket address renders as, parses back.
        let v6: std::net::SocketAddr = join("fd00::7", 5432).parse().expect("a SocketAddr");
        assert_eq!(v6.port(), 5432);
    }

    #[test]
    fn hosts_of_authorities_and_urls() {
        assert_eq!(host_of("[::1]:80"), "::1");
        assert_eq!(host_of("::1"), "::1");
        assert_eq!(host_of("10.0.0.5:4848"), "10.0.0.5");
        assert_eq!(host_of("db.internal"), "db.internal");
        assert_eq!(url_host("http://10.0.0.5:4848"), Some("10.0.0.5"));
        assert_eq!(url_host("https://[fd00::5]:4848/x"), Some("fd00::5"));
        assert_eq!(url_host("http://fd00::5"), Some("fd00::5"));
        assert_eq!(url_host("10.0.0.5"), Some("10.0.0.5"));
        assert_eq!(url_host("http://user@node-1:4848"), Some("node-1"));
        assert_eq!(url_host("http://:4848"), None);
    }
}
