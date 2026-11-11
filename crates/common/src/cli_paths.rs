//! Where the CLI keeps its token for a manager address. Shared with the
//! manager, whose first start writes the initial management token to the
//! path a CLI on the same machine will read
//! (`manager::bootstrap_token`).

/// The socket path of a `unix://` manager address (`unix:///path/legion.sock`
/// or `unix:relative.sock`), or `None` for a network address.
pub fn unix_socket_path(address: &str) -> Option<std::path::PathBuf> {
    let rest = address
        .strip_prefix("unix://")
        .or_else(|| address.strip_prefix("unix:"))?;
    (!rest.is_empty()).then(|| std::path::PathBuf::from(rest))
}

/// A manager address as a token-file name: `host_port`, so
/// `http://127.0.0.1:4747` is `127.0.0.1_4747`. The scheme is dropped (http
/// and https on one port are one manager), a missing port is the scheme's
/// default, and anything outside `[A-Za-z0-9.-]` becomes `_`, so an IPv6
/// address or a typo can never name a path outside the tokens directory.
pub fn token_file_key(address: &str) -> String {
    if let Some(path) = unix_socket_path(address) {
        // A socket has no host and port; its path names it. `unix_` keeps
        // it apart from every network address's key.
        return format!("unix_{}", path.display())
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
    }
    let host_port = match address.parse::<http::Uri>() {
        Ok(uri) if uri.host().is_some() => {
            let host = uri.host().unwrap_or_default().trim_matches(['[', ']']);
            let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
                Some("https" | "grpcs") => 443,
                _ => 80,
            });
            format!("{host}_{port}")
        }
        _ => address.to_string(),
    };
    host_port
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Path of the stored CLI token for the manager at `address`: the file
/// `legion login` writes, and the one a manager's first start writes the
/// initial management token to.
///
/// Tokens are kept per manager address (`~/.legion/tokens/<host>_<port>`) so
/// two clusters on one machine never present each other's token.
/// `LEGION_TOKEN_FILE` names one file for every address instead.
pub fn cli_token_path(address: &str) -> std::path::PathBuf {
    if let Ok(path) = std::env::var("LEGION_TOKEN_FILE") {
        return std::path::PathBuf::from(path);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home)
        .join(".legion")
        .join("tokens")
        .join(token_file_key(address))
}
