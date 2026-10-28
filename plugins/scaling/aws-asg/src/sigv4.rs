//! AWS Signature Version 4, by hand: the whole of it is SHA-256, HMAC, and
//! a canonical form of the request
//! (<https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html>).
//!
//! Written here rather than taken from the AWS SDK so the plugin builds
//! from crates Legion already uses, offline, in seconds.

use sha2::{Digest, Sha256};

/// AWS credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The access key id (`AKIA…`, `ASIA…` for temporary credentials).
    pub access_key_id: String,
    /// The secret access key.
    pub secret_access_key: String,
    /// The session token of temporary credentials.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    // Never print the secret or the session token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// SHA-256 of `data`, as lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(data);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// Percent-encoding as SigV4 and the Query API want it (RFC 3986): every
/// byte but `A-Z a-z 0-9 - _ . ~` is `%XX`, uppercase.
pub fn uri_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// A `key=value&…` form body (or query string), encoded, in the order given.
pub fn form(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", uri_encode(key), uri_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The key every signature of one day, region and service is made with.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// What one request is, for signing.
#[derive(Debug, Clone)]
pub struct Request<'a> {
    /// `GET`, `POST`.
    pub method: &'a str,
    /// The host the request goes to (and its `host` header).
    pub host: &'a str,
    /// The path, `/` for every API this plugin calls.
    pub path: &'a str,
    /// Query parameters, unencoded.
    pub query: &'a [(String, String)],
    /// Headers to sign besides `host` and `x-amz-date` (names in any case).
    pub headers: &'a [(String, String)],
    /// The body.
    pub payload: &'a [u8],
}

/// The canonical request (step 1 of SigV4).
pub fn canonical_request(request: &Request<'_>, amz_date: &str, session_token: Option<&str>) -> (String, String) {
    let mut query: Vec<(String, String)> = request
        .query
        .iter()
        .map(|(key, value)| (uri_encode(key), uri_encode(value)))
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), collapse(value)))
        .collect();
    headers.push(("host".to_string(), request.host.to_string()));
    headers.push(("x-amz-date".to_string(), amz_date.to_string()));
    if let Some(token) = session_token {
        headers.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    headers.sort();
    let block: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let path = if request.path.is_empty() {
        "/".to_string()
    } else {
        request
            .path
            .split('/')
            .map(uri_encode)
            .collect::<Vec<_>>()
            .join("/")
    };
    let canonical = format!(
        "{}\n{path}\n{query}\n{block}\n{signed}\n{}",
        request.method,
        sha256_hex(request.payload)
    );
    (canonical, signed)
}

/// Trim, and fold runs of spaces to one (the canonical header value).
fn collapse(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Sign `request`. Returns the headers to send with it besides `host`:
/// `x-amz-date`, `x-amz-security-token` for temporary credentials, and
/// `authorization`. `amz_date` is `YYYYMMDDTHHMMSSZ`.
pub fn sign(
    credentials: &Credentials,
    region: &str,
    service: &str,
    amz_date: &str,
    request: &Request<'_>,
) -> Vec<(String, String)> {
    let date = &amz_date[..8.min(amz_date.len())];
    let token = credentials.session_token.as_deref();
    let (canonical, signed) = canonical_request(request, amz_date, token);
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let key = signing_key(&credentials.secret_access_key, date, region, service);
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    let mut out = vec![("x-amz-date".to_string(), amz_date.to_string())];
    if let Some(token) = token {
        out.push(("x-amz-security-token".to_string(), token.to_string()));
    }
    out.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            credentials.access_key_id
        ),
    ));
    out
}

/// `YYYYMMDDTHHMMSSZ` for unix seconds.
pub fn amz_date(unix_seconds: i64) -> String {
    let (year, month, day, hour, minute, second) = civil(unix_seconds);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// (year, month, day, hour, minute, second) in UTC for unix seconds.
pub fn civil(unix_seconds: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = unix_seconds.div_euclid(86_400);
    let rest = unix_seconds.rem_euclid(86_400);
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (
        year,
        month,
        day,
        (rest / 3600) as u32,
        (rest % 3600 / 60) as u32,
        (rest % 60) as u32,
    )
}

/// Unix seconds for an ISO 8601 UTC time as AWS writes it
/// (`2015-08-30T12:36:00Z`, fractions allowed).
pub fn parse_iso8601(text: &str) -> Option<i64> {
    let text = text.trim();
    let bytes = text.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days from civil (the inverse of `civil`).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hour * 3600 + minute * 60 + second)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> Credentials {
        // The credentials of AWS's published SigV4 examples.
        Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        }
    }

    /// AWS's "deriving the signing key" example.
    #[test]
    fn the_signing_key_matches_the_published_example_test() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex(&key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    /// The `get-vanilla` case of AWS's SigV4 test suite.
    #[test]
    fn get_vanilla_matches_the_test_suite_test() {
        let headers = sign(
            &example(),
            "us-east-1",
            "service",
            "20150830T123600Z",
            &Request {
                method: "GET",
                host: "example.amazonaws.com",
                path: "/",
                query: &[],
                headers: &[],
                payload: b"",
            },
        );
        let authorization = &headers.iter().find(|(name, _)| name == "authorization").unwrap().1;
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// The IAM `ListUsers` example of the SigV4 documentation: its
    /// canonical request hash and its signature.
    #[test]
    fn the_iam_example_matches_the_documentation_test() {
        let query = [
            ("Action".to_string(), "ListUsers".to_string()),
            ("Version".to_string(), "2010-05-08".to_string()),
        ];
        let content_type = [(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded; charset=utf-8".to_string(),
        )];
        let request = Request {
            method: "GET",
            host: "iam.amazonaws.com",
            path: "/",
            query: &query,
            headers: &content_type,
            payload: b"",
        };
        let (canonical, signed) = canonical_request(&request, "20150830T123600Z", None);
        assert_eq!(signed, "content-type;host;x-amz-date");
        assert_eq!(
            sha256_hex(canonical.as_bytes()),
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
        );
        let headers = sign(&example(), "us-east-1", "iam", "20150830T123600Z", &request);
        assert!(headers[1]
            .1
            .ends_with("Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"));
    }

    /// Temporary credentials sign their session token too.
    #[test]
    fn a_session_token_is_signed_and_sent_test() {
        let mut credentials = example();
        credentials.session_token = Some("FQoGZXIvYXdz".to_string());
        let headers = sign(
            &credentials,
            "eu-west-1",
            "autoscaling",
            "20261028T120000Z",
            &Request {
                method: "POST",
                host: "autoscaling.eu-west-1.amazonaws.com",
                path: "/",
                query: &[],
                headers: &[],
                payload: b"Action=DescribeAutoScalingGroups",
            },
        );
        assert_eq!(headers[1], ("x-amz-security-token".to_string(), "FQoGZXIvYXdz".to_string()));
        assert!(headers[2].1.contains("SignedHeaders=host;x-amz-date;x-amz-security-token"));
        assert!(!format!("{credentials:?}").contains("wJalr"));
        assert!(!format!("{credentials:?}").contains("FQoG"));
    }

    #[test]
    fn encoding_and_dates_test() {
        assert_eq!(uri_encode("a b/c~d=é"), "a%20b%2Fc~d%3D%C3%A9");
        assert_eq!(
            form(&[("Action".into(), "X".into()), ("Name".into(), "a&b".into())]),
            "Action=X&Name=a%26b"
        );
        assert_eq!(amz_date(1_440_938_160), "20150830T123600Z");
        assert_eq!(parse_iso8601("2015-08-30T12:36:00Z"), Some(1_440_938_160));
        assert_eq!(parse_iso8601("2015-08-30T12:36:00.000Z"), Some(1_440_938_160));
        assert_eq!(parse_iso8601("2024-02-29T00:00:00Z"), Some(1_709_164_800));
        assert_eq!(amz_date(1_709_164_800), "20240229T000000Z");
        assert_eq!(parse_iso8601("yesterday"), None);
    }
}
