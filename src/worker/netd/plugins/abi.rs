//! The proxy-wasm ABI (0.2.0 and 0.2.1) as the host sees it: the enums the
//! module passes as integers, and the two wire encodings the host and the
//! module exchange through linear memory (header maps and property paths).
//! Values follow the spec at <https://github.com/proxy-wasm/spec> and the
//! Rust SDK's `types.rs`.

/// `Status` returned by every host call.
pub mod status {
    pub const OK: i32 = 0;
    pub const NOT_FOUND: i32 = 1;
    pub const BAD_ARGUMENT: i32 = 2;
    pub const PARSE_FAILURE: i32 = 4;
    pub const EMPTY: i32 = 7;
    pub const CAS_MISMATCH: i32 = 8;
    pub const INTERNAL_FAILURE: i32 = 10;
    pub const UNIMPLEMENTED: i32 = 12;
}

/// `Action` a stream callback returns.
pub const ACTION_CONTINUE: i32 = 0;
pub const ACTION_PAUSE: i32 = 1;

/// `BufferType`.
pub mod buffer {
    pub const HTTP_REQUEST_BODY: i32 = 0;
    pub const HTTP_RESPONSE_BODY: i32 = 1;
    pub const DOWNSTREAM_DATA: i32 = 2;
    pub const UPSTREAM_DATA: i32 = 3;
    pub const HTTP_CALL_RESPONSE_BODY: i32 = 4;
    pub const VM_CONFIGURATION: i32 = 6;
    pub const PLUGIN_CONFIGURATION: i32 = 7;
    pub const CALL_DATA: i32 = 8;
}

/// `MapType`.
pub mod map {
    pub const HTTP_REQUEST_HEADERS: i32 = 0;
    pub const HTTP_REQUEST_TRAILERS: i32 = 1;
    pub const HTTP_RESPONSE_HEADERS: i32 = 2;
    pub const HTTP_RESPONSE_TRAILERS: i32 = 3;
    pub const HTTP_CALL_RESPONSE_HEADERS: i32 = 6;
    pub const HTTP_CALL_RESPONSE_TRAILERS: i32 = 7;
}

/// `StreamType`.
pub mod stream {
    pub const HTTP_REQUEST: i32 = 0;
    pub const HTTP_RESPONSE: i32 = 1;
    pub const DOWNSTREAM: i32 = 2;
    pub const UPSTREAM: i32 = 3;
}

/// `MetricType`.
pub mod metric {
    pub const COUNTER: i32 = 0;
    pub const GAUGE: i32 = 1;
    pub const HISTOGRAM: i32 = 2;
}

/// `LogLevel`, as `tracing` sees it.
pub fn log_level(level: i32) -> tracing::Level {
    match level {
        0 => tracing::Level::TRACE,
        1 => tracing::Level::DEBUG,
        2 => tracing::Level::INFO,
        3 => tracing::Level::WARN,
        _ => tracing::Level::ERROR,
    }
}

pub fn log_level_name(level: i32) -> &'static str {
    match level {
        0 => "trace",
        1 => "debug",
        2 => "info",
        3 => "warn",
        4 => "error",
        _ => "critical",
    }
}

/// A header map as pairs of bytes, in order. Names are lowercase for HTTP
/// headers; pseudo-headers (`:method`, `:path`, `:authority`, `:scheme`,
/// `:status`) come first.
pub type Pairs = Vec<(String, Vec<u8>)>;

/// Encode a header map: `u32` count, then `u32` name and value lengths per
/// pair, then each name and value with a NUL after it. Little-endian.
pub fn encode_pairs(pairs: &Pairs) -> Vec<u8> {
    let mut size = 4;
    for (name, value) in pairs {
        size += 8 + name.len() + 1 + value.len() + 1;
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    for (name, value) in pairs {
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    }
    for (name, value) in pairs {
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        out.extend_from_slice(value);
        out.push(0);
    }
    out
}

/// Decode a header map; `None` when the bytes are not one.
pub fn decode_pairs(bytes: &[u8]) -> Option<Pairs> {
    let count = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    // Every pair costs at least ten bytes, so a count beyond that is a lie.
    if count > bytes.len() / 10 + 1 {
        return None;
    }
    let mut lengths = Vec::with_capacity(count);
    let mut at = 4;
    for _ in 0..count {
        let name = u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?) as usize;
        let value = u32::from_le_bytes(bytes.get(at + 4..at + 8)?.try_into().ok()?) as usize;
        lengths.push((name, value));
        at += 8;
    }
    let mut pairs = Vec::with_capacity(count);
    for (name_len, value_len) in lengths {
        let name = bytes.get(at..at + name_len)?;
        at += name_len + 1;
        let value = bytes.get(at..at + value_len)?;
        at += value_len + 1;
        if at > bytes.len() {
            return None;
        }
        pairs.push((
            String::from_utf8_lossy(name).to_ascii_lowercase(),
            value.to_vec(),
        ));
    }
    Some(pairs)
}

/// A property path's segments (`legion\0labels\0tier`).
pub fn decode_path(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|b| *b == 0)
        .map(|segment| String::from_utf8_lossy(segment).into_owned())
        .collect()
}

#[cfg(test)]
mod abi_test {
    use super::*;

    #[test]
    fn header_maps_round_trip_test() {
        let pairs: Pairs = vec![
            (":method".into(), b"GET".to_vec()),
            ("x-a".into(), b"".to_vec()),
            ("x-b".into(), b"two words".to_vec()),
        ];
        let encoded = encode_pairs(&pairs);
        assert_eq!(decode_pairs(&encoded), Some(pairs));
        assert_eq!(decode_pairs(&encode_pairs(&Vec::new())), Some(Vec::new()));
        assert_eq!(decode_pairs(&encoded[..encoded.len() - 3]), None);
        assert_eq!(decode_pairs(&[0xff, 0xff, 0xff, 0x7f]), None);
    }

    #[test]
    fn property_paths_split_on_nul_test() {
        assert_eq!(
            decode_path(b"legion\0labels\0tier"),
            vec!["legion", "labels", "tier"]
        );
    }
}
