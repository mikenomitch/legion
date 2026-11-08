//! Per-token write rate limits: an in-memory token bucket per accessor (or
//! principal, for JWT callers) on each manager
//! (docs/approvals-and-rate-limits.md, "Rate limits").
//!
//! Limits are per manager: a caller spreading writes over three managers
//! gets up to three times the configured rate. The buckets live on the hot
//! write path, so they are a mutex over a map with arithmetic only; a token's
//! own limit (`acl_token_meta.write_rate`) is read from the store at most
//! once per [`TOKEN_LIMIT_CACHE`] per accessor.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::storage::store::Store;

/// How long a token's own limit is cached.
const TOKEN_LIMIT_CACHE: Duration = Duration::from_secs(30);

/// Buckets kept before idle ones are swept.
const MAX_BUCKETS: usize = 50_000;

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// The per-manager rate limiter.
#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, Bucket>>,
    token_limits: Mutex<HashMap<String, (Instant, Option<(f64, u32)>)>>,
}

/// Parses a `write_rate` spec: `<n>/s`, `<n>/s,burst=<m>`, `<n>/m`.
/// The burst defaults to five seconds' worth (at least 1).
pub fn parse_write_rate(raw: &str) -> Result<(f64, u32), String> {
    let raw = raw.trim();
    let (rate_part, burst_part) = match raw.split_once(',') {
        Some((rate, burst)) => (rate.trim(), Some(burst.trim())),
        None => (raw, None),
    };
    let (number, unit) = rate_part
        .split_once('/')
        .ok_or_else(|| format!("invalid write rate `{raw}`: expected `<n>/s` or `<n>/m`"))?;
    let number: f64 = number
        .trim()
        .parse()
        .map_err(|_| format!("invalid write rate `{raw}`: `{number}` is not a number"))?;
    let per_second = match unit.trim() {
        "s" | "sec" | "second" => number,
        "m" | "min" | "minute" => number / 60.0,
        other => {
            return Err(format!(
                "invalid write rate `{raw}`: unknown unit `{other}`"
            ))
        }
    };
    if per_second <= 0.0 || !per_second.is_finite() {
        return Err(format!(
            "invalid write rate `{raw}`: the rate must be positive"
        ));
    }
    let burst = match burst_part {
        Some(burst) => burst
            .strip_prefix("burst=")
            .and_then(|n| n.trim().parse::<u32>().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("invalid write rate `{raw}`: expected `burst=<n>` (n > 0)"))?,
        None => ((per_second * 5.0).ceil() as u32).max(1),
    };
    Ok((per_second, burst))
}

impl RateLimiter {
    /// Takes one token from `key`'s bucket, or returns how long until one is
    /// available.
    pub fn take(&self, key: &str, rate: f64, burst: u32, now: Instant) -> Result<(), Duration> {
        let burst = f64::from(burst.max(1));
        let mut buckets = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        if buckets.len() >= MAX_BUCKETS && !buckets.contains_key(key) {
            // Drop buckets that have refilled completely: forgetting them
            // changes nothing.
            buckets
                .retain(|_, b| b.tokens + now.duration_since(b.last).as_secs_f64() * rate < burst);
        }
        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return Ok(());
        }
        let missing = 1.0 - bucket.tokens;
        Err(Duration::from_secs_f64(
            missing / rate.max(f64::MIN_POSITIVE),
        ))
    }

    /// The limit stored on the token itself, if any (cached).
    pub fn token_limit(&self, store: &Store, accessor: &str) -> Option<(f64, u32)> {
        let now = Instant::now();
        {
            let cache = self.token_limits.lock().unwrap_or_else(|p| p.into_inner());
            if let Some((at, limit)) = cache.get(accessor) {
                if now.duration_since(*at) < TOKEN_LIMIT_CACHE {
                    return *limit;
                }
            }
        }
        let limit = store
            .get_token_meta(accessor)
            .ok()
            .flatten()
            .filter(|meta| meta.write_rate > 0.0)
            .map(|meta| (meta.write_rate, meta.write_burst.max(1) as u32));
        let mut cache = self.token_limits.lock().unwrap_or_else(|p| p.into_inner());
        if cache.len() > MAX_BUCKETS {
            cache.clear();
        }
        cache.insert(accessor.to_string(), (now, limit));
        limit
    }
}

#[cfg(test)]
mod rate_limit_test {
    use super::*;

    #[test]
    fn bucket_trips_and_recovers_test() {
        let limiter = RateLimiter::default();
        let start = Instant::now();
        for _ in 0..3 {
            limiter
                .take("a", 1.0, 3, start)
                .expect("burst admits three");
        }
        let wait = limiter
            .take("a", 1.0, 3, start)
            .expect_err("fourth is refused");
        assert!(wait <= Duration::from_secs(1) && wait > Duration::ZERO);
        // Another key has its own bucket.
        limiter.take("b", 1.0, 3, start).expect("separate bucket");
        // One second later one token has refilled.
        limiter
            .take("a", 1.0, 3, start + Duration::from_millis(1001))
            .expect("refilled");
        assert!(limiter
            .take("a", 1.0, 3, start + Duration::from_millis(1002))
            .is_err());
    }

    #[test]
    fn write_rate_specs_parse_test() {
        assert_eq!(parse_write_rate("5/s").unwrap(), (5.0, 25));
        assert_eq!(parse_write_rate("5/s,burst=20").unwrap(), (5.0, 20));
        assert_eq!(parse_write_rate("60/m").unwrap(), (1.0, 5));
        assert!(parse_write_rate("0/s").is_err());
        assert!(parse_write_rate("5").is_err());
        assert!(parse_write_rate("5/h").is_err());
        assert!(parse_write_rate("5/s,burst=0").is_err());
    }
}
