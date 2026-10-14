//! PROXY protocol v2 with Legion TLVs — how the datapath names a flow to
//! something else that owns bytes (docs/network-handlers.md, "Handlers"
//! and "mTLS").
//!
//! Two consumers:
//!
//! - a `proxy` **handler**: `netd` connects to the external proxy, writes
//!   one header, then splices. The proxy learns who is sending (the workload
//!   identity, the allocation, its labels) and where the flow was going
//!   (the original destination) without parsing a byte of payload.
//! - **node-to-node mesh TLS**: the dialing node writes the same header
//!   inside the TLS session so the receiving node can attribute the
//!   connection to a *workload*, having verified that the peer is a *node*.
//!
//! Only TCP/IPv4 and TCP/IPv6 are ever emitted; a hostname destination that
//! has not been resolved rides in the `DESTINATION` TLV with an unspecified
//! address in the fixed part. The wire format is the HAProxy spec's, so any
//! PROXY-aware proxy (HAProxy, Envoy, nginx, Caddy) reads the addresses and
//! ignores the TLVs it does not know.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The v2 signature.
pub const SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// Custom TLV types (`PP2_TYPE_MIN_CUSTOM` upward).
pub mod tlv {
    /// The workload identity the flow belongs to (`<region>:<job>:<group>:<task>`).
    pub const IDENTITY: u8 = 0xE0;
    /// The original destination as `host:port` (egress) or the task's private
    /// port (ingress hand-off).
    pub const DESTINATION: u8 = 0xE1;
    /// The allocation id, decimal.
    pub const ALLOCATION: u8 = 0xE2;
    /// The task's `network.labels`, `k=v` joined by `;`.
    pub const LABELS: u8 = 0xE3;
    /// `egress` or `ingress`.
    pub const DIRECTION: u8 = 0xE4;
    /// The job id.
    pub const JOB: u8 = 0xE5;
    /// The task name.
    pub const TASK: u8 = 0xE6;
}

/// Largest header this side will read (the fixed part plus TLVs).
const MAX_HEADER: usize = 16 + 4096;

/// A parsed header.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProxyHeader {
    pub source: Option<SocketAddr>,
    pub destination: Option<SocketAddr>,
    /// TLVs in wire order, values as bytes.
    pub tlvs: Vec<(u8, Vec<u8>)>,
}

impl ProxyHeader {
    /// The first TLV of `kind`, as a string.
    pub fn text(&self, kind: u8) -> Option<String> {
        self.tlvs
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, v)| String::from_utf8_lossy(v).to_string())
    }
}

fn unspecified_like(other: Option<SocketAddr>) -> SocketAddr {
    match other {
        Some(SocketAddr::V6(_)) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        _ => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
    }
}

/// Encode one header. A missing side is written as the unspecified address
/// of the other side's family (both missing: IPv4 zeros).
pub fn encode(
    source: Option<SocketAddr>,
    destination: Option<SocketAddr>,
    tlvs: &[(u8, &[u8])],
) -> Vec<u8> {
    let src = source.unwrap_or_else(|| unspecified_like(destination));
    let mut dst = destination.unwrap_or_else(|| unspecified_like(source));
    // Both addresses must share a family; map a mixed pair onto v6.
    let (src, family) = match (src, dst) {
        (SocketAddr::V4(_), SocketAddr::V4(_)) => (src, 0x11u8),
        (SocketAddr::V6(_), SocketAddr::V6(_)) => (src, 0x21u8),
        (s, d) => {
            let to6 = |a: SocketAddr| match a {
                SocketAddr::V4(v4) => {
                    SocketAddr::new(IpAddr::V6(v4.ip().to_ipv6_mapped()), v4.port())
                }
                v6 => v6,
            };
            dst = to6(d);
            (to6(s), 0x21u8)
        }
    };
    let mut body = Vec::with_capacity(64);
    match (src, dst) {
        (SocketAddr::V4(s), SocketAddr::V4(d)) => {
            body.extend_from_slice(&s.ip().octets());
            body.extend_from_slice(&d.ip().octets());
            body.extend_from_slice(&s.port().to_be_bytes());
            body.extend_from_slice(&d.port().to_be_bytes());
        }
        (SocketAddr::V6(s), SocketAddr::V6(d)) => {
            body.extend_from_slice(&s.ip().octets());
            body.extend_from_slice(&d.ip().octets());
            body.extend_from_slice(&s.port().to_be_bytes());
            body.extend_from_slice(&d.port().to_be_bytes());
        }
        _ => unreachable!("families were unified above"),
    }
    for (kind, value) in tlvs {
        let value = &value[..value.len().min(u16::MAX as usize)];
        body.push(*kind);
        body.extend_from_slice(&(value.len() as u16).to_be_bytes());
        body.extend_from_slice(value);
    }
    let mut out = Vec::with_capacity(16 + body.len());
    out.extend_from_slice(&SIGNATURE);
    out.push(0x21); // version 2, command PROXY
    out.push(family);
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// What `parse` found.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// A complete header of this many bytes.
    Header(ProxyHeader, usize),
    /// The bytes so far are a prefix of a header; read more.
    Incomplete,
    /// The bytes do not start a PROXY v2 header.
    NotProxy,
}

/// Parse a header from the start of `buf`.
pub fn parse(buf: &[u8]) -> Parsed {
    let probe = buf.len().min(SIGNATURE.len());
    if buf[..probe] != SIGNATURE[..probe] {
        return Parsed::NotProxy;
    }
    if buf.len() < 16 {
        return Parsed::Incomplete;
    }
    if buf[12] >> 4 != 2 {
        return Parsed::NotProxy;
    }
    let command = buf[12] & 0x0F;
    let family = buf[13];
    let len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    let total = 16 + len;
    if total > MAX_HEADER {
        return Parsed::NotProxy;
    }
    if buf.len() < total {
        return Parsed::Incomplete;
    }
    let mut header = ProxyHeader::default();
    let mut rest = &buf[16..total];
    // LOCAL (0) carries no addresses; PROXY (1) does.
    if command == 1 {
        match family {
            0x11 | 0x12 if rest.len() >= 12 => {
                let s = Ipv4Addr::new(rest[0], rest[1], rest[2], rest[3]);
                let d = Ipv4Addr::new(rest[4], rest[5], rest[6], rest[7]);
                let sp = u16::from_be_bytes([rest[8], rest[9]]);
                let dp = u16::from_be_bytes([rest[10], rest[11]]);
                header.source = Some(SocketAddr::new(IpAddr::V4(s), sp));
                header.destination = Some(SocketAddr::new(IpAddr::V4(d), dp));
                rest = &rest[12..];
            }
            0x21 | 0x22 if rest.len() >= 36 => {
                let mut s = [0u8; 16];
                let mut d = [0u8; 16];
                s.copy_from_slice(&rest[..16]);
                d.copy_from_slice(&rest[16..32]);
                let sp = u16::from_be_bytes([rest[32], rest[33]]);
                let dp = u16::from_be_bytes([rest[34], rest[35]]);
                header.source = Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(s)), sp));
                header.destination = Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(d)), dp));
                rest = &rest[36..];
            }
            0x00 => {}
            _ => return Parsed::NotProxy,
        }
    }
    while rest.len() >= 3 {
        let kind = rest[0];
        let len = u16::from_be_bytes([rest[1], rest[2]]) as usize;
        if rest.len() < 3 + len {
            break;
        }
        header.tlvs.push((kind, rest[3..3 + len].to_vec()));
        rest = &rest[3 + len..];
    }
    Parsed::Header(header, total)
}

/// Read a header off the start of a stream. Returns the header (if the
/// stream started with one) and any bytes read past it, which the caller
/// must feed to whatever consumes the stream next.
pub async fn read_header<S>(stream: &mut S) -> std::io::Result<(Option<ProxyHeader>, Vec<u8>)>
where
    S: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 512];
    loop {
        match parse(&buf) {
            Parsed::Header(header, used) => {
                let leftover = buf[used..].to_vec();
                return Ok((Some(header), leftover));
            }
            Parsed::NotProxy => return Ok((None, buf)),
            Parsed::Incomplete => {}
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok((None, buf));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// A stream with bytes already read off it put back in front — what a
/// header parser hands to whatever consumes the connection next.
pub struct Prefixed<S> {
    prefix: Vec<u8>,
    pos: usize,
    inner: S,
}

impl<S> Prefixed<S> {
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        Prefixed {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = buf.remaining().min(self.prefix.len() - self.pos);
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

/// Splices only once the prefix has been read: until then some bytes live
/// here, not in the socket.
impl<S: crate::worker::netd::splice::SpliceStream> crate::worker::netd::splice::SpliceStream
    for Prefixed<S>
{
    fn socket(&self) -> Option<crate::worker::netd::splice::Socket<'_>> {
        if self.pos < self.prefix.len() {
            return None;
        }
        self.inner.socket()
    }

    fn spliced(&self, bytes: usize) {
        self.inner.spliced(bytes);
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prefixed_streams_serve_the_prefix_first() {
        use tokio::io::AsyncReadExt;
        let inner = std::io::Cursor::new(b" world".to_vec());
        let mut stream = Prefixed::new(b"hello".to_vec(), inner);
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "hello world");
    }

    #[test]
    fn v4_header_round_trips_with_tlvs() {
        let src: SocketAddr = "10.1.2.3:41234".parse().unwrap();
        let dst: SocketAddr = "93.184.216.34:443".parse().unwrap();
        let bytes = encode(
            Some(src),
            Some(dst),
            &[
                (tlv::IDENTITY, b"auto:web:default:server"),
                (tlv::DESTINATION, b"example.com:443"),
            ],
        );
        assert_eq!(&bytes[..12], &SIGNATURE);
        match parse(&bytes) {
            Parsed::Header(header, used) => {
                assert_eq!(used, bytes.len());
                assert_eq!(header.source, Some(src));
                assert_eq!(header.destination, Some(dst));
                assert_eq!(
                    header.text(tlv::IDENTITY).as_deref(),
                    Some("auto:web:default:server")
                );
                assert_eq!(
                    header.text(tlv::DESTINATION).as_deref(),
                    Some("example.com:443")
                );
                assert_eq!(header.text(tlv::LABELS), None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn mixed_families_are_promoted_to_v6() {
        let src: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let dst: SocketAddr = "[2001:db8::1]:2".parse().unwrap();
        let bytes = encode(Some(src), Some(dst), &[]);
        assert_eq!(bytes[13], 0x21);
        match parse(&bytes) {
            Parsed::Header(header, _) => {
                assert_eq!(header.destination, Some(dst));
                assert_eq!(
                    header.source.map(|s| s.ip()),
                    Some(IpAddr::V6(Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped()))
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn partial_and_foreign_bytes_are_classified() {
        let full = encode(None, None, &[(tlv::DIRECTION, b"egress")]);
        assert_eq!(parse(&full[..5]), Parsed::Incomplete);
        assert_eq!(parse(&full[..20]), Parsed::Incomplete);
        assert_eq!(parse(b"GET / HTTP/1.1\r\n"), Parsed::NotProxy);
        assert_eq!(parse(b""), Parsed::Incomplete);
    }

    #[tokio::test]
    async fn read_header_returns_the_bytes_after_it() {
        let mut bytes = encode(None, None, &[(tlv::ALLOCATION, b"7")]);
        bytes.extend_from_slice(b"hello");
        let mut cursor = std::io::Cursor::new(bytes);
        let (header, leftover) = read_header(&mut cursor).await.unwrap();
        assert_eq!(header.unwrap().text(tlv::ALLOCATION).as_deref(), Some("7"));
        assert_eq!(leftover, b"hello");

        let mut plain = std::io::Cursor::new(b"GET / HTTP/1.1\r\n\r\n".to_vec());
        let (header, leftover) = read_header(&mut plain).await.unwrap();
        assert!(header.is_none());
        assert_eq!(leftover, b"GET / HTTP/1.1\r\n\r\n");
    }
}
