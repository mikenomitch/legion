//! Host-initiated vsock: how the node datapath opens a connection *into* a
//! gated sandbox guest (docs/network-handlers.md, "Ingress into a sandbox").
//!
//! Firecracker and cloud-hypervisor implement a guest's virtio-vsock device
//! as Unix sockets on the host. A guest's connection to host port P arrives
//! on `<base>_P`, which is where the egress proxy and resolver listen. The
//! other direction goes through the base socket itself, and both VMMs speak
//! the same small handshake on it:
//!
//! 1. the host connects to `<base>` and writes `CONNECT <guest port>\n`;
//! 2. the VMM forwards the connection to the guest's listener on that port
//!    and, once the guest accepts, answers `OK <host port>\n`, where the
//!    host port is a number the VMM assigned to the host's end (Firecracker
//!    counts them up from 2^30);
//! 3. from then on the stream is the connection.
//!
//! Anything else is a failed dial: the VMM closing the stream (nothing
//! listens on that guest port), a line that is not `OK <n>`, or no answer
//! within [`DIAL_TIMEOUT`] (a paused guest).

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;

/// How long one dial may take, the connect and the handshake together.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest answer line accepted (`OK 4294967295` is 13 bytes).
const MAX_ANSWER: usize = 64;

/// The host-initiated handshake on a stream already connected to the VMM's
/// base socket: ask for guest port `port`, and return the host port the VMM
/// assigned once the guest accepted. On success the stream carries the
/// connection, and nothing the guest sent after the answer has been read.
pub async fn handshake<S>(stream: &mut S, port: u32) -> io::Result<u32>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(format!("CONNECT {port}\n").as_bytes())
        .await?;
    stream.flush().await?;
    // One byte at a time: a guest may speak first (a banner, a TLS server
    // never does but SSH and SMTP do), and those bytes belong to the
    // connection, not to the handshake. The answer is a dozen bytes.
    let mut line = Vec::with_capacity(16);
    loop {
        let mut byte = [0u8; 1];
        if stream.read(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!(
                    "the VMM closed the connection instead of answering CONNECT {port}: \
                     nothing in the guest listens on vsock port {port}"
                ),
            ));
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > MAX_ANSWER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the VMM's answer to CONNECT {port} is longer than {MAX_ANSWER} bytes"),
            ));
        }
    }
    parse_answer(&line, port)
}

/// `OK <n>` (a trailing `\r` tolerated), or the line quoted in the error.
fn parse_answer(line: &[u8], port: u32) -> io::Result<u32> {
    std::str::from_utf8(line)
        .ok()
        .map(|text| text.trim_end_matches('\r'))
        .and_then(|text| text.strip_prefix("OK "))
        .and_then(|number| number.trim().parse::<u32>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the VMM answered CONNECT {port} with {:?}, not OK <port>",
                    String::from_utf8_lossy(line)
                ),
            )
        })
}

/// Open a connection to guest vsock port `port` through the VMM's base
/// socket `base` ([`handshake`]), within [`DIAL_TIMEOUT`].
pub async fn dial(base: &Path, port: u32) -> io::Result<UnixStream> {
    dial_within(base, port, DIAL_TIMEOUT).await
}

async fn dial_within(base: &Path, port: u32, limit: Duration) -> io::Result<UnixStream> {
    let attempt = async {
        let mut stream = UnixStream::connect(base).await.map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "connect to the VMM's vsock socket {}: {err}",
                    base.display()
                ),
            )
        })?;
        handshake(&mut stream, port).await?;
        Ok(stream)
    };
    match tokio::time::timeout(limit, attempt).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "the VMM did not answer CONNECT {port} on {} within {}ms",
                base.display(),
                limit.as_millis()
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Read one `\n`-terminated line, byte by byte.
    async fn read_line(stream: &mut UnixStream) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while stream.read(&mut byte).await.unwrap_or(0) == 1 && byte[0] != b'\n' {
            line.push(byte[0]);
        }
        String::from_utf8_lossy(&line).to_string()
    }

    /// A fake VMM on `<dir>/vsock.sock`: for each connection, reads the
    /// CONNECT line, then does what `answer` says with it.
    fn fake_vmm(dir: &Path, answer: &'static str) -> std::path::PathBuf {
        let base = dir.join("vsock.sock");
        let listener = UnixListener::bind(&base).unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let request = read_line(&mut stream).await;
                    match answer {
                        // Firecracker's answer, with the guest speaking
                        // first in the same write, then an echo of what the
                        // host sends, prefixed with what it asked for.
                        "ok" => {
                            stream.write_all(b"OK 1073741824\nbanner\n").await.unwrap();
                            let line = read_line(&mut stream).await;
                            let _ = stream
                                .write_all(format!("{request}: {line}\n").as_bytes())
                                .await;
                        }
                        "close" => drop(stream),
                        "garbage" => {
                            let _ = stream.write_all(b"ERR no such port\n").await;
                        }
                        // Never answers: a paused guest.
                        _ => {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            drop(stream);
                        }
                    }
                });
            }
        });
        base
    }

    /// The handshake both VMMs implement: `CONNECT <port>\n` out, `OK <n>\n`
    /// back, and the bytes after the answer are the guest's, untouched.
    #[tokio::test]
    async fn handshake_returns_the_vmm_host_port_and_leaves_the_stream_intact_test() {
        let dir = tempfile::tempdir().unwrap();
        let base = fake_vmm(dir.path(), "ok");
        let mut stream = UnixStream::connect(&base).await.unwrap();
        assert_eq!(handshake(&mut stream, 20001).await.unwrap(), 1 << 30);
        assert_eq!(read_line(&mut stream).await, "banner");
        stream.write_all(b"hello\n").await.unwrap();
        assert_eq!(read_line(&mut stream).await, "CONNECT 20001: hello");

        // `dial` is the same, from the base path.
        let mut stream = dial(&base, 8080).await.unwrap();
        assert_eq!(read_line(&mut stream).await, "banner");
        stream.write_all(b"again\n").await.unwrap();
        assert_eq!(read_line(&mut stream).await, "CONNECT 8080: again");
    }

    /// Everything that is not `OK <n>` is a failed dial, named.
    #[tokio::test]
    async fn anything_but_ok_is_a_failed_dial_test() {
        let dir = tempfile::tempdir().unwrap();

        let closed = fake_vmm(&subdir(dir.path(), "closed"), "close");
        let err = dial(&closed, 20001).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        assert!(err.to_string().contains("vsock port 20001"), "{err}");

        let garbage = fake_vmm(&subdir(dir.path(), "garbage"), "garbage");
        let err = dial(&garbage, 20001).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("ERR no such port"), "{err}");

        let silent = fake_vmm(&subdir(dir.path(), "silent"), "silent");
        let err = dial_within(&silent, 20001, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        // No VMM at all (not booted yet, or already gone).
        let err = dial(&dir.path().join("nothing.sock"), 20001)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nothing.sock"), "{err}");

        assert_eq!(parse_answer(b"OK 7\r", 1).unwrap(), 7);
        assert!(parse_answer(b"OK", 1).is_err());
        assert!(parse_answer(b"OK x", 1).is_err());
    }

    /// `dir/name`, created.
    fn subdir(dir: &Path, name: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
