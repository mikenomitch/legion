//! Host TAP device control without a subprocess.
//!
//! The sandbox runtime creates, raises, shuts, enslaves, and deletes TAP
//! devices on its start and restore paths. Each of those used to fork and
//! exec `ip`, which costs a process spawn (and a copy of the worker's page
//! tables) per call on a node starting hundreds of guests a second. These are
//! the same kernel calls `ip` makes:
//!
//! - create: `TUNSETIFF` then `TUNSETPERSIST` on `/dev/net/tun`
//!   (`ip tuntap add dev <d> mode tap`);
//! - up / down: `SIOCGIFFLAGS` and `SIOCSIFFLAGS` on a datagram socket
//!   (`ip link set dev <d> up|down`);
//! - master: an `RTM_NEWLINK` carrying `IFLA_MASTER` over rtnetlink
//!   (`ip link set dev <d> master <m>`), so any master kind works, not only a
//!   Linux bridge;
//! - delete: an `RTM_DELLINK` over rtnetlink (`ip link delete <d>`).
//!
//! Every error names the operation as the `ip` command an operator would run
//! by hand, so the failure text still points at the right manual check.

#[cfg(target_os = "linux")]
pub(crate) use linux::{create_persistent_tap, delete_link, set_link_up, set_master};

#[cfg(target_os = "linux")]
mod linux {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use anyhow::{bail, Context};

    type R<T> = anyhow::Result<T>;

    /// `_IOW('T', 202, int)`: attach to (or create) a TUN/TAP device.
    const TUNSETIFF: u64 = 0x4004_54ca;
    /// `_IOW('T', 203, int)`: keep the device after its last holder closes.
    const TUNSETPERSIST: u64 = 0x4004_54cb;

    /// `struct ifreq` as the TUN and flag ioctls read it: the name, then a
    /// `short` at the start of the 24-byte union.
    #[repr(C)]
    struct IfreqFlags {
        name: [libc::c_char; libc::IFNAMSIZ],
        flags: libc::c_short,
        _pad: [u8; 22],
    }

    fn ifreq(device: &str) -> R<IfreqFlags> {
        if device.is_empty() || device.len() >= libc::IFNAMSIZ || device.contains('\0') {
            bail!("network interface name {device:?} is not valid for the kernel");
        }
        let mut request = IfreqFlags {
            name: [0; libc::IFNAMSIZ],
            flags: 0,
            _pad: [0; 22],
        };
        for (slot, byte) in request.name.iter_mut().zip(device.bytes()) {
            *slot = byte as libc::c_char;
        }
        Ok(request)
    }

    fn os_error(what: String) -> anyhow::Error {
        anyhow::Error::new(std::io::Error::last_os_error()).context(what)
    }

    /// `ip tuntap add dev <device> mode tap`: a persistent, single-queue TAP
    /// with no packet-info header, which is what both VMMs attach to.
    pub(crate) fn create_persistent_tap(device: &str) -> R<()> {
        let what = || format!("`ip tuntap add dev {device} mode tap` failed");
        let mut request = ifreq(device).with_context(what)?;
        request.flags = (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short;
        let tun = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .context("failed to open /dev/net/tun")
            .with_context(what)?;
        // SAFETY: `request` is a valid, initialized `struct ifreq` prefix the
        // kernel reads and writes in place, and `tun` stays open for the call.
        if unsafe { libc::ioctl(tun.as_raw_fd(), TUNSETIFF as _, &mut request) } != 0 {
            return Err(os_error(what()));
        }
        // SAFETY: TUNSETPERSIST takes its argument by value.
        if unsafe { libc::ioctl(tun.as_raw_fd(), TUNSETPERSIST as _, 1 as libc::c_ulong) } != 0 {
            return Err(os_error(what()));
        }
        Ok(())
    }

    fn datagram_socket() -> R<OwnedFd> {
        // SAFETY: a plain socket(2) call; the fd is owned below.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(os_error(
                "failed to open a socket for interface ioctls".into(),
            ));
        }
        // SAFETY: `fd` is a fresh descriptor nothing else owns.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// `ip link set dev <device> up|down`.
    pub(crate) fn set_link_up(device: &str, up: bool) -> R<()> {
        let state = if up { "up" } else { "down" };
        let what = || format!("`ip link set dev {device} {state}` failed");
        let mut request = ifreq(device).with_context(what)?;
        let socket = datagram_socket().with_context(what)?;
        // SAFETY: `request` is a correctly laid out ifreq for both ioctls.
        if unsafe { libc::ioctl(socket.as_raw_fd(), libc::SIOCGIFFLAGS, &mut request) } != 0 {
            return Err(os_error(what()));
        }
        if up {
            request.flags |= libc::IFF_UP as libc::c_short;
        } else {
            request.flags &= !(libc::IFF_UP as libc::c_short);
        }
        if unsafe { libc::ioctl(socket.as_raw_fd(), libc::SIOCSIFFLAGS, &request) } != 0 {
            return Err(os_error(what()));
        }
        Ok(())
    }

    fn index_of(device: &str) -> R<u32> {
        let name = std::ffi::CString::new(device)?;
        // SAFETY: `name` is a valid NUL-terminated string for the call.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if index == 0 {
            return Err(os_error(format!("no network interface named {device}")));
        }
        Ok(index)
    }

    /// `ip link set dev <device> master <master>`.
    pub(crate) fn set_master(device: &str, master: &str) -> R<()> {
        let what = || format!("`ip link set dev {device} master {master}` failed");
        let index = index_of(device).with_context(what)?;
        let master_index = index_of(master).with_context(what)?;
        let mut attr = Vec::with_capacity(8);
        attr.extend_from_slice(&8u16.to_ne_bytes());
        attr.extend_from_slice(&libc::IFLA_MASTER.to_ne_bytes());
        attr.extend_from_slice(&master_index.to_ne_bytes());
        rtnetlink_link_request(libc::RTM_NEWLINK, index, &attr).with_context(what)
    }

    /// `ip link delete <device>`.
    pub(crate) fn delete_link(device: &str) -> R<()> {
        let what = || format!("`ip link delete {device}` failed");
        let index = index_of(device).with_context(what)?;
        rtnetlink_link_request(libc::RTM_DELLINK, index, &[]).with_context(what)
    }

    /// Send one link request (an `ifinfomsg` for `index`, then `attrs`) on a
    /// fresh rtnetlink socket and wait for the kernel's acknowledgement.
    fn rtnetlink_link_request(kind: u16, index: u32, attrs: &[u8]) -> R<()> {
        const HEADER: usize = 16; // struct nlmsghdr
        const IFINFO: usize = 16; // struct ifinfomsg
                                  // SAFETY: a plain socket(2) call; the fd is owned below.
        let fd = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                libc::NETLINK_ROUTE,
            )
        };
        if fd < 0 {
            return Err(os_error("failed to open an rtnetlink socket".into()));
        }
        // SAFETY: `fd` is a fresh descriptor nothing else owns.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };

        let length = HEADER + IFINFO + attrs.len();
        let mut message = Vec::with_capacity(length);
        message.extend_from_slice(&(length as u32).to_ne_bytes());
        message.extend_from_slice(&kind.to_ne_bytes());
        let flags = (libc::NLM_F_REQUEST | libc::NLM_F_ACK) as u16;
        message.extend_from_slice(&flags.to_ne_bytes());
        message.extend_from_slice(&1u32.to_ne_bytes()); // sequence
        message.extend_from_slice(&0u32.to_ne_bytes()); // port id: the kernel
        message.push(libc::AF_UNSPEC as u8); // ifi_family
        message.push(0); // padding
        message.extend_from_slice(&0u16.to_ne_bytes()); // ifi_type
        message.extend_from_slice(&(index as i32).to_ne_bytes());
        message.extend_from_slice(&0u32.to_ne_bytes()); // ifi_flags
        message.extend_from_slice(&0u32.to_ne_bytes()); // ifi_change
        message.extend_from_slice(attrs);

        // SAFETY: an all-zero sockaddr_nl is the kernel's address.
        let mut kernel: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        kernel.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        // SAFETY: `message` and `kernel` are valid for the lengths given.
        let sent = unsafe {
            libc::sendto(
                socket.as_raw_fd(),
                message.as_ptr().cast(),
                message.len(),
                0,
                std::ptr::addr_of!(kernel).cast(),
                std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if sent < 0 {
            return Err(os_error("failed to send an rtnetlink request".into()));
        }

        let mut reply = [0u8; 4096];
        loop {
            // SAFETY: `reply` is valid for writes of its length.
            let received = unsafe {
                libc::recv(
                    socket.as_raw_fd(),
                    reply.as_mut_ptr().cast(),
                    reply.len(),
                    0,
                )
            };
            if received < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(anyhow::Error::new(err).context("failed to read the rtnetlink reply"));
            }
            return parse_ack(&reply[..received as usize]);
        }
    }

    /// The kernel's answer to an `NLM_F_ACK` request: an `NLMSG_ERROR` whose
    /// code is zero for success and a negated errno otherwise.
    pub(super) fn parse_ack(reply: &[u8]) -> R<()> {
        if reply.len() < 20 {
            bail!("rtnetlink reply was {} bytes, too short", reply.len());
        }
        let kind = u16::from_ne_bytes([reply[4], reply[5]]);
        if kind != libc::NLMSG_ERROR as u16 {
            bail!("rtnetlink replied with message type {kind}, not an acknowledgement");
        }
        let code = i32::from_ne_bytes([reply[16], reply[17], reply[18], reply[19]]);
        if code == 0 {
            return Ok(());
        }
        Err(anyhow::Error::new(std::io::Error::from_raw_os_error(-code)))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::linux::parse_ack;

    fn ack(code: i32) -> Vec<u8> {
        let mut reply = Vec::new();
        reply.extend_from_slice(&36u32.to_ne_bytes());
        reply.extend_from_slice(&(libc::NLMSG_ERROR as u16).to_ne_bytes());
        reply.extend_from_slice(&0u16.to_ne_bytes());
        reply.extend_from_slice(&1u32.to_ne_bytes());
        reply.extend_from_slice(&0u32.to_ne_bytes());
        reply.extend_from_slice(&code.to_ne_bytes());
        reply.extend_from_slice(&[0u8; 16]);
        reply
    }

    #[test]
    fn a_zero_code_acknowledges_the_request() {
        parse_ack(&ack(0)).expect("ack");
    }

    #[test]
    fn a_negative_code_is_the_kernel_errno() {
        let err = parse_ack(&ack(-libc::EPERM)).expect_err("refused");
        let io = err.downcast_ref::<std::io::Error>().expect("io error");
        assert_eq!(io.raw_os_error(), Some(libc::EPERM));
    }

    #[test]
    fn a_short_or_foreign_reply_is_refused() {
        assert!(parse_ack(&[0u8; 8]).is_err());
        let mut reply = ack(0);
        reply[4..6].copy_from_slice(&(libc::RTM_NEWLINK).to_ne_bytes());
        assert!(parse_ack(&reply).is_err());
    }

    /// Exercised only where the test runs with `CAP_NET_ADMIN` and a
    /// `/dev/net/tun`; elsewhere it reports and returns.
    #[test]
    fn creates_raises_shuts_and_deletes_a_tap_when_privileged() {
        let device = format!("lgtst{}", std::process::id() % 100_000);
        if let Err(err) = super::create_persistent_tap(&device) {
            eprintln!("skipping: cannot create TAP devices here: {err:#}");
            return;
        }
        let flags = || {
            std::fs::read_to_string(format!("/sys/class/net/{device}/flags"))
                .ok()
                .and_then(|text| u32::from_str_radix(text.trim().trim_start_matches("0x"), 16).ok())
                .unwrap_or(0)
        };
        assert!(std::path::Path::new(&format!("/sys/class/net/{device}/tun_flags")).exists());
        super::set_link_up(&device, true).expect("up");
        assert_ne!(flags() & libc::IFF_UP as u32, 0);
        super::set_link_up(&device, false).expect("down");
        assert_eq!(flags() & libc::IFF_UP as u32, 0);
        super::delete_link(&device).expect("delete");
        assert!(!std::path::Path::new(&format!("/sys/class/net/{device}")).exists());
    }
}
