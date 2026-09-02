//! `legion guest-agent` — the datapath's foothold inside a gated sandbox
//! (docs/network-handlers.md, "Sandboxes"). Started by the generated guest
//! init from the worker's own binary copied into the rootfs; never run by an
//! operator. Relays the guest's loopback proxy and resolver ports to the
//! host datapath over vsock and then parks.

use crate::prelude::*;
use crate::worker::netd::guest_agent::{self, AgentConfig, VsockDial, HOST_CID};
use clap::{Arg, ArgMatches, Command};
use std::sync::Arc;

pub fn config() -> Command {
    Command::new("guest-agent")
        .about("Sandbox guest agent: relay loopback proxy and DNS ports to the host over vsock (started by the guest init, not by hand)")
        .hide(true)
        .arg(
            Arg::new("proxy-port")
                .long("proxy-port")
                .default_value("1080")
                .help("Guest loopback port (and host vsock port) for the egress proxy"),
        )
        .arg(
            Arg::new("dns-port")
                .long("dns-port")
                .default_value("53")
                .help("Guest loopback UDP port (and host vsock port) for DNS; 0 disables"),
        )
        .arg(
            Arg::new("host-cid")
                .long("host-cid")
                .default_value("2")
                .help("The host's vsock context id"),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    let proxy_port: u16 = matches
        .get_one::<String>("proxy-port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(1080);
    let dns_port: u16 = matches
        .get_one::<String>("dns-port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(53);
    let cid: u32 = matches
        .get_one::<String>("host-cid")
        .and_then(|c| c.parse().ok())
        .unwrap_or(HOST_CID);
    let config = AgentConfig {
        proxy_listen: format!("127.0.0.1:{proxy_port}").parse()?,
        proxy_port: u32::from(proxy_port),
        dns_listen: (dns_port != 0)
            .then(|| format!("127.0.0.1:{dns_port}").parse().expect("static")),
        dns_port: u32::from(dns_port),
    };
    let started = guest_agent::start(config, Arc::new(VsockDial { cid }))?;
    eprintln!(
        "legion-guest-agent: proxy on {}, dns on {} -> host cid {cid}",
        started.proxy_addr,
        started
            .dns_addr
            .map(|a| a.to_string())
            .unwrap_or_else(|| "off".into())
    );
    // The relays run on their own threads; this task only keeps the
    // process alive.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}
