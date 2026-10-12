//! The guest's command line: `guest-agent` and `guest-resume`.
//!
//! The generated guest init runs `/legion/legion <subcommand> ...`. That file
//! is the static `legion-guest` binary when the worker has one, and the
//! worker's own `legion` binary otherwise, so both parse their arguments
//! here: the `legion` subcommands of the same names pass their arguments
//! straight through. One parser, so an init script written for one binary
//! works with the other.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::agent::{self, AgentConfig, VsockDial, VsockListen, HOST_CID};

const USAGE: &str = "\
usage: legion-guest guest-agent [--proxy-port PORT] [--dns-port PORT] [--host-cid CID]
                                [--ingress-port PORT]...
                                [--resume-watch [--identity DEVICE] [--poll-ms MS]]
       legion-guest guest-resume [--identity DEVICE] [--poll-ms MS]

Started by the sandbox guest's init, not by hand.";

/// What `guest-agent` was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentArgs {
    pub proxy_port: u16,
    /// 0 disables the DNS relay.
    pub dns_port: u16,
    pub host_cid: u32,
    /// Ports the node datapath fronts (`--ingress-port`, repeated): the
    /// agent listens on each as a vsock port and relays to the workload's
    /// loopback port of the same number.
    pub ingress_ports: Vec<u16>,
    /// Also run the resume watch in this process (a gated sandbox launched
    /// for memory checkpoints), so a restore can close the relays it holds.
    pub resume_watch: Option<ResumeArgs>,
}

impl Default for AgentArgs {
    fn default() -> Self {
        AgentArgs {
            proxy_port: agent::PROXY_PORT as u16,
            dns_port: agent::DNS_PORT as u16,
            host_cid: HOST_CID,
            ingress_ports: Vec::new(),
            resume_watch: None,
        }
    }
}

/// What `guest-resume` was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeArgs {
    /// The identity disk; `None` reads `legion.identity=` off the kernel
    /// cmdline.
    pub identity: Option<PathBuf>,
    pub poll: Duration,
}

impl Default for ResumeArgs {
    fn default() -> Self {
        ResumeArgs {
            identity: None,
            poll: Duration::from_millis(200),
        }
    }
}

/// A parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    Agent(AgentArgs),
    Resume(ResumeArgs),
    Help,
}

fn value<'a>(flag: &str, rest: &mut impl Iterator<Item = &'a String>) -> Result<&'a str> {
    rest.next()
        .map(String::as_str)
        .with_context(|| format!("{flag} needs a value\n{USAGE}"))
}

fn number<T: std::str::FromStr>(flag: &str, raw: &str) -> Result<T> {
    raw.parse()
        .ok()
        .with_context(|| format!("{flag} must be a number, got {raw:?}"))
}

/// Parse `args`, where `args[0]` is the subcommand. Both `--flag value` and
/// `--flag=value` are accepted, like the `legion` CLI's own parser.
pub fn parse(args: &[String]) -> Result<Invocation> {
    let Some((command, rest)) = args.split_first() else {
        return Ok(Invocation::Help);
    };
    // Split `--flag=value` into two words up front.
    let words: Vec<String> = rest
        .iter()
        .flat_map(|arg| match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => {
                vec![flag.to_string(), value.to_string()]
            }
            _ => vec![arg.clone()],
        })
        .collect();
    let mut rest = words.iter();
    match command.as_str() {
        "guest-agent" => {
            let mut parsed = AgentArgs::default();
            let mut watch = ResumeArgs::default();
            let mut resume_watch = false;
            let mut watch_flag = None;
            while let Some(flag) = rest.next() {
                match flag.as_str() {
                    "--proxy-port" => {
                        parsed.proxy_port = number(flag, value(flag, &mut rest)?)?;
                    }
                    "--dns-port" => parsed.dns_port = number(flag, value(flag, &mut rest)?)?,
                    "--host-cid" => parsed.host_cid = number(flag, value(flag, &mut rest)?)?,
                    "--ingress-port" => {
                        let port: u16 = number(flag, value(flag, &mut rest)?)?;
                        if port == 0 {
                            bail!("--ingress-port must be a port from 1 to 65535\n{USAGE}");
                        }
                        if !parsed.ingress_ports.contains(&port) {
                            parsed.ingress_ports.push(port);
                        }
                    }
                    "--resume-watch" => resume_watch = true,
                    "--identity" => {
                        watch.identity = Some(PathBuf::from(value(flag, &mut rest)?));
                        watch_flag = Some("--identity");
                    }
                    "--poll-ms" => {
                        watch.poll = Duration::from_millis(number(flag, value(flag, &mut rest)?)?);
                        watch_flag = Some("--poll-ms");
                    }
                    "-h" | "--help" => return Ok(Invocation::Help),
                    other => bail!("unknown guest-agent argument {other:?}\n{USAGE}"),
                }
            }
            if let (false, Some(flag)) = (resume_watch, watch_flag) {
                bail!("guest-agent {flag} only applies with --resume-watch\n{USAGE}");
            }
            parsed.resume_watch = resume_watch.then_some(watch);
            Ok(Invocation::Agent(parsed))
        }
        "guest-resume" => {
            let mut parsed = ResumeArgs::default();
            while let Some(flag) = rest.next() {
                match flag.as_str() {
                    "--identity" => {
                        parsed.identity = Some(PathBuf::from(value(flag, &mut rest)?));
                    }
                    "--poll-ms" => {
                        parsed.poll = Duration::from_millis(number(flag, value(flag, &mut rest)?)?);
                    }
                    "-h" | "--help" => return Ok(Invocation::Help),
                    other => bail!("unknown guest-resume argument {other:?}\n{USAGE}"),
                }
            }
            Ok(Invocation::Resume(parsed))
        }
        "-h" | "--help" | "help" => Ok(Invocation::Help),
        other => bail!("unknown command {other:?}\n{USAGE}"),
    }
}

/// Parse and run. Only returns on an error (or `--help`): both commands run
/// for the life of the guest.
pub fn run(args: &[String]) -> Result<()> {
    match parse(args)? {
        Invocation::Help => {
            println!("{USAGE}");
            Ok(())
        }
        Invocation::Agent(args) => run_agent(&args),
        Invocation::Resume(args) => run_resume(&args),
    }
}

fn run_agent(args: &AgentArgs) -> Result<()> {
    let config = AgentConfig {
        proxy_listen: format!("127.0.0.1:{}", args.proxy_port).parse()?,
        proxy_port: u32::from(args.proxy_port),
        dns_listen: (args.dns_port != 0)
            .then(|| format!("127.0.0.1:{}", args.dns_port).parse())
            .transpose()?,
        dns_port: u32::from(args.dns_port),
        ingress_ports: args.ingress_ports.clone(),
    };
    let started = agent::start(
        config,
        Arc::new(VsockDial { cid: args.host_cid }),
        Arc::new(VsockListen),
    )?;
    eprintln!(
        "legion-guest-agent: proxy on {}, dns on {} -> host cid {}; ingress on vsock {}",
        started.proxy_addr,
        started
            .dns_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "off".into()),
        args.host_cid,
        port_list(&started.ingress.ports()),
    );
    log_ingress_failures(&started.ingress_errors);
    if let Some(watch) = args.resume_watch.as_ref() {
        return run_agent_watch(watch, &|identity: &crate::identity::ResumeIdentity| {
            let (closed, failed) = started.restored(&identity.ingress_ports);
            if closed > 0 {
                eprintln!(
                    "legion-guest-agent: restored; closed {closed} connection(s) the capture \
                     left dead"
                );
            }
            log_ingress_failures(&failed);
        });
    }
    // The relays run on their own threads; this one only keeps the process
    // alive.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// `20001, 20002`, or `none`.
fn port_list(ports: &[u16]) -> String {
    if ports.is_empty() {
        return "none".into();
    }
    ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// One console line per ingress port the agent could not listen on. The
/// datapath's connections to that port then fail, and the console says why.
fn log_ingress_failures(failed: &[(u16, std::io::Error)]) {
    for (port, err) in failed {
        eprintln!("legion-guest-agent: cannot listen on vsock port {port} for ingress: {err}");
    }
}

/// The agent's resume watch: the resume handler's loop, closing the agent's
/// relays (and adding the restore's ingress ports) first on every restore.
#[cfg(target_os = "linux")]
fn run_agent_watch(
    args: &ResumeArgs,
    on_restore: &(dyn Fn(&crate::identity::ResumeIdentity) + Sync),
) -> Result<()> {
    let device = identity_device(args.identity.as_ref())?;
    eprintln!("legion-guest-resume: watching {}", device.display());
    crate::resume::watch_with(&device, &crate::resume::LinuxGuest, args.poll, on_restore)
}

#[cfg(not(target_os = "linux"))]
fn run_agent_watch(
    args: &ResumeArgs,
    _on_restore: &(dyn Fn(&crate::identity::ResumeIdentity) + Sync),
) -> Result<()> {
    let _ = identity_device(args.identity.as_ref());
    bail!("guest-agent --resume-watch runs inside a Linux sandbox guest only")
}

/// The identity disk: the flag, else `legion.identity=` on the kernel
/// cmdline.
fn identity_device(explicit: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(device) = explicit {
        return Ok(device.clone());
    }
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    crate::resume::identity_device_from_cmdline(&cmdline).context(
        "no legion.identity= on the kernel cmdline; this sandbox was not launched for memory \
         checkpoints",
    )
}

#[cfg(target_os = "linux")]
fn run_resume(args: &ResumeArgs) -> Result<()> {
    let device = identity_device(args.identity.as_ref())?;
    eprintln!("legion-guest-resume: watching {}", device.display());
    crate::resume::watch(&device, &crate::resume::LinuxGuest, args.poll)
}

#[cfg(not(target_os = "linux"))]
fn run_resume(args: &ResumeArgs) -> Result<()> {
    let _ = identity_device(args.identity.as_ref());
    bail!("guest-resume runs inside a Linux sandbox guest only")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| word.to_string()).collect()
    }

    #[test]
    fn the_init_scripts_command_lines_parse() {
        assert_eq!(
            parse(&args(&["guest-agent"])).unwrap(),
            Invocation::Agent(AgentArgs::default())
        );
        assert_eq!(
            parse(&args(&["guest-agent", "--dns-port", "0", "--host-cid=2"])).unwrap(),
            Invocation::Agent(AgentArgs {
                dns_port: 0,
                ..AgentArgs::default()
            })
        );
        assert_eq!(
            parse(&args(&[
                "guest-resume",
                "--identity",
                "/dev/vdc",
                "--poll-ms=50"
            ]))
            .unwrap(),
            Invocation::Resume(ResumeArgs {
                identity: Some(PathBuf::from("/dev/vdc")),
                poll: Duration::from_millis(50),
            })
        );
        assert_eq!(parse(&[]).unwrap(), Invocation::Help);
        // A gated sandbox launched for checkpoints: one process.
        assert_eq!(
            parse(&args(&[
                "guest-agent",
                "--resume-watch",
                "--poll-ms",
                "100"
            ]))
            .unwrap(),
            Invocation::Agent(AgentArgs {
                resume_watch: Some(ResumeArgs {
                    identity: None,
                    poll: Duration::from_millis(100),
                }),
                ..AgentArgs::default()
            })
        );
        // A gated sandbox that advertises ports: one flag per port,
        // de-duplicated.
        assert_eq!(
            parse(&args(&[
                "guest-agent",
                "--ingress-port",
                "20001",
                "--ingress-port=8080",
                "--ingress-port",
                "20001",
                "--resume-watch",
            ]))
            .unwrap(),
            Invocation::Agent(AgentArgs {
                ingress_ports: vec![20001, 8080],
                resume_watch: Some(ResumeArgs::default()),
                ..AgentArgs::default()
            })
        );
        let err = parse(&args(&["guest-agent", "--ingress-port", "0"])).unwrap_err();
        assert!(err.to_string().contains("from 1 to 65535"), "{err}");
        let err = parse(&args(&["guest-agent", "--identity", "/dev/vdc"])).unwrap_err();
        assert!(
            err.to_string().contains("only applies with --resume-watch"),
            "{err}"
        );
    }

    #[test]
    fn bad_arguments_are_named() {
        let err = parse(&args(&["guest-agent", "--proxy-port", "x"])).unwrap_err();
        assert!(
            err.to_string().contains("--proxy-port must be a number"),
            "{err}"
        );
        let err = parse(&args(&["guest-agent", "--nope"])).unwrap_err();
        assert!(
            err.to_string().contains("unknown guest-agent argument"),
            "{err}"
        );
        let err = parse(&args(&["guest-resume", "--identity"])).unwrap_err();
        assert!(
            err.to_string().contains("--identity needs a value"),
            "{err}"
        );
        assert!(parse(&args(&["serve"])).is_err());
    }
}
