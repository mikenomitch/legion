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

use crate::agent::{self, AgentConfig, VsockDial, HOST_CID};

const USAGE: &str = "\
usage: legion-guest guest-agent [--proxy-port PORT] [--dns-port PORT] [--host-cid CID]
       legion-guest guest-resume [--identity DEVICE] [--poll-ms MS]

Started by the sandbox guest's init, not by hand.";

/// What `guest-agent` was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentArgs {
    pub proxy_port: u16,
    /// 0 disables the DNS relay.
    pub dns_port: u16,
    pub host_cid: u32,
}

impl Default for AgentArgs {
    fn default() -> Self {
        AgentArgs {
            proxy_port: agent::PROXY_PORT as u16,
            dns_port: agent::DNS_PORT as u16,
            host_cid: HOST_CID,
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
            while let Some(flag) = rest.next() {
                match flag.as_str() {
                    "--proxy-port" => {
                        parsed.proxy_port = number(flag, value(flag, &mut rest)?)?;
                    }
                    "--dns-port" => parsed.dns_port = number(flag, value(flag, &mut rest)?)?,
                    "--host-cid" => parsed.host_cid = number(flag, value(flag, &mut rest)?)?,
                    "-h" | "--help" => return Ok(Invocation::Help),
                    other => bail!("unknown guest-agent argument {other:?}\n{USAGE}"),
                }
            }
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
    };
    let started = agent::start(config, Arc::new(VsockDial { cid: args.host_cid }))?;
    eprintln!(
        "legion-guest-agent: proxy on {}, dns on {} -> host cid {}",
        started.proxy_addr,
        started
            .dns_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "off".into()),
        args.host_cid
    );
    // The relays run on their own threads; this one only keeps the process
    // alive.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
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
