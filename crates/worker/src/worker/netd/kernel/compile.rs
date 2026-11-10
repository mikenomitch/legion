//! Compile an allocation's effective egress chain into the rule array the
//! kernel programs walk (plan §6.A.2), and a Rust model of that walk.
//!
//! Compiling is a pure function of the chain. The kernel sees an address
//! and a port, never a name, so a host rule becomes a bit: the allocation's
//! resolver sets, for every address it returns for an allowed name, the
//! bits of the host rules that name matches (`learned_bits`). An address no
//! answer taught the kernel cannot satisfy a host rule; the walk stops there
//! and asks netd, which reads the name off the flow (SNI or `Host`).
//!
//! The model ([`model`]) is what the property test checks against
//! `evaluate_connect`: for an address learned from one name, the kernel's
//! verdict is the userspace verdict for that name, or a hand-off to netd.

use crate::structs::netpolicy::{host_matches, parse_cidr, EffectiveChain, ACTION_ALLOW};
use crate::worker::netd::kernel::abi;
use std::net::IpAddr;

/// One compiled chain, ready for the `ln_rules` map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    pub rules: Vec<abi::Rule>,
    /// The host globs, by bit: a host rule's `host_bit` indexes this.
    pub hosts: Vec<String>,
    /// Each compiled rule's source (`policy/sandbox#2`, `task#0`) and
    /// reason, for the events a denial raises.
    pub labels: Vec<(String, Option<String>)>,
    /// What each compiled rule matches, as the service graph names a pass
    /// flow's destination (`*.pypi.org`, `10.0.0.0/8:5432`, `port 25`).
    pub targets: Vec<String>,
    pub default_allow: bool,
    /// Allowed TCP flows still need netd: the chain has hops, or it is an
    /// HTTP chain with an access log.
    pub inspect: bool,
}

/// Why a chain cannot run in the kernel. The allocation keeps today's
/// behavior and its status says why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsupported(pub String);

/// Compile `chain` for the kernel.
pub fn compile(chain: &EffectiveChain) -> Result<Compiled, Unsupported> {
    let mut out = Compiled {
        rules: Vec::new(),
        hosts: Vec::new(),
        labels: Vec::new(),
        targets: Vec::new(),
        default_allow: !chain.denies_by_default(),
        inspect: !chain.handlers.is_empty() || (chain.is_http() && chain.access_log),
    };
    for entry in &chain.rules {
        let matcher = &entry.rule.matcher;
        // An egress flow has no verified peer identity, so a rule that
        // needs one never matches it (as evaluate_connect sees it).
        if matcher.identity.is_some() {
            continue;
        }
        if out.rules.len() == abi::MAX_RULES {
            return Err(Unsupported(format!(
                "the egress chain has more than {} connect-time rules",
                abi::MAX_RULES
            )));
        }
        let mut rule = abi::Rule {
            action: if entry.rule.action == ACTION_ALLOW {
                abi::ALLOW
            } else {
                abi::DENY
            },
            ..Default::default()
        };
        if let Some(port) = matcher.port {
            rule.flags |= abi::R_PORT;
            rule.port = abi::port_ne(port);
        }
        if let Some(cidr) = matcher.cidr.as_deref() {
            let net = parse_cidr(cidr)
                .map_err(|err| Unsupported(format!("{}: {err:#}", entry.source)))?;
            // Words without the IPv4-mapped folding: a mapped CIDR stays
            // IPv6, as `Cidr::contains` treats it.
            let (words, family) = match net.addr {
                IpAddr::V4(_) => abi::words_from_ip(net.addr),
                IpAddr::V6(v6) => {
                    let octets = v6.octets();
                    let mut words = [0u32; 4];
                    for (i, word) in words.iter_mut().enumerate() {
                        *word = u32::from_ne_bytes([
                            octets[i * 4],
                            octets[i * 4 + 1],
                            octets[i * 4 + 2],
                            octets[i * 4 + 3],
                        ]);
                    }
                    (words, 6)
                }
            };
            rule.flags |= abi::R_CIDR;
            rule.addr = words;
            rule.family = family;
            rule.prefix = net.prefix;
        }
        if let Some(glob) = matcher.host.as_deref() {
            let bit = match out.hosts.iter().position(|known| known == glob) {
                Some(bit) => bit,
                None => {
                    if out.hosts.len() == 64 {
                        return Err(Unsupported(
                            "the egress chain names more than 64 distinct hosts".into(),
                        ));
                    }
                    out.hosts.push(glob.to_string());
                    out.hosts.len() - 1
                }
            };
            rule.flags |= abi::R_HOST;
            rule.host_bit = bit as u8;
        }
        if matcher.needs_request() {
            rule.flags |= abi::R_DEFER;
        }
        out.rules.push(rule);
        out.labels
            .push((entry.source.clone(), entry.rule.reason.clone()));
        let what = matcher
            .host
            .clone()
            .or_else(|| matcher.cidr.clone())
            .unwrap_or_else(|| "*".to_string());
        out.targets.push(match matcher.port {
            Some(port) if what == "*" => format!("port {port}"),
            Some(port) => format!("{what}:{port}"),
            None => what,
        });
    }
    Ok(out)
}

impl Compiled {
    /// The bits a name sets on the addresses it resolves to: one per host
    /// glob it matches.
    pub fn learned_bits(&self, name: &str) -> u64 {
        self.hosts
            .iter()
            .enumerate()
            .filter(|(_, glob)| host_matches(glob, name))
            .fold(0, |bits, (bit, _)| bits | (1u64 << bit))
    }

    /// Whether any rule names a host, so resolver answers are worth
    /// teaching the kernel.
    pub fn has_hosts(&self) -> bool {
        !self.hosts.is_empty()
    }

    /// The source and reason of the rule at `index` (a compiled index, as an
    /// event carries it).
    pub fn label(&self, index: u32) -> Option<&(String, Option<String>)> {
        self.labels.get(index as usize)
    }

    /// The `ln_rules` map value.
    pub fn rules_value(&self) -> abi::Rules {
        let mut value = abi::Rules::default();
        for (slot, rule) in value.rule.iter_mut().zip(&self.rules) {
            *slot = *rule;
        }
        value
    }
}

/// What the kernel walk decides for one flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Model {
    /// Allowed by the compiled rule at this index (`None`: the fallthrough).
    Allow(Option<usize>),
    Deny(Option<usize>),
    /// Only netd can decide: a deferring rule, or a host rule for an
    /// address no answer taught the kernel (`None`).
    NeedsNetd(Option<usize>),
}

/// The kernel's walk, in Rust: `learned` is the bits the kernel holds for
/// `ip` (`None` when it holds none, or they expired).
pub fn model(compiled: &Compiled, ip: IpAddr, port: u16, learned: Option<u64>) -> Model {
    let (words, family) = abi::words_from_ip(ip);
    for (index, rule) in compiled.rules.iter().enumerate() {
        if rule.flags & abi::R_PORT != 0 && rule.port != abi::port_ne(port) {
            continue;
        }
        if rule.flags & abi::R_CIDR != 0
            && (rule.family != family || !prefix_match(&rule.addr, &words, rule.prefix, family))
        {
            continue;
        }
        if rule.flags & abi::R_HOST != 0 {
            let Some(bits) = learned else {
                return Model::NeedsNetd(None);
            };
            if bits & (1u64 << rule.host_bit) == 0 {
                continue;
            }
        }
        if rule.flags & abi::R_DEFER != 0 {
            return Model::NeedsNetd(Some(index));
        }
        return if rule.action == abi::ALLOW {
            Model::Allow(Some(index))
        } else {
            Model::Deny(Some(index))
        };
    }
    if compiled.default_allow {
        Model::Allow(None)
    } else {
        Model::Deny(None)
    }
}

/// `prefix_match` in the C programs: the first `prefix` bits of
/// network-order words.
fn prefix_match(want: &[u32; 4], have: &[u32; 4], prefix: u8, family: u8) -> bool {
    let words = if family == 4 { 1 } else { 4 };
    let mut prefix = u32::from(prefix);
    for i in 0..words {
        if prefix >= 32 {
            if want[i] != have[i] {
                return false;
            }
            prefix -= 32;
            continue;
        }
        if prefix == 0 {
            return true;
        }
        let mask = (!((1u32 << (32 - prefix)) - 1)).to_be();
        return want[i] & mask == have[i] & mask;
    }
    true
}

#[cfg(test)]
mod compile_test {
    use super::*;
    use crate::structs::netpolicy::{
        evaluate_connect, merge_policies, ChainConfig, ConnectFacts, InlinePolicy, Verdict,
    };
    use serde_json::json;

    fn chain(value: serde_json::Value) -> EffectiveChain {
        let mut config: ChainConfig = serde_json::from_value(value).unwrap();
        config.normalize_and_validate("egress").unwrap();
        merge_policies(
            &[],
            InlinePolicy {
                egress: Some(&config),
                ..Default::default()
            },
        )
        .egress
        .unwrap()
    }

    #[test]
    fn rules_compile_in_order_with_host_bits_test() {
        let c = chain(json!({
            "policy": "deny",
            "rules": [
                { "when": { "port": 25 }, "action": "deny", "reason": "no mail" },
                { "when": { "host": "*.pypi.org" }, "action": "allow" },
                { "when": { "cidr": "10.0.0.0/8", "port": 5432 }, "action": "allow" },
                { "when": { "identity": "x" }, "action": "allow" },
                { "when": { "host": "*.pypi.org", "port": 80 }, "action": "deny" },
            ],
        }));
        let compiled = compile(&c).unwrap();
        assert_eq!(
            compiled.rules.len(),
            4,
            "the identity rule never matches egress"
        );
        assert_eq!(compiled.hosts, vec!["*.pypi.org".to_string()]);
        assert_eq!(compiled.rules[3].host_bit, 0, "one bit per distinct glob");
        assert_eq!(compiled.label(0).unwrap().1.as_deref(), Some("no mail"));
        assert!(!compiled.default_allow);
        assert_eq!(compiled.learned_bits("files.pypi.org"), 1);
        assert_eq!(compiled.learned_bits("pypi.org"), 0);
    }

    #[test]
    fn the_walk_hands_unlearned_hosts_and_deferred_rules_to_netd_test() {
        let c = chain(json!({
            "protocol": "http",
            "policy": "deny",
            "rules": [
                { "when": { "cidr": "192.0.2.0/24" }, "action": "deny" },
                { "when": { "host": "api.github.com", "path": "/repos/**" }, "action": "allow" },
                { "when": { "host": "*.github.com" }, "action": "allow" },
            ],
        }));
        let compiled = compile(&c).unwrap();
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        assert_eq!(
            model(&compiled, ip("192.0.2.9"), 443, None),
            Model::Deny(Some(0))
        );
        assert_eq!(
            model(&compiled, ip("140.82.112.5"), 443, None),
            Model::NeedsNetd(None)
        );
        let api = compiled.learned_bits("api.github.com");
        assert_eq!(
            model(&compiled, ip("140.82.112.5"), 443, Some(api)),
            Model::NeedsNetd(Some(1))
        );
        let raw = compiled.learned_bits("raw.github.com");
        assert_eq!(
            model(&compiled, ip("140.82.112.5"), 443, Some(raw)),
            Model::Allow(Some(2))
        );
        assert_eq!(
            model(&compiled, ip("8.8.8.8"), 443, Some(0)),
            Model::Deny(None)
        );
    }

    #[test]
    fn inspect_follows_hops_and_the_http_access_log_test() {
        assert!(
            !compile(&chain(json!({ "policy": "allow" })))
                .unwrap()
                .inspect
        );
        assert!(
            compile(&chain(json!({ "protocol": "http", "accessLog": true })))
                .unwrap()
                .inspect
        );
        assert!(
            compile(&chain(json!({
                "handlers": [{ "authz": { "url": "http://policy.internal/check" } }]
            })))
            .unwrap()
            .inspect
        );
    }

    #[test]
    fn too_many_rules_is_unsupported_test() {
        let rules: Vec<_> = (0..65)
            .map(|i| json!({ "when": { "port": 1000 + i }, "action": "deny" }))
            .collect();
        let err = compile(&chain(json!({ "rules": rules }))).unwrap_err();
        assert!(err.0.contains("more than 64"), "{err:?}");
    }

    /// The compiler and `evaluate_connect` agree (plan §9): for random
    /// chains and flows whose address was learned from one name, the
    /// kernel walk's verdict is the userspace verdict for that name, or a
    /// hand-off to netd; and a flow the kernel decides without a name is
    /// decided the same way with any name.
    #[test]
    fn the_kernel_walk_agrees_with_evaluate_connect_property_test() {
        let mut rng = Lcg(0x5eed_1e61);
        let hosts = [
            "*.pypi.org",
            "files.pypi.org",
            "api.github.com",
            "*.github.com",
            "*",
        ];
        let names = [
            "files.pypi.org",
            "pypi.org",
            "api.github.com",
            "raw.github.com",
            "x.example",
        ];
        let cidrs = [
            "10.0.0.0/8",
            "10.1.0.0/16",
            "192.0.2.0/24",
            "0.0.0.0/0",
            "2001:db8::/32",
        ];
        let ips = [
            "10.1.2.3",
            "10.9.9.9",
            "192.0.2.7",
            "8.8.8.8",
            "2001:db8::9",
            "2001:dead::1",
        ];
        let ports = [443u16, 80, 5432, 25];
        for _ in 0..3000 {
            let mut rules = Vec::new();
            for _ in 0..rng.below(6) {
                let mut when = serde_json::Map::new();
                if rng.below(2) == 0 {
                    when.insert("host".into(), json!(hosts[rng.below(hosts.len())]));
                }
                if rng.below(3) == 0 {
                    when.insert("cidr".into(), json!(cidrs[rng.below(cidrs.len())]));
                }
                if rng.below(3) == 0 {
                    when.insert("port".into(), json!(ports[rng.below(ports.len())]));
                }
                if rng.below(5) == 0 {
                    when.insert("path".into(), json!("/admin/**"));
                }
                let action = if rng.below(2) == 0 { "allow" } else { "deny" };
                rules.push(json!({ "when": when, "action": action }));
            }
            let needs_http = rules.iter().any(|rule| rule["when"].get("path").is_some());
            let c = chain(json!({
                "protocol": if needs_http { "http" } else { "tcp" },
                "policy": if rng.below(2) == 0 { "allow" } else { "deny" },
                "rules": rules,
            }));
            let compiled = compile(&c).unwrap();
            let ip: IpAddr = ips[rng.below(ips.len())].parse().unwrap();
            let port = ports[rng.below(ports.len())];
            let name = names[rng.below(names.len())];
            let userspace = evaluate_connect(
                &c,
                &ConnectFacts {
                    host: Some(name),
                    ip: Some(ip),
                    port,
                    identity: None,
                },
            );
            let kernel = model(&compiled, ip, port, Some(compiled.learned_bits(name)));
            match (kernel, &userspace) {
                (Model::Allow(_), Verdict::Allow) | (Model::Deny(_), Verdict::Deny { .. }) => {}
                (Model::NeedsNetd(_), _) => {}
                (kernel, userspace) => panic!(
                    "kernel {kernel:?} vs userspace {userspace:?} for {name} {ip}:{port} under {c:?}"
                ),
            }
            // Decided without a name: the same for every name.
            if let model @ (Model::Allow(_) | Model::Deny(_)) = model(&compiled, ip, port, None) {
                for name in names {
                    let userspace = evaluate_connect(
                        &c,
                        &ConnectFacts {
                            host: Some(name),
                            ip: Some(ip),
                            port,
                            identity: None,
                        },
                    );
                    assert_eq!(
                        matches!(model, Model::Allow(_)),
                        matches!(userspace, Verdict::Allow),
                        "nameless {model:?} vs {userspace:?} for {name} {ip}:{port}"
                    );
                    assert!(!matches!(userspace, Verdict::Defer));
                }
            }
        }
    }

    /// A tiny deterministic generator, so the property test needs no crate.
    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % n.max(1)
        }
    }
}
