//! What a desired allocation set implies for the datapath.
//!
//! The listeners each allocation needs ([`upstream_specs`], [`dns_specs`],
//! [`proxy_specs`], [`ingress_specs`]), the endpoints each upstream may dial
//! ([`dialable_endpoints`]), the credentials the node must fetch
//! ([`declared_credentials`]), and what the node reports back about each
//! allocation ([`net_status`]). Where every listener binds, and whether it
//! fails closed, comes from the allocation's [`Attachment`].

use crate::prelude::*;
use crate::structs::{AllocNetwork, TaskAllocation, UpstreamEndpoints};
use crate::worker::balance::Endpoint;
use crate::worker::forwarder::{CredentialKey, UpstreamForwarder, UpstreamSpec};
use crate::worker::netd::attach::{guest_socket, Attachment};
use crate::worker::netd::dns::DnsSpec;
use crate::worker::netd::ingress::IngressSpec;
use crate::worker::netd::proxy::ProxySpec;

use std::path::Path;

/// An allocation's network payload, when it carries a valid one.
fn network_of(allocation: &TaskAllocation) -> Option<AllocNetwork> {
    if allocation.network.trim().is_empty() {
        return None;
    }
    serde_json::from_str(&allocation.network).ok()
}

/// Derive the listeners a desired allocation set implies: one per upstream of
/// every allocation whose task declares a `connect` block.
///
/// `task_connect` is the declaration side (which services, which strategy);
/// the loopback ports those upstreams were *assigned* live on the allocation's
/// network, next to its host ports, because both come out of the same per-node
/// port space. Allocations that declare no upstreams cost one string check.
///
/// A gated sandbox's upstream listens on its VMM's vsock host socket for the
/// upstream's local port, which the guest agent relays the workload's
/// connections to; the VIP is the guest's to serve. A guest restored from a
/// memory checkpoint still dials the loopback port it was captured with, and
/// its agent relays that to this allocation's local port, so nothing here
/// depends on the guest's history.
pub fn upstream_specs(desired: &[TaskAllocation], alloc_root: &Path) -> Vec<UpstreamSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        if allocation.task_connect.trim().is_empty() || allocation.network.trim().is_empty() {
            continue;
        }
        let network: AllocNetwork = match serde_json::from_str(&allocation.network) {
            Ok(network) => network,
            Err(err) => {
                // Retrying cannot fix a payload the manager wrote; skip this
                // allocation's upstreams rather than the whole heartbeat.
                warn!(
                    allocation_id = allocation.allocation_id,
                    error = ?err,
                    "Skipping upstream forwarders; invalid allocation network payload"
                );
                continue;
            }
        };
        // The declaration side carries the `http` block and handlers, keyed
        // the same way the assigned loopback ports are.
        let declared: crate::structs::ConnectConfig =
            serde_json::from_str(&allocation.task_connect).unwrap_or_default();
        let attachment = Attachment::of(allocation, &network, alloc_root);
        let vips = attachment.serves_vips();
        for upstream in &network.upstreams {
            let declaration = declared
                .upstreams
                .iter()
                .find(|candidate| candidate.key() == upstream.service);
            specs.push(UpstreamSpec {
                allocation_id: allocation.allocation_id,
                service: upstream.service.clone(),
                local_port: upstream.local_port,
                balance: upstream.balance.clone(),
                to: upstream.to.clone(),
                http: declaration.and_then(|d| d.http.clone()),
                handlers: declaration.map(|d| d.handlers.clone()).unwrap_or_default(),
                credentials: declaration
                    .map(|d| d.credentials.clone())
                    .unwrap_or_default(),
                vip: upstream.vip.clone().filter(|_| vips),
                canonical_port: upstream.canonical_port.filter(|_| vips),
                attachment: attachment.clone(),
            });
        }
    }
    specs
}

/// The per-allocation resolvers a desired set implies: one per allocation
/// that was assigned a VIP block (or the gated default address), answering
/// `<key>.svc.legion` with that allocation's upstream VIPs, and one per gated
/// sandbox, on its vsock socket, with or without a block.
pub fn dns_specs(desired: &[TaskAllocation], alloc_root: &Path) -> Vec<DnsSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        let Some(network) = network_of(allocation) else {
            continue;
        };
        let resolver = network
            .resolver_address()
            .and_then(|addr| addr.parse::<std::net::Ipv4Addr>().ok());
        let attachment = Attachment::of(allocation, &network, alloc_root);
        // Every gated sandbox gets a resolver on its vsock socket, VIP block
        // or not: without one, a guest with no upstreams has no DNS at all
        // (gated plan §6.5, calls entry 29). Everything else needs an
        // address.
        if resolver.is_none() && attachment.guest_vsock().is_none() {
            continue;
        }
        let names = network
            .vip_names()
            .into_iter()
            .filter_map(|(key, (vip, port))| {
                Some((key, (vip.parse::<std::net::Ipv4Addr>().ok()?, port)))
            })
            .collect();
        specs.push(DnsSpec {
            allocation_id: allocation.allocation_id,
            resolver,
            names,
            attachment,
        });
    }
    specs
}

/// The egress proxies a desired set implies: one for every gated
/// allocation, inside its namespace or on its guest's vsock socket.
pub fn proxy_specs(desired: &[TaskAllocation], alloc_root: &Path) -> Vec<ProxySpec> {
    desired
        .iter()
        .filter_map(|allocation| {
            let network = network_of(allocation)?;
            let attachment = Attachment::of(allocation, &network, alloc_root);
            attachment.is_gated().then(|| ProxySpec {
                allocation_id: allocation.allocation_id,
                attachment,
                listen: None,
            })
        })
        .collect()
}

/// The ingress listeners a desired set implies, all bound in the host
/// namespace:
///
/// - one per advertised port that was given a private twin at placement
///   (the task has an ingress chain), dialing the twin on loopback, inside
///   the task's namespace when it is gated;
/// - one per advertised port of a gated or TAP-wired sandbox, with or
///   without a chain: the guest has its own port space, which nothing else
///   on the node reaches. A gated guest is dialed over host-initiated vsock
///   on its VMM's socket, a TAP guest at its leased address on the bridge.
pub fn ingress_specs(desired: &[TaskAllocation], alloc_root: &Path) -> Vec<IngressSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        let Some(network) = network_of(allocation) else {
            continue;
        };
        specs.extend(allocation_ingress_specs(allocation, &network, alloc_root));
    }
    specs
}

/// [`ingress_specs`] for one allocation whose network is already parsed.
fn allocation_ingress_specs(
    allocation: &TaskAllocation,
    network: &AllocNetwork,
    alloc_root: &Path,
) -> Vec<IngressSpec> {
    if network.ports.is_empty() {
        return Vec::new();
    }
    let attachment = Attachment::of(allocation, network, alloc_root);
    let dial = attachment.task_dial();
    network
        .ports
        .iter()
        .filter(|port| attachment.fronts_every_port() || port.private_port.is_some())
        .map(|port| IngressSpec {
            allocation_id: allocation.allocation_id,
            label: port.name.clone(),
            host_port: port.host_port,
            private_port: port.bind_port(),
            dial: dial.clone(),
            bind: None,
        })
        .collect()
}

/// The allocations that get a gated network namespace (`container` and
/// `exec` on `network.mode = "gated"`).
pub fn gated_namespace_ids(desired: &[TaskAllocation], alloc_root: &Path) -> Vec<i64> {
    desired
        .iter()
        .filter(|allocation| {
            network_of(allocation).is_some_and(|network| {
                Attachment::of(allocation, &network, alloc_root)
                    .netns()
                    .is_some()
            })
        })
        .map(|allocation| allocation.allocation_id)
        .collect()
}

/// Endpoints from one `UpstreamEndpoints` entry that the forwarder can
/// actually dial. The manager mirrors `ResolveService` and keeps entries whose
/// node advertises no address (or whose port is unassigned); those are not
/// dialable, so they are dropped here rather than turned into a bogus
/// `:0` address. Endpoints are already healthy-only and intention-filtered.
/// `mesh_tls` comes through as the manager set it: a mesh endpoint refuses a
/// plaintext dial at the handshake.
pub fn dialable_endpoints(update: &UpstreamEndpoints) -> Vec<Endpoint> {
    update
        .endpoints
        .iter()
        .filter(|endpoint| !endpoint.address.trim().is_empty() && endpoint.port != 0)
        .map(|endpoint| Endpoint {
            addr: format!("{}:{}", endpoint.address, endpoint.port),
            allocation_id: endpoint.allocation_id,
            node_id: endpoint.node_id.clone(),
            tags: endpoint.tags.clone(),
            mesh_tls: endpoint.mesh_tls,
        })
        .collect()
}

/// Every credential header the desired set declares, as
/// (allocation, upstream key, lower-cased header): what the node fetches
/// and what its credential store may keep.
pub fn declared_credentials(desired: &[TaskAllocation]) -> Vec<CredentialKey> {
    let mut keys = Vec::new();
    for allocation in desired {
        if allocation.task_connect.trim().is_empty() {
            continue;
        }
        let Ok(connect) =
            serde_json::from_str::<crate::structs::ConnectConfig>(&allocation.task_connect)
        else {
            continue;
        };
        for upstream in &connect.upstreams {
            for credential in &upstream.credentials {
                keys.push((
                    allocation.allocation_id,
                    upstream.key().to_string(),
                    credential.header.trim().to_ascii_lowercase(),
                ));
            }
        }
    }
    keys
}

/// What the datapath reports about each desired allocation on the next
/// heartbeat (`NetAllocStatus`): the node's own answer to "is this
/// enforced?", read back by `legion net doctor` and the UI.
pub fn net_status(
    forwarder: &UpstreamForwarder,
    desired: &[TaskAllocation],
    alloc_root: &Path,
) -> Vec<crate::structs::NetAllocStatus> {
    let bound_upstreams: HashMap<i64, Vec<String>> =
        forwarder
            .active()
            .into_iter()
            .fold(HashMap::new(), |mut acc, spec| {
                acc.entry(spec.allocation_id)
                    .or_default()
                    .push(spec.service);
                acc
            });
    let resolvers: std::collections::HashSet<i64> = forwarder
        .dns()
        .map(|dns| dns.active().into_iter().collect())
        .unwrap_or_default();
    let ingress_bound: HashMap<i64, Vec<String>> = forwarder
        .ingress()
        .map(|ingress| {
            ingress.active().into_iter().fold(
                HashMap::<i64, Vec<String>>::new(),
                |mut acc, (id, label)| {
                    acc.entry(id).or_default().push(label);
                    acc
                },
            )
        })
        .unwrap_or_default();
    let context = forwarder.context();
    let cache = context.cache();
    let mut out = Vec::new();
    for allocation in desired {
        let id = allocation.allocation_id;
        // A TAP sandbox's leased guest address (docs/task-runtime-sandbox.md,
        // "Addressing"), which makes the allocation worth reporting even with
        // nothing else on the datapath.
        let lease = (allocation.task_runtime == "sandbox")
            .then(|| {
                crate::task_plugins::sandbox_addr::read_lease(
                    &crate::task_plugins::sandbox_dir_for(&alloc_root.join(id.to_string())),
                )
            })
            .flatten();
        let Some(network) = network_of(allocation) else {
            if let Some(lease) = lease {
                out.push(crate::structs::NetAllocStatus {
                    allocation_id: id,
                    mode: crate::structs::netpolicy::MODE_HOST.to_string(),
                    capture_ready: true,
                    tls_ready: true,
                    guest_address: lease.address.to_string(),
                    guest_bridge: lease.bridge,
                    ..Default::default()
                });
            }
            continue;
        };
        let attachment = Attachment::of(allocation, &network, alloc_root);
        let gated = attachment.is_gated();
        // A gated sandbox has no namespace: its capture is the vsock host
        // sockets, bound like any listener.
        let guest = attachment.guest_vsock();
        let expected_upstreams = network.upstreams.len();
        let expected_ingress = allocation_ingress_specs(allocation, &network, alloc_root).len();
        if !gated
            && expected_upstreams == 0
            && expected_ingress == 0
            && network.vip_block.is_none()
            && lease.is_none()
        {
            continue;
        }
        let upstreams = bound_upstreams.get(&id).cloned().unwrap_or_default();
        let ingress = ingress_bound.get(&id).cloned().unwrap_or_default();
        let resolver_ready = resolvers.contains(&id);
        let proxy_ready = forwarder
            .proxies()
            .is_some_and(|proxies| proxies.is_running(id));
        let mut problems: Vec<String> = forwarder
            .ingress()
            .map(|ingress| ingress.problems(id))
            .unwrap_or_default();
        let namespace_ready = match &attachment {
            Attachment::Netns { .. } => crate::worker::netd::netns::netns_exists(id),
            _ => true,
        };
        if !namespace_ready {
            problems.push("gated network namespace does not exist yet".into());
        }
        if gated && !proxy_ready {
            problems.push(match guest {
                // A restored guest resumes fail-closed without it (gated
                // plan §6.3); this is how `net doctor` names the socket.
                Some(scratch) => format!(
                    "egress proxy is not bound on the guest's vsock socket {}",
                    guest_socket(scratch, crate::worker::netd::proxy::PROXY_PORT).display()
                ),
                None => "egress proxy is not bound inside the namespace".into(),
            });
        }
        if upstreams.len() < expected_upstreams {
            problems.push(format!(
                "{} of {} upstream listeners bound",
                upstreams.len(),
                expected_upstreams
            ));
            // A gated sandbox's upstreams are vsock host sockets: name the
            // unbound ones, as for the proxy and the resolver.
            if let Some(scratch) = guest {
                for upstream in network
                    .upstreams
                    .iter()
                    .filter(|upstream| !upstreams.contains(&upstream.service))
                {
                    problems.push(format!(
                        "upstream {} is not bound on the guest's vsock socket {}",
                        upstream.service,
                        guest_socket(scratch, upstream.local_port).display()
                    ));
                }
            }
        }
        match guest {
            Some(scratch) if !resolver_ready => problems.push(format!(
                "allocation resolver is not bound on the guest's vsock socket {}",
                guest_socket(scratch, crate::worker::netd::dns::DNS_PORT).display()
            )),
            None if network.vip_block.is_some() && !resolver_ready => problems.push(
                "allocation resolver is not bound (port 53 needs CAP_NET_BIND_SERVICE)".into(),
            ),
            _ => {}
        }
        // A gated sandbox's capture is its vsock host sockets: the proxy, the
        // resolver, and one per declared upstream (counted with the rest).
        let capture_ready = if gated {
            namespace_ready
                && proxy_ready
                && (guest.is_none() || resolver_ready)
                && upstreams.len() >= expected_upstreams
        } else {
            true
        };
        let policy_hash = cache
            .filter(|cache| cache.is_ready(id))
            .and_then(|cache| cache.hash_for(id))
            .unwrap_or_default();
        let (tls_ready, tls_error) = context.ingress_tls_status(id);
        if !tls_ready {
            problems.push(format!("ingress TLS: {tls_error}"));
        } else if !tls_error.is_empty() {
            // A rejected KV renewal while the previous certificate serves.
            problems.push(format!(
                "ingress TLS: serving the previous certificate; {tls_error}"
            ));
        }
        // Credential injection: a header with no value fails its upstream's
        // requests with a 503; one whose refresh failed still injects the
        // previous value.
        problems.extend(context.credential_problems(id));
        if network.ingress_tls.as_deref() == Some("legion") && context.mesh_identity().is_none() {
            problems.push("mesh certificate not issued yet".into());
        }
        out.push(crate::structs::NetAllocStatus {
            allocation_id: id,
            mode: network
                .mode
                .clone()
                .unwrap_or_else(|| crate::structs::netpolicy::MODE_HOST.to_string()),
            capture_ready,
            policy_hash,
            upstreams_bound: upstreams,
            ingress_bound: ingress,
            resolver_ready,
            proxy_ready,
            gate_shut: gated && crate::worker::netd::netns::gate_is_shut(id),
            problems,
            handlers: context
                .handler_statuses(id)
                .into_iter()
                .map(|h| crate::structs::NetHandlerStatus {
                    source: h.source,
                    handler: h.handler,
                    target: h.target,
                    healthy: h.healthy,
                    last_error: h.last_error.unwrap_or_default(),
                    calls: h.calls,
                    errors: h.errors,
                })
                .collect(),
            tls_ready,
            tls_error,
            mesh_identity: context.mesh_identity().is_some(),
            guest_address: lease
                .as_ref()
                .map(|lease| lease.address.to_string())
                .unwrap_or_default(),
            guest_bridge: lease.map(|lease| lease.bridge).unwrap_or_default(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::netd::listen::ListenAddr;
    use std::path::Path;
    use tempfile::TempDir;

    fn connect_allocation(allocation_id: i64, service: &str, local_port: u16) -> TaskAllocation {
        TaskAllocation {
            provisional: None,
            allocation_id,
            task_connect: json!({ "upstreams": [{ "service": service }] }).to_string(),
            network: json!({
                "ports": [{ "name": "http", "hostPort": 20000 }],
                "upstreams": [{
                    "service": service,
                    "localPort": local_port,
                    "balance": "least_conn",
                }],
            })
            .to_string(),
            ..TaskAllocation::default()
        }
    }

    /// Gated plan §6.5, calls entry 29: every gated sandbox gets a resolver
    /// on its vsock socket even with no upstreams (and so no VIP block),
    /// and never binds an address. A gated container still needs a block
    /// (its namespace resolver gap is tracked separately).
    #[test]
    fn every_gated_sandbox_gets_a_resolver_on_its_vsock_socket_test() {
        let alloc_root = std::path::Path::new("/var/lib/legion/allocs");
        let gated = |allocation_id: i64, runtime: &str| TaskAllocation {
            allocation_id,
            task_runtime: runtime.into(),
            network: json!({ "mode": "gated" }).to_string(),
            ..TaskAllocation::default()
        };
        let specs = dns_specs(&[gated(7, "sandbox"), gated(8, "container")], alloc_root);
        assert_eq!(specs.len(), 1, "{specs:?}");
        assert_eq!(specs[0].allocation_id, 7);
        assert_eq!(specs[0].resolver, None, "no address to bind");
        assert_eq!(specs[0].attachment.netns(), None);
        assert_eq!(
            specs[0].listen(),
            Some(ListenAddr::Unix(
                "/var/lib/legion/allocs/7/.sandbox/vsock.sock_53".into()
            ))
        );
    }

    #[test]
    fn upstream_specs_come_from_the_allocation_network() {
        let specs = upstream_specs(&[connect_allocation(11, "db", 20003)], Path::new("/allocs"));
        assert_eq!(
            specs,
            vec![UpstreamSpec {
                allocation_id: 11,
                service: "db".into(),
                local_port: 20003,
                balance: "least_conn".into(),
                to: None,
                http: None,
                handlers: Vec::new(),
                credentials: Vec::new(),
                vip: None,
                canonical_port: None,
                attachment: Attachment::Host,
            }]
        );
    }

    /// The node fetches exactly the headers its desired set declares, and the
    /// forwarder's upstream spec carries them so HTTP mode injects them.
    #[test]
    fn declared_credentials_follow_the_desired_set_test() {
        let connect = json!({ "upstreams": [
            { "service": "db" },
            { "name": "openai", "to": "https://api.openai.com", "credentials": [
                { "header": "authorization", "prefix": "Bearer ", "from": { "kv": "secrets/openai" } },
                { "header": "openai-organization", "from": { "kv": "secrets/org" } },
            ] },
        ] })
        .to_string();
        let with = TaskAllocation {
            provisional: None,
            allocation_id: 21,
            task_connect: connect,
            network: json!({ "upstreams": [
                { "service": "openai", "localPort": 20010, "balance": "round_robin", "to": "https://api.openai.com" },
            ] })
            .to_string(),
            ..TaskAllocation::default()
        };
        let without = connect_allocation(22, "db", 20011);
        assert_eq!(
            declared_credentials(&[with.clone(), without.clone()]),
            vec![
                (21, "openai".to_string(), "authorization".to_string()),
                (21, "openai".to_string(), "openai-organization".to_string()),
            ]
        );
        assert!(declared_credentials(&[without]).is_empty());
        let specs = upstream_specs(&[with], Path::new("/allocs"));
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].credentials.len(), 2);
        assert_eq!(specs[0].credentials[0].prefix.as_deref(), Some("Bearer "));
    }

    /// A gated sandbox's upstream listens on its VMM's vsock host socket for
    /// its local port, never in a namespace or on a VIP (the guest serves
    /// the VIP). A restored guest's boot record, which says the workload
    /// still dials the loopback port it was captured with, does not move
    /// it: the guest agent relays that port to this one.
    #[test]
    fn a_gated_sandboxs_upstreams_listen_on_its_vsock_socket() {
        let root = TempDir::new().unwrap();
        let mut allocation = connect_allocation(7, "db", 20003);
        allocation.task_runtime = "sandbox".into();
        allocation.task_config = json!({ "image": "a" }).to_string();
        allocation.network = json!({
            "mode": "gated",
            "upstreams": [{
                "service": "db",
                "localPort": 20003,
                "balance": "least_conn",
                "vip": "127.42.0.2",
                "canonicalPort": 5432,
            }],
        })
        .to_string();
        let scratch = crate::task_plugins::sandbox_dir_for(&root.path().join("7"));
        let socket = |port: u16| {
            crate::worker::netd::guest_agent::host_socket_path(
                &crate::task_plugins::vsock_socket_base(&scratch),
                u32::from(port),
            )
        };
        let specs = upstream_specs(std::slice::from_ref(&allocation), root.path());
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].listen(), ListenAddr::Unix(socket(20003)));
        assert!(socket(20003).ends_with(".sandbox/vsock.sock_20003"));
        assert_eq!(
            (
                specs[0].attachment.netns(),
                &specs[0].vip,
                specs[0].canonical_port
            ),
            (None, &None, None)
        );

        std::fs::create_dir_all(&scratch).unwrap();
        crate::task_plugins::write_guest_upstreams_record(&scratch, &[("db", 20009)]).unwrap();
        let restored = upstream_specs(&[allocation], root.path());
        assert_eq!(restored, specs);
    }

    /// A gated sandbox's capture includes a vsock socket per upstream:
    /// `net doctor` names each unbound one, and binding it clears that
    /// problem (the proxy and the resolver, not bound here, still hold
    /// `capture_ready` down).
    #[tokio::test]
    async fn net_status_names_a_gated_sandboxs_unbound_upstream_socket() {
        let root = TempDir::new().unwrap();
        let mut allocation = connect_allocation(7, "db", 20003);
        allocation.task_runtime = "sandbox".into();
        allocation.task_config = json!({ "image": "a" }).to_string();
        allocation.network = json!({
            "mode": "gated",
            "upstreams": [{ "service": "db", "localPort": 20003, "balance": "least_conn" }],
        })
        .to_string();
        let desired = vec![allocation];
        let forwarder = UpstreamForwarder::new();
        let socket = crate::worker::netd::guest_agent::host_socket_path(
            &crate::task_plugins::vsock_socket_base_for(&root.path().join("7")),
            20003,
        );
        let unbound = format!(
            "upstream db is not bound on the guest's vsock socket {}",
            socket.display()
        );

        let status = net_status(&forwarder, &desired, root.path());
        assert_eq!(status.len(), 1);
        assert!(!status[0].capture_ready);
        assert!(
            status[0].problems.contains(&unbound),
            "{:?}",
            status[0].problems
        );

        forwarder.sync(&upstream_specs(&desired, root.path())).await;
        assert!(socket.exists());
        let status = net_status(&forwarder, &desired, root.path());
        assert!(!status[0].problems.contains(&unbound));
        assert!(!status[0]
            .problems
            .iter()
            .any(|problem| problem.contains("upstream listeners bound")));
        assert!(!status[0].capture_ready, "the proxy is not bound here");
    }

    /// Gated plan §6.10: how the ingress listener reaches each task. A task
    /// with an ingress chain is dialed on its private twin; a gated sandbox
    /// over its VMM's vsock and a TAP sandbox at its leased address, chain
    /// or not; a loopback-only sandbox and a chainless task are not fronted.
    #[test]
    fn ingress_specs_pick_how_each_task_is_reached() {
        use crate::worker::netd::ingress::TaskDial;
        let root = std::path::Path::new("/var/lib/legion/allocs");
        let port = |host: u16, private: Option<u16>| {
            let mut port = json!({ "name": "http", "hostPort": host });
            if let Some(private) = private {
                port["privatePort"] = json!(private);
            }
            port
        };
        let allocation = |id: i64, runtime: &str, network: Value, config: Value| TaskAllocation {
            provisional: None,
            allocation_id: id,
            task_runtime: runtime.into(),
            network: network.to_string(),
            task_config: config.to_string(),
            ..TaskAllocation::default()
        };
        let desired = vec![
            allocation(
                1,
                "exec",
                json!({ "ports": [port(20000, Some(20001))] }),
                json!({}),
            ),
            allocation(
                2,
                "exec",
                json!({ "ports": [port(20002, None)] }),
                json!({}),
            ),
            allocation(
                3,
                "sandbox",
                json!({ "mode": "gated", "ports": [port(20004, None)] }),
                json!({ "image": "a" }),
            ),
            allocation(
                4,
                "sandbox",
                json!({ "ports": [port(20006, Some(20007))] }),
                json!({ "image": "a", "network": { "mode": "tap", "bridge": "br0" } }),
            ),
            allocation(
                5,
                "sandbox",
                json!({ "ports": [port(20008, None)] }),
                json!({ "image": "a" }),
            ),
        ];
        let specs = ingress_specs(&desired, root);
        let reached: Vec<(i64, u16, u16, TaskDial)> = specs
            .into_iter()
            .map(|spec| {
                (
                    spec.allocation_id,
                    spec.host_port,
                    spec.private_port,
                    spec.dial,
                )
            })
            .collect();
        let scratch = |id: i64| crate::task_plugins::sandbox_dir_for(&root.join(id.to_string()));
        assert_eq!(
            reached,
            vec![
                (1, 20000, 20001, TaskDial::Loopback { netns: None }),
                // No twin: the guest binds the advertised number itself,
                // and the vsock port is that number.
                (
                    3,
                    20004,
                    20004,
                    TaskDial::Vsock {
                        scratch: scratch(3)
                    }
                ),
                (
                    4,
                    20006,
                    20007,
                    TaskDial::Bridge {
                        scratch: scratch(4)
                    }
                ),
            ]
        );
    }

    #[test]
    fn upstream_specs_skip_allocations_without_connect() {
        let plain = TaskAllocation {
            provisional: None,
            allocation_id: 1,
            network: json!({ "ports": [{ "name": "http", "hostPort": 20000 }] }).to_string(),
            ..TaskAllocation::default()
        };
        // Declared upstreams but no network yet (or a corrupt payload) cannot
        // produce a listener — there is no port to bind.
        let unplaced = TaskAllocation {
            provisional: None,
            allocation_id: 2,
            task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
            ..TaskAllocation::default()
        };
        let corrupt = TaskAllocation {
            provisional: None,
            allocation_id: 3,
            task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
            network: "{not json".into(),
            ..TaskAllocation::default()
        };
        assert!(upstream_specs(&[plain, unplaced, corrupt], Path::new("/allocs")).is_empty());
    }

    #[test]
    fn dialable_endpoints_drop_entries_the_worker_cannot_dial() {
        let update = UpstreamEndpoints {
            allocation_id: 11,
            for_handler: false,
            service: "db".into(),
            endpoints: vec![
                crate::structs::ServiceEndpoint {
                    address: "10.0.0.5".into(),
                    port: 20001,
                    allocation_id: 42,
                    node_id: "node-b".into(),
                    // The catalog marks a mesh endpoint; the forwarder must
                    // originate mesh TLS to it, so the flag survives.
                    mesh_tls: true,
                    ..crate::structs::ServiceEndpoint::default()
                },
                // Node advertises no address yet.
                crate::structs::ServiceEndpoint {
                    address: String::new(),
                    port: 20002,
                    ..crate::structs::ServiceEndpoint::default()
                },
                // No port assigned to the advertised label.
                crate::structs::ServiceEndpoint {
                    address: "10.0.0.6".into(),
                    port: 0,
                    ..crate::structs::ServiceEndpoint::default()
                },
            ],
        };

        assert_eq!(
            dialable_endpoints(&update),
            vec![Endpoint {
                addr: "10.0.0.5:20001".into(),
                allocation_id: 42,
                node_id: "node-b".into(),
                tags: Vec::new(),
                mesh_tls: true,
            }]
        );
    }
}
