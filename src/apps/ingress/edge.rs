//! Where to point DNS (docs/ingress.md, "Point DNS at it").
//!
//! The ingress's own job declares a catalog service, `legion-system/ingress`,
//! so its replicas are listed like any service's endpoints: each node's
//! cluster address, and its public address when the worker was started
//! with `LEGION_WORKER_PUBLIC_ADDRESS`. The app reads that list for the
//! "Point DNS here" block of its page, and checks every routed host's DNS
//! against it, so a route whose name points somewhere else says so next to
//! the route.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::Duration;

use super::backend::CatalogEndpoint;

/// The ingress's own service, which the catalog lists its replicas under.
pub const SERVICE: &str = "legion-system/ingress";
/// How long one host's lookup may take.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

/// One replica: the node it runs on, and where clients reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeAddress {
    pub node_id: String,
    /// The node's cluster address.
    pub address: String,
    /// The node's public address, when its worker has one
    /// (`LEGION_WORKER_PUBLIC_ADDRESS`).
    pub public_address: Option<String>,
}

impl EdgeAddress {
    /// What DNS should point at: the public address, else the cluster one.
    pub fn target(&self) -> &str {
        self.public_address.as_deref().unwrap_or(&self.address)
    }
}

/// The healthy replicas in the catalog's endpoints for [`SERVICE`].
pub fn edge_addresses(endpoints: &[(String, CatalogEndpoint)]) -> Vec<EdgeAddress> {
    let mut seen = BTreeSet::new();
    let mut addresses = Vec::new();
    for (_, endpoint) in endpoints {
        if !endpoint.healthy || endpoint.host.is_empty() {
            continue;
        }
        if seen.insert(endpoint.node_id.clone()) {
            addresses.push(EdgeAddress {
                node_id: endpoint.node_id.clone(),
                address: endpoint.host.clone(),
                public_address: Some(endpoint.public_address.clone())
                    .filter(|address| !address.is_empty()),
            });
        }
    }
    addresses.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    addresses
}

/// What a host's DNS says, as this node resolves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsCheck {
    /// It resolves to at least one ingress replica.
    Here,
    /// It resolves only to addresses no replica has.
    Elsewhere(Vec<IpAddr>),
    /// It does not resolve.
    Missing,
}

/// Every address the replicas are reached at, cluster and public, with
/// public names resolved.
pub async fn edge_ips(edge: &[EdgeAddress]) -> BTreeSet<IpAddr> {
    let mut ips = BTreeSet::new();
    for replica in edge {
        for address in
            std::iter::once(replica.address.as_str()).chain(replica.public_address.as_deref())
        {
            match address.parse::<IpAddr>() {
                Ok(ip) => {
                    ips.insert(ip);
                }
                Err(_) => {
                    for ip in resolve(address).await {
                        ips.insert(ip);
                    }
                }
            }
        }
    }
    ips
}

/// The addresses `host` resolves to here, or none.
pub async fn resolve(host: &str) -> Vec<IpAddr> {
    match tokio::time::timeout(LOOKUP_TIMEOUT, tokio::net::lookup_host((host, 0))).await {
        Ok(Ok(addresses)) => {
            let mut ips: Vec<IpAddr> = addresses.map(|address| address.ip()).collect();
            ips.sort();
            ips.dedup();
            ips
        }
        _ => Vec::new(),
    }
}

/// Judge `resolved` (a host's addresses) against the replicas' `edge` ones.
pub fn judge(resolved: &[IpAddr], edge: &BTreeSet<IpAddr>) -> DnsCheck {
    if resolved.is_empty() {
        DnsCheck::Missing
    } else if resolved.iter().any(|ip| edge.contains(ip)) {
        DnsCheck::Here
    } else {
        DnsCheck::Elsewhere(resolved.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(node: &str, host: &str, public: &str, healthy: bool) -> (String, CatalogEndpoint) {
        (
            "legion-system/ingress\u{0}proxy".into(),
            CatalogEndpoint {
                host: host.into(),
                port: 80,
                allocation_id: 1,
                node_id: node.into(),
                healthy,
                mesh_tls: false,
                public_address: public.into(),
            },
        )
    }

    #[test]
    fn replicas_are_listed_once_with_their_public_address_first() {
        let edge = edge_addresses(&[
            endpoint("mgr-2", "10.0.0.2", "", true),
            endpoint("mgr-1", "10.0.0.1", "203.0.113.7", true),
            endpoint("mgr-1", "10.0.0.1", "203.0.113.7", true),
            endpoint("mgr-3", "10.0.0.3", "", false),
        ]);
        assert_eq!(edge.len(), 2, "one per node, healthy only");
        assert_eq!(edge[0].node_id, "mgr-1");
        assert_eq!(edge[0].target(), "203.0.113.7");
        assert_eq!(
            edge[1].target(),
            "10.0.0.2",
            "no public address: the cluster one"
        );
    }

    #[tokio::test]
    async fn a_host_points_here_elsewhere_or_nowhere() {
        let edge = edge_addresses(&[endpoint("mgr-1", "10.0.0.1", "203.0.113.7", true)]);
        let ips = edge_ips(&edge).await;
        assert!(ips.contains(&"203.0.113.7".parse().unwrap()));
        assert!(ips.contains(&"10.0.0.1".parse().unwrap()));
        assert_eq!(
            judge(&["203.0.113.7".parse().unwrap()], &ips),
            DnsCheck::Here
        );
        assert_eq!(
            judge(&["198.51.100.4".parse().unwrap()], &ips),
            DnsCheck::Elsewhere(vec!["198.51.100.4".parse().unwrap()])
        );
        assert_eq!(judge(&[], &ips), DnsCheck::Missing);
        // A real lookup: `localhost` resolves without the network.
        assert!(!resolve("localhost").await.is_empty());
    }
}
