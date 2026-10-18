//! FetchDatapathMaterial: everything a node's datapath fetches from the
//! manager, in one call (mesh interfaces plan §6.S5; docs/network-handlers.md).
//!
//! A ref names a kind of material and what it is for (an allocation, an
//! upstream key, a header), never where it lives. Resolving one is two steps:
//!
//! 1. **The declaration.** The manager finds the [`Source`] the allocation
//!    itself declares for that ref (its ports' `tls`, as placed, or its
//!    task's `connect` block), after checking the allocation is placed on
//!    the calling node. A ref nothing on the node declares gets an error.
//! 2. **The provider.** One [`MaterialProvider`] per kind of source reads the
//!    value: `kv`, `identity`, and `policy` (a body by content hash). A new
//!    secret store is one provider plus one `Source` variant, with no new RPC
//!    and no netd change.
//!
//! Values are secrets: they are never logged, only their sources.

use super::*;
use crate::structs::netpolicy::EffectivePolicy;
use crate::structs::source::{
    Source, SourceKind, MATERIAL_CLIENT_CA, MATERIAL_CREDENTIAL, MATERIAL_POLICY,
    MATERIAL_PORT_CERT, MATERIAL_UPSTREAM_CA,
};
use crate::structs::{
    FetchDatapathMaterialReply, FetchDatapathMaterialRequest, Material, MaterialRef, TaskAllocation,
};

/// What a provider read for one ref.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProviderRead {
    /// The current value and its version.
    Value { value: String, version: i64 },
    /// The version the node holds is current.
    Unchanged { version: i64 },
    /// Authoritatively gone (no such key, no such identity): the node drops
    /// what it holds.
    Absent(String),
    /// A transient failure: the node keeps its last good value.
    Failed(String),
}

/// One ref, once its declaration has been resolved.
pub(crate) struct ProviderInput<'a> {
    pub allocation_id: i64,
    /// The KV path, identity name, or policy hash.
    pub name: &'a str,
    /// The version the node holds (0 for none).
    pub have_version: i64,
}

/// Reads one kind of source for the manager.
pub(crate) trait MaterialProvider: Sync {
    /// The source kind this provider reads, as the jobspec names it (`kv`,
    /// `identity`), or `policy` for content-addressed policy bodies.
    fn name(&self) -> &'static str;

    fn read(&self, service: &ManagerDaemonService, input: &ProviderInput<'_>) -> ProviderRead;
}

/// Every provider. A `Source` kind the node fetches (not `file`) must have
/// one here; `providers_cover_every_fetched_source_test` checks it.
pub(crate) const PROVIDERS: &[&dyn MaterialProvider] =
    &[&KvProvider, &IdentityProvider, &PolicyProvider];

pub(crate) fn provider(name: &str) -> Option<&'static dyn MaterialProvider> {
    PROVIDERS.iter().copied().find(|p| p.name() == name)
}

fn source_provider_name(source: &Source) -> &'static str {
    match source.kind() {
        SourceKind::Kv => "kv",
        SourceKind::Identity => "identity",
        SourceKind::File => "file",
    }
}

/// `kv`: a KV entry, versioned by its KV version.
pub(crate) struct KvProvider;

impl MaterialProvider for KvProvider {
    fn name(&self) -> &'static str {
        "kv"
    }

    fn read(&self, service: &ManagerDaemonService, input: &ProviderInput<'_>) -> ProviderRead {
        let path = input.name;
        match service.store.get_kv(path) {
            Ok(Some(entry)) if entry.version == input.have_version => ProviderRead::Unchanged {
                version: entry.version,
            },
            Ok(Some(entry)) => ProviderRead::Value {
                value: entry.value,
                version: entry.version,
            },
            Ok(None) => ProviderRead::Absent(format!("kv {path} not found")),
            Err(err) => {
                warn!(kv_path = %path, error = ?err, "Failed reading datapath material from KV");
                ProviderRead::Failed(format!("kv {path} could not be read"))
            }
        }
    }
}

/// `identity`: the allocation's task identity of that name, minted exactly
/// as `IssueWorkloadIdentityToken` would. The version is the token's expiry
/// in unix seconds; the token the node holds stays until less than half the
/// identity's TTL remains, so a steady node mints about twice per TTL.
pub(crate) struct IdentityProvider;

impl MaterialProvider for IdentityProvider {
    fn name(&self) -> &'static str {
        "identity"
    }

    fn read(&self, service: &ManagerDaemonService, input: &ProviderInput<'_>) -> ProviderRead {
        service.mint_material_identity(input)
    }
}

/// `policy`: a merged policy body by its content hash, as JSON. A hash names
/// its body forever, so a body is never `Unchanged`; the node only asks for
/// hashes it holds no body for.
pub(crate) struct PolicyProvider;

impl MaterialProvider for PolicyProvider {
    fn name(&self) -> &'static str {
        "policy"
    }

    fn read(&self, service: &ManagerDaemonService, input: &ProviderInput<'_>) -> ProviderRead {
        match service.cached_policy(input.name) {
            Some(policy) => ProviderRead::Value {
                value: serde_json::to_string(&policy).unwrap_or_default(),
                version: 0,
            },
            None => ProviderRead::Absent(format!("no policy body for hash {}", input.name)),
        }
    }
}

/// How a resolved value is checked before it goes on the wire.
#[derive(Clone, Copy)]
enum ValueCheck {
    /// Passed through as read.
    Raw,
    /// A certificate chain and private key, validated so a bad bundle is
    /// reported here instead of failing handshakes on the node.
    PemBundle,
    /// A CA bundle: at least one certificate.
    CaBundle,
    /// An HTTP header value: one trailing line ending (a value put from a
    /// file) is trimmed; an empty value, or one with CR, LF, or NUL, which
    /// would split the request, is refused.
    HeaderValue,
}

impl ValueCheck {
    /// `source` names where the value came from, for the error.
    fn apply(self, source: &str, value: String) -> Result<String, String> {
        match self {
            ValueCheck::Raw => Ok(value),
            ValueCheck::PemBundle => crate::worker::netd::tls::validate_bundle(&value)
                .map(|()| value)
                .map_err(|err| format!("{source} {err:#}")),
            ValueCheck::CaBundle => crate::worker::netd::tls::roots_from_pem(value.as_bytes())
                .map(|_| value)
                .map_err(|err| format!("{source} {err:#}")),
            ValueCheck::HeaderValue => {
                let trimmed = value.trim_end_matches(['\r', '\n']);
                if trimmed.is_empty() || trimmed.contains(['\r', '\n', '\0']) {
                    Err(format!(
                        "{source} is empty or holds a line break or NUL, which an HTTP header \
                         cannot carry"
                    ))
                } else {
                    Ok(trimmed.to_string())
                }
            }
        }
    }
}

/// The node's desired allocations' placed networks and connect blocks,
/// computed at most once per call and only when a ref needs them.
struct NodeView<'a> {
    service: &'a ManagerDaemonService,
    node_id: &'a str,
    loaded: Option<Result<NodeDeclarations, String>>,
}

struct NodeDeclarations {
    connects: HashMap<i64, crate::structs::ConnectConfig>,
    networks: HashMap<i64, crate::structs::AllocNetwork>,
}

impl NodeView<'_> {
    fn declarations(&mut self) -> Result<&NodeDeclarations, String> {
        if self.loaded.is_none() {
            let loaded = build_desired_allocations(&self.service.store, self.node_id, &[])
                .map(|desired| self.declare(&desired))
                .map_err(|err| {
                    warn!(node_id = %self.node_id, error = ?err, "Failed building desired allocations for material");
                    "the node's allocations could not be loaded".to_string()
                });
            self.loaded = Some(loaded);
        }
        match self.loaded.as_ref().expect("loaded above") {
            Ok(declarations) => Ok(declarations),
            Err(err) => Err(err.clone()),
        }
    }

    fn declare(&self, desired: &[TaskAllocation]) -> NodeDeclarations {
        // The same merge the heartbeat ran; it leaves each body in the cache
        // for a policy ref that missed it.
        let _ = self.service.build_net_policies(self.node_id, desired);
        let networks = desired
            .iter()
            .filter(|allocation| !allocation.network.trim().is_empty())
            .filter_map(|allocation| {
                serde_json::from_str(&allocation.network)
                    .ok()
                    .map(|network| (allocation.allocation_id, network))
            })
            .collect();
        let connects = desired
            .iter()
            .filter(|allocation| !allocation.task_connect.trim().is_empty())
            .filter_map(|allocation| {
                serde_json::from_str(&allocation.task_connect)
                    .ok()
                    .map(|connect| (allocation.allocation_id, connect))
            })
            .collect();
        NodeDeclarations { connects, networks }
    }
}

/// Why a ref resolved to nothing. `absent` refs are authoritative.
struct Undeclared {
    error: String,
    absent: bool,
}

impl Undeclared {
    fn absent(error: String) -> Self {
        Self {
            error,
            absent: true,
        }
    }
}

impl ManagerDaemonService {
    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id, refs = request.get_ref().refs.len()))]
    pub(super) async fn fetch_datapath_material_rpc(
        &self,
        request: Request<FetchDatapathMaterialRequest>,
    ) -> Result<Response<FetchDatapathMaterialReply>, Status> {
        // ACL: worker-plane endpoint (nodes fetch the material their own
        // allocations declare). The node never names a KV path or an
        // identity: each source is derived below from the allocation's own
        // declaration, which only a submitter holding `kv:read` on the path
        // could have written.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        acl::enforce_node_binding(&identity, &req.node_id)?;

        let mut view = NodeView {
            service: self,
            node_id: &req.node_id,
            loaded: None,
        };
        let materials = req
            .refs
            .into_iter()
            .map(|wanted| self.resolve_material(&mut view, wanted))
            .collect();
        Ok(Response::new(FetchDatapathMaterialReply { materials }))
    }

    fn resolve_material(&self, view: &mut NodeView<'_>, wanted: MaterialRef) -> Material {
        let mut out = Material {
            r#ref: Some(wanted.clone()),
            ..Default::default()
        };
        let declared = match self.declared_source(view, &wanted) {
            Ok(declared) => declared,
            Err(undeclared) => {
                out.error = undeclared.error;
                out.absent = undeclared.absent;
                return out;
            }
        };
        let (provider_name, name, check) = match &declared {
            Declared::Policy(hash) => {
                out.source = format!("policy {hash}");
                ("policy", hash.clone(), ValueCheck::Raw)
            }
            Declared::Source(source, check) => {
                out.source = source.to_string();
                let name = source
                    .kv_path()
                    .or(source.identity_name())
                    .unwrap_or_default();
                (source_provider_name(source), name.to_string(), *check)
            }
        };
        let Some(provider) = provider(provider_name) else {
            out.error = format!(
                "{} is read by the node, not fetched from the manager",
                out.source
            );
            return out;
        };
        let input = ProviderInput {
            allocation_id: wanted.allocation_id,
            name: &name,
            have_version: wanted.version,
        };
        let mut read = provider.read(self, &input);
        // A policy cache miss after a failover or restart: recompute from the
        // node's desired set, which is what produced the hash, and retry.
        if matches!(declared, Declared::Policy(_))
            && matches!(read, ProviderRead::Absent(_))
            && view.loaded.is_none()
        {
            let _ = view.declarations();
            read = provider.read(self, &input);
        }
        let label = out.source.clone();
        fill(&mut out, read, |value| check.apply(&label, value));
        out
    }

    /// The source an allocation on this node declares for `wanted`.
    fn declared_source(
        &self,
        view: &mut NodeView<'_>,
        wanted: &MaterialRef,
    ) -> Result<Declared, Undeclared> {
        match wanted.kind.as_str() {
            MATERIAL_POLICY => {
                let hash = wanted.key.trim();
                if hash.is_empty() {
                    return Err(Undeclared::absent("a policy ref needs its hash".into()));
                }
                Ok(Declared::Policy(hash.to_string()))
            }
            MATERIAL_PORT_CERT | MATERIAL_CLIENT_CA => {
                let declarations = view.declarations().map_err(|error| Undeclared {
                    error,
                    absent: false,
                })?;
                let tls = declarations
                    .networks
                    .get(&wanted.allocation_id)
                    .and_then(|network| network.port(&wanted.key))
                    .and_then(|port| port.tls.as_ref());
                let source = if wanted.kind == MATERIAL_PORT_CERT {
                    tls.map(|tls| tls.cert.clone())
                } else {
                    tls.and_then(|tls| tls.clients.as_ref())
                        .map(|clients| clients.ca.clone())
                };
                let source = source.filter(Source::is_fetched).ok_or_else(|| {
                    Undeclared::absent(format!(
                        "allocation is not placed on this node or port {} declares no KV-held \
                         {}",
                        wanted.key,
                        if wanted.kind == MATERIAL_PORT_CERT {
                            "tls.cert"
                        } else {
                            "tls.clients.ca"
                        }
                    ))
                })?;
                let check = if wanted.kind == MATERIAL_PORT_CERT {
                    ValueCheck::PemBundle
                } else {
                    ValueCheck::CaBundle
                };
                Ok(Declared::Source(source, check))
            }
            MATERIAL_UPSTREAM_CA => {
                let declarations = view.declarations().map_err(|error| Undeclared {
                    error,
                    absent: false,
                })?;
                let source = declarations
                    .connects
                    .get(&wanted.allocation_id)
                    .and_then(|connect| {
                        connect
                            .upstreams
                            .iter()
                            .find(|upstream| upstream.key() == wanted.key)
                    })
                    .and_then(|upstream| upstream.ca.clone())
                    .filter(Source::is_fetched)
                    .ok_or_else(|| {
                        Undeclared::absent(format!(
                            "allocation is not placed on this node or upstream {} declares no \
                             KV-held ca",
                            wanted.key
                        ))
                    })?;
                Ok(Declared::Source(source, ValueCheck::CaBundle))
            }
            MATERIAL_CREDENTIAL => {
                let header = wanted.part.trim().to_ascii_lowercase();
                let declarations = view.declarations().map_err(|error| Undeclared {
                    error,
                    absent: false,
                })?;
                let credential = declarations
                    .connects
                    .get(&wanted.allocation_id)
                    .and_then(|connect| {
                        connect
                            .upstreams
                            .iter()
                            .find(|upstream| upstream.key() == wanted.key)
                    })
                    .and_then(|upstream| {
                        upstream
                            .credentials
                            .iter()
                            .find(|credential| credential.header.eq_ignore_ascii_case(&header))
                    })
                    .ok_or_else(|| {
                        Undeclared::absent(format!(
                            "allocation is not placed on this node or upstream {} declares no \
                             credential for header {header}",
                            wanted.key
                        ))
                    })?;
                Ok(Declared::Source(
                    credential.from.clone(),
                    ValueCheck::HeaderValue,
                ))
            }
            other => Err(Undeclared {
                error: format!(
                    "unknown material kind {other:?} (this manager serves policy, port_cert, \
                     client_ca, upstream_ca, credential)"
                ),
                absent: false,
            }),
        }
    }

    /// The cached policy body for `hash`, if the heartbeat merge left one.
    pub(super) fn cached_policy(&self, hash: &str) -> Option<EffectivePolicy> {
        self.net_policy_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(hash)
            .cloned()
    }

    /// The `identity` provider. The caller has already checked the
    /// allocation is on the calling node and declares this identity.
    fn mint_material_identity(&self, input: &ProviderInput<'_>) -> ProviderRead {
        let identity_name = input.name;
        let Some(state) = self.workload_identity.as_ref() else {
            return ProviderRead::Failed(
                "workload identity is not enabled on this manager (set \
                 workloadIdentity.enabled: true in the manager config)"
                    .to_string(),
            );
        };
        let allocation = match self.store.get_allocation_by_id(input.allocation_id) {
            Ok(Some(allocation)) => allocation,
            Ok(None) => {
                return ProviderRead::Absent(format!(
                    "allocation {} not found",
                    input.allocation_id
                ))
            }
            Err(err) => {
                warn!(allocation_id = input.allocation_id, error = ?err, "Failed loading allocation for a datapath identity");
                return ProviderRead::Failed("allocation could not be loaded".to_string());
            }
        };
        let task = match self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)
        {
            Ok(tasks) => tasks
                .into_iter()
                .find(|(id, _)| *id == allocation.task_config_id)
                .map(|(_, task)| task),
            Err(err) => {
                warn!(allocation_id = input.allocation_id, error = ?err, "Failed loading task config for a datapath identity");
                None
            }
        };
        let Some(task) = task else {
            return ProviderRead::Failed("task config could not be loaded".to_string());
        };
        let Some(identity) = task
            .identities
            .iter()
            .find(|identity| identity.name == identity_name)
        else {
            return ProviderRead::Absent(format!(
                "task {} declares no identity named {identity_name:?}",
                task.name
            ));
        };

        let now = std::time::SystemTime::now();
        let now_secs = now
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or_default();
        let half_life = (state.token_lifetime(identity).as_secs() / 2) as i64;
        if input.have_version > 0 && input.have_version - now_secs > half_life {
            return ProviderRead::Unchanged {
                version: input.have_version,
            };
        }

        let claims = match claims_for_allocation(
            &self.store,
            &allocation,
            &task,
            DEF_CLUSTER_REGION,
        ) {
            Ok(claims) => claims,
            Err(err) => {
                error!(error = ?err, "Error deriving workload identity claims for the datapath");
                return ProviderRead::Failed(
                    "failed to derive workload identity claims".to_string(),
                );
            }
        };
        match state.issue(&claims, identity, now) {
            Ok(issued) => {
                // Lifecycle log only, never the token.
                info!(
                    allocation_id = input.allocation_id,
                    identity = %identity_name,
                    expires_at = issued.expires_at,
                    kid = %issued.kid,
                    "Issued workload identity token for the datapath"
                );
                ProviderRead::Value {
                    value: issued.token,
                    version: issued.expires_at as i64,
                }
            }
            Err(err)
                if crate::manager::workload_identity::SigningKeyUnavailable::in_chain(&err) =>
            {
                debug!(error = %err, "No signing key yet for a datapath workload identity token");
                ProviderRead::Failed(err.to_string())
            }
            Err(err) => {
                error!(error = ?err, "Error signing workload identity token for the datapath");
                ProviderRead::Failed("failed to sign workload identity token".to_string())
            }
        }
    }
}

/// What a ref's declaration names.
enum Declared {
    Policy(String),
    Source(Source, ValueCheck),
}

/// Copy a provider's read into the reply entry, checking a new value.
fn fill(
    out: &mut Material,
    read: ProviderRead,
    check: impl FnOnce(String) -> Result<String, String>,
) {
    match read {
        ProviderRead::Value { value, version } => {
            out.version = version;
            match check(value) {
                Ok(value) => out.value = value,
                Err(err) => out.error = err,
            }
        }
        ProviderRead::Unchanged { version } => {
            out.version = version;
            out.unchanged = true;
        }
        ProviderRead::Absent(err) => {
            out.error = err;
            out.absent = true;
        }
        ProviderRead::Failed(err) => out.error = err,
    }
}

#[cfg(test)]
mod material_rpcs_test {
    use super::*;

    #[test]
    fn providers_cover_every_fetched_source_test() {
        let mut names: Vec<_> = PROVIDERS.iter().map(|p| p.name()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "two providers share a name");
        for source in [
            Source::Kv("a".into()),
            Source::Identity("a".into()),
            Source::File(crate::structs::source::FileSource::Path("/a".into())),
        ] {
            let has = provider(source_provider_name(&source)).is_some();
            assert_eq!(
                has,
                source.is_fetched(),
                "{source}: a fetched source needs a provider, and a file source must not have one"
            );
        }
        assert!(provider("policy").is_some());
    }

    #[test]
    fn header_values_are_checked_test() {
        let source = "kv secrets/x";
        assert_eq!(
            ValueCheck::HeaderValue.apply(source, "abc\n".into()),
            Ok("abc".into())
        );
        assert!(ValueCheck::HeaderValue
            .apply(source, "a\r\nb".into())
            .is_err());
        assert!(ValueCheck::HeaderValue.apply(source, "\n".into()).is_err());
    }
}
