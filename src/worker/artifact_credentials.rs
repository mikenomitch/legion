//! Worker-side [`CredentialSource`]: resolves `auth.kv` references through
//! the manager's `GetArtifactCredential` RPC at fetch time (artifact system
//! plan §6). Credentials are used immediately and never persisted or logged.
//!
//! `auth.nodeIdentity` is resolved through the manager's
//! `IssueNodeIdentityToken` RPC (a fresh cluster-grain OIDC token per fetch).
//! Task-scoped `auth.identity` still inherits the trait's default error: it
//! needs the fetching allocation's id threaded through to the token request,
//! which is not wired yet.

use crate::artifacts::{Credential, CredentialSource};
use crate::prelude::*;
use crate::structs::{
    manager_client::ManagerClient, GetArtifactCredentialRequest, IssueNodeIdentityTokenRequest,
};

/// Fetch-time credential resolver backed by the worker's manager set.
/// Calls run on staging threads (blocking), so each lookup connects on a
/// private current-thread runtime — to whichever manager the worker is
/// currently talking to, so a fetch follows a manager failover.
pub struct ManagerCredentialSource {
    managers: std::sync::Arc<crate::worker::managers::ManagerEndpoints>,
    node_id: String,
    /// Worker-plane credentials; credential fetches require the node token
    /// when ACLs are enforced.
    credentials: std::sync::Arc<crate::worker::daemon::WorkerCredentials>,
}

impl ManagerCredentialSource {
    pub fn new(
        managers: std::sync::Arc<crate::worker::managers::ManagerEndpoints>,
        node_id: String,
        credentials: std::sync::Arc<crate::worker::daemon::WorkerCredentials>,
    ) -> Self {
        Self {
            managers,
            node_id,
            credentials,
        }
    }
}

impl CredentialSource for ManagerCredentialSource {
    #[instrument(skip(self), fields(node_id = %self.node_id))]
    fn kv_credential(&self, kv_path: &str, redacted_source: &str) -> R<Credential> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build credential fetch runtime")?;
        let manager_addr = self.managers.active();
        let request = self
            .credentials
            .attach(tonic::Request::new(GetArtifactCredentialRequest {
                node_id: self.node_id.clone(),
                kv_path: kv_path.to_string(),
                source: redacted_source.to_string(),
            }));
        let credential_json = runtime.block_on(async move {
            let channel = crate::worker::plane::manager_channel(&manager_addr)
                .await
                .context("connect to manager for artifact credential")?;
            let mut client = ManagerClient::new(channel);
            let reply = client
                .get_artifact_credential(request)
                .await
                .map_err(|status| {
                    anyhow!(
                        "GetArtifactCredential failed: {} ({})",
                        status.message(),
                        status.code()
                    )
                })?
                .into_inner();
            Ok::<String, anyhow::Error>(reply.credential_json)
        })?;
        Credential::from_json(&credential_json)
    }

    #[instrument(skip(self), fields(node_id = %self.node_id))]
    fn node_identity_token(&self, name: &str) -> R<String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build node-identity fetch runtime")?;
        let manager_addr = self.managers.active();
        let request = self
            .credentials
            .attach(tonic::Request::new(IssueNodeIdentityTokenRequest {
                node_id: self.node_id.clone(),
                identity_name: name.to_string(),
            }));
        // Fetched fresh per artifact fetch; the token is used immediately for a
        // single provider exchange and never persisted (never logged).
        let token = runtime.block_on(async move {
            let channel = crate::worker::plane::manager_channel(&manager_addr)
                .await
                .context("connect to manager for node identity token")?;
            let mut client = ManagerClient::new(channel);
            let reply = client
                .issue_node_identity_token(request)
                .await
                .map_err(|status| {
                    anyhow!(
                        "IssueNodeIdentityToken failed: {} ({})",
                        status.message(),
                        status.code()
                    )
                })?
                .into_inner();
            Ok::<String, anyhow::Error>(reply.token)
        })?;
        Ok(token)
    }
}
