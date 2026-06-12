//! Worker-side [`CredentialSource`]: resolves `auth.kv` references through
//! the manager's `GetArtifactCredential` RPC at fetch time (artifact system
//! plan §6). Credentials are used immediately and never persisted or logged.
//!
//! Identity-based auth (`auth.identity` / `auth.nodeIdentity`) inherits the
//! trait's default error until workload-identity token issuance and worker
//! delivery land (WI plan phases 2 and 4).

use crate::artifacts::{Credential, CredentialSource};
use crate::prelude::*;
use crate::structs::{manager_client::ManagerClient, GetArtifactCredentialRequest};

/// Fetch-time credential resolver backed by the worker's manager channel.
/// Calls run on staging threads (blocking), so each lookup connects on a
/// private current-thread runtime.
pub struct ManagerCredentialSource {
    manager_addr: String,
    node_id: String,
}

impl ManagerCredentialSource {
    pub fn new(manager_addr: String, node_id: String) -> Self {
        Self {
            manager_addr,
            node_id,
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
        let manager_addr = self.manager_addr.clone();
        let request = GetArtifactCredentialRequest {
            node_id: self.node_id.clone(),
            kv_path: kv_path.to_string(),
            source: redacted_source.to_string(),
        };
        let credential_json = runtime.block_on(async move {
            let channel = tonic::transport::Channel::from_shared(manager_addr)
                .context("manager address")?
                .connect()
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
}
