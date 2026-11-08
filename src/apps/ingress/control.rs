//! What the ingress asks of Legion, behind one trait: its routes
//! (`WatchAppExtensions`), each routed service's endpoints
//! (`ResolveService`), its own KV and lock, and the cert manager's objects
//! (`certs/Issuer` and `certs/Certificate`) it asks for certificates with.
//! [`GrpcControl`] is the real one, over the app's `legion` identity; tests
//! drive the proxy with a fake.

use std::time::Duration;

use futures::stream::BoxStream;
use futures::StreamExt;

use super::backend::CatalogEndpoint;
use super::certs::KvVersion;
use crate::prelude::*;
use crate::structs::{
    AcquireKvLockRequest, AppExtensionEvent, AppObject, ApplyObjectRequest, DeleteKvRequest,
    DeleteObjectRequest, GetKvRequest, GetObjectRequest, KvValueType, ListKvRequest,
    ListObjectsRequest, PutKvRequest, ReleaseKvLockRequest, ResolveServiceRequest,
    WatchAppExtensionsRequest,
};

/// An app object as the ingress reads it: who it is, what was asked, and
/// what its app last reported.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectView {
    pub name: String,
    pub namespace: String,
    pub labels: std::collections::BTreeMap<String, String>,
    /// The spec as written.
    pub spec: Value,
    /// The status's free-form fields (`{}` before the app reports).
    pub fields: Value,
    /// The `Ready` condition: (`true`/`false`/..., reason, message).
    pub ready: Option<(String, String, String)>,
    /// Whether the owning app has reported any status yet.
    pub reported: bool,
}

impl ObjectView {
    /// Read an object from its wire form.
    pub fn from_proto(object: &AppObject) -> ObjectView {
        let status = object.status.as_ref();
        ObjectView {
            name: object.name.clone(),
            namespace: object.namespace.clone(),
            labels: object
                .labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            spec: serde_json::from_str(&object.spec_json).unwrap_or(Value::Null),
            fields: status
                .and_then(|status| serde_json::from_str(&status.fields_json).ok())
                .unwrap_or_else(|| json!({})),
            ready: status.and_then(|status| {
                status
                    .conditions
                    .iter()
                    .find(|condition| condition.r#type == "Ready")
                    .map(|condition| {
                        (
                            condition.status.clone(),
                            condition.reason.clone(),
                            condition.message.clone(),
                        )
                    })
            }),
            reported: status.is_some_and(|status| status.updated_at_unix_ms > 0),
        }
    }
}

/// Everything the ingress needs from Legion.
#[async_trait::async_trait]
pub trait Control: Send + Sync + 'static {
    /// Open the route watch: a snapshot, then upserts and deletes.
    async fn watch_routes(&self) -> R<BoxStream<'static, R<AppExtensionEvent>>>;
    /// Every endpoint the catalog lists for `service` (`name` or
    /// `namespace/name`), healthy or not.
    async fn resolve(&self, service: &str) -> R<Vec<(String, CatalogEndpoint)>>;
    /// Every key under `prefix`, with its version.
    async fn kv_list(&self, prefix: &str) -> R<Vec<KvVersion>>;
    /// A key's value and version.
    async fn kv_get(&self, key: &str) -> R<Option<(String, i64)>>;
    async fn kv_put(&self, key: &str, value: &str) -> R<()>;
    async fn kv_delete(&self, key: &str) -> R<()>;
    /// Take or renew lock `key` as `lock_id`. `Ok(false)` when another
    /// holder has it.
    async fn lock(&self, key: &str, lock_id: &str, owner: &str, ttl: Duration) -> R<bool>;
    async fn unlock(&self, key: &str, lock_id: &str);
    /// Create or update an object from its document (`{ kind, namespace,
    /// name, labels, spec }`).
    async fn apply_object(&self, document: &Value) -> R<()>;
    /// One object, or `None` when there is no such object.
    async fn get_object(&self, kind: &str, name: &str, namespace: &str) -> R<Option<ObjectView>>;
    /// The objects of `kind` in `namespace` carrying every label in `labels`.
    async fn list_objects(
        &self,
        kind: &str,
        namespace: &str,
        labels: &std::collections::BTreeMap<String, String>,
    ) -> R<Vec<ObjectView>>;
    /// Delete an object; one that is already gone is not an error.
    async fn delete_object(&self, kind: &str, name: &str, namespace: &str) -> R<()>;
}

/// [`Control`] over the app's Legion client.
#[derive(Clone)]
pub struct GrpcControl {
    client: crate::cmd::helpers::AuthedManagerClient,
}

impl GrpcControl {
    pub fn new(client: crate::cmd::helpers::AuthedManagerClient) -> GrpcControl {
        GrpcControl { client }
    }
}

fn status_error(what: &str, status: tonic::Status) -> anyhow::Error {
    anyhow!("{what}: {}", status.message())
}

#[async_trait::async_trait]
impl Control for GrpcControl {
    async fn watch_routes(&self) -> R<BoxStream<'static, R<AppExtensionEvent>>> {
        let stream = self
            .client
            .clone()
            .watch_app_extensions(WatchAppExtensionsRequest::default())
            .await
            .map_err(|status| status_error("watching the app's routes", status))?
            .into_inner();
        Ok(stream
            .map(|event| event.map_err(|status| status_error("the route watch", status)))
            .boxed())
    }

    async fn resolve(&self, service: &str) -> R<Vec<(String, CatalogEndpoint)>> {
        let reply = self
            .client
            .clone()
            .resolve_service(ResolveServiceRequest {
                service: service.to_string(),
                include_unhealthy: true,
            })
            .await
            .map_err(|status| status_error(&format!("resolving {service}"), status))?
            .into_inner();
        Ok(reply
            .endpoints
            .into_iter()
            .map(|endpoint| {
                (
                    format!("{}\u{0}{}", endpoint.job_id, endpoint.task_name),
                    CatalogEndpoint {
                        host: endpoint.address,
                        port: u16::try_from(endpoint.port).unwrap_or(0),
                        allocation_id: endpoint.allocation_id,
                        node_id: endpoint.node_id,
                        healthy: endpoint.healthy,
                        mesh_tls: endpoint.mesh_tls,
                        public_address: endpoint.public_address,
                    },
                )
            })
            .collect())
    }

    async fn kv_list(&self, prefix: &str) -> R<Vec<KvVersion>> {
        let reply = self
            .client
            .clone()
            .list_kv(ListKvRequest {
                prefix: prefix.to_string(),
                include_values: false,
                include_locks: false,
            })
            .await
            .map_err(|status| status_error("listing the app's KV", status))?
            .into_inner();
        Ok(reply
            .entries
            .into_iter()
            .map(|entry| KvVersion {
                key: entry.key,
                version: entry.version,
            })
            .collect())
    }

    async fn kv_get(&self, key: &str) -> R<Option<(String, i64)>> {
        match self
            .client
            .clone()
            .get_kv(GetKvRequest {
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(reply) => Ok(reply
                .into_inner()
                .entry
                .map(|entry| (entry.value, entry.version))),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status_error(&format!("reading {key}"), status)),
        }
    }

    async fn kv_put(&self, key: &str, value: &str) -> R<()> {
        self.client
            .clone()
            .put_kv(PutKvRequest {
                key: key.to_string(),
                value: value.to_string(),
                value_type: KvValueType::String as i32,
                ..Default::default()
            })
            .await
            .map_err(|status| status_error(&format!("writing {key}"), status))?;
        Ok(())
    }

    async fn kv_delete(&self, key: &str) -> R<()> {
        match self
            .client
            .clone()
            .delete_kv(DeleteKvRequest {
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(status_error(&format!("deleting {key}"), status)),
        }
    }

    async fn lock(&self, key: &str, lock_id: &str, owner: &str, ttl: Duration) -> R<bool> {
        match self
            .client
            .clone()
            .acquire_kv_lock(AcquireKvLockRequest {
                key: key.to_string(),
                lock_id: Some(lock_id.to_string()),
                owner: Some(owner.to_string()),
                ttl_seconds: ttl.as_secs().max(1) as i64,
                force: false,
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::AlreadyExists
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::Aborted
                ) =>
            {
                Ok(false)
            }
            Err(status) => Err(status_error("taking the issuer lock", status)),
        }
    }

    async fn unlock(&self, key: &str, lock_id: &str) {
        if let Err(status) = self
            .client
            .clone()
            .release_kv_lock(ReleaseKvLockRequest {
                key: key.to_string(),
                lock_id: lock_id.to_string(),
            })
            .await
        {
            debug!(key, "Releasing the certificates lock: {}", status.message());
        }
    }

    async fn apply_object(&self, document: &Value) -> R<()> {
        let namespace = document["namespace"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.client
            .clone()
            .apply_object(ApplyObjectRequest {
                object_json: document.to_string(),
                namespace,
                ..Default::default()
            })
            .await
            .map_err(|status| {
                status_error(
                    &format!(
                        "applying {} {}",
                        document["kind"].as_str().unwrap_or("object"),
                        document["name"].as_str().unwrap_or_default()
                    ),
                    status,
                )
            })?;
        Ok(())
    }

    async fn get_object(&self, kind: &str, name: &str, namespace: &str) -> R<Option<ObjectView>> {
        match self
            .client
            .clone()
            .get_object(GetObjectRequest {
                kind: kind.to_string(),
                name: name.to_string(),
                namespace: namespace.to_string(),
            })
            .await
        {
            Ok(reply) => Ok(reply
                .into_inner()
                .object
                .as_ref()
                .map(ObjectView::from_proto)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status_error(&format!("reading {kind} {name}"), status)),
        }
    }

    async fn list_objects(
        &self,
        kind: &str,
        namespace: &str,
        labels: &std::collections::BTreeMap<String, String>,
    ) -> R<Vec<ObjectView>> {
        let reply = self
            .client
            .clone()
            .list_objects(ListObjectsRequest {
                kind: kind.to_string(),
                namespaces: vec![namespace.to_string()],
                labels: labels
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                ..Default::default()
            })
            .await
            .map_err(|status| status_error(&format!("listing {kind}"), status))?
            .into_inner();
        Ok(reply.objects.iter().map(ObjectView::from_proto).collect())
    }

    async fn delete_object(&self, kind: &str, name: &str, namespace: &str) -> R<()> {
        match self
            .client
            .clone()
            .delete_object(DeleteObjectRequest {
                kind: kind.to_string(),
                name: name.to_string(),
                namespace: namespace.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(status_error(&format!("deleting {kind} {name}"), status)),
        }
    }
}
