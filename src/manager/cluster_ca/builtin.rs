//! `builtin`: the cluster CA Legion generates and keeps itself (the
//! default). Exactly the behavior before providers existed: generated on
//! first use by the control-lane leader, stored (key included) in the
//! replicated `net_mesh_ca` row, rotated in two steps with cross-signing by
//! `legion net ca rotate` (worker-plane-auth calls entry 20).
//!
//! This is the only code that reads `net_mesh_ca`; everything else asks the
//! [`super::ClusterCa`].

use super::{CaError, CaView, ClusterCaProvider};
use crate::manager::mesh_ca::LeafSpec;
use crate::storage::netpolicy_store::MeshCa;
use crate::storage::store::Store;
use crate::structs::config::CLUSTER_CA_BUILTIN;

/// The built-in provider. Stateless: the store row is the state.
pub struct BuiltinCa;

/// The view of a built-in CA row.
pub fn view_of(ca: &MeshCa) -> Result<CaView, CaError> {
    let pin = ca
        .pin()
        .map_err(|err| CaError::Internal(err.context("the built-in cluster CA certificate")))?;
    let bundle_pem = ca.bundle_pem();
    let phase = ca.phase(time::OffsetDateTime::now_utc());
    let overlap = matches!(
        phase,
        crate::storage::netpolicy_store::CaRotationPhase::Overlap { .. }
    );
    Ok(CaView {
        provider: CLUSTER_CA_BUILTIN,
        location: String::new(),
        signing_cert_pem: ca.cert_pem.clone(),
        bundle_pins: crate::cluster_tls::bundle_pins(&bundle_pem),
        bundle_pem,
        chain_pem: ca.chain_suffix(),
        pin,
        created_at: ca.created_at.clone(),
        phase,
        previous_cert_pem: if overlap {
            ca.previous_cert_pem.clone()
        } else {
            None
        },
    })
}

impl ClusterCaProvider for BuiltinCa {
    fn kind(&self) -> &'static str {
        CLUSTER_CA_BUILTIN
    }

    fn location(&self) -> String {
        String::new()
    }

    fn view(&self, store: &Store) -> Result<Option<CaView>, CaError> {
        store
            .mesh_ca()
            .map_err(CaError::Internal)?
            .as_ref()
            .map(view_of)
            .transpose()
    }

    fn ensure(&self, store: &Store) -> Result<CaView, CaError> {
        let ca = store.ensure_mesh_ca().map_err(|err| {
            CaError::Internal(
                err.context("the cluster CA does not exist yet and could not be created"),
            )
        })?;
        view_of(&ca)
    }

    fn sign(
        &self,
        store: &Store,
        csr_pem: &str,
        spec: &LeafSpec,
    ) -> Result<(String, CaView), CaError> {
        // One read for the key, the chain, and the view, so a rotation
        // between them cannot pair a leaf with the wrong chain.
        let ca = store.ensure_mesh_ca().map_err(CaError::Internal)?;
        let (leaf, _) =
            crate::manager::mesh_ca::sign_leaf(&ca.cert_pem, &ca.key_pem, csr_pem, spec)
                .map_err(|err| CaError::Internal(err.context("signing with the built-in CA")))?;
        Ok((format!("{leaf}{}", ca.chain_suffix()), view_of(&ca)?))
    }

    fn rotate(&self, store: &Store) -> Result<CaView, CaError> {
        let ca = store.rotate_mesh_ca().map_err(CaError::Internal)?;
        view_of(&ca)
    }

    fn verifies_output(&self) -> bool {
        false
    }
}
