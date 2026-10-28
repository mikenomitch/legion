//! Getting a module's bytes: by URL and `sha256:` checksum, verified before
//! anything compiles them.

/// Fetches a module's bytes, checked against its checksum. Blocking.
pub trait ModuleLoader: Send + Sync {
    fn load(&self, source: &str, checksum: &str) -> Result<Vec<u8>, String>;
}

/// Fetches through the worker's artifact stager into the node content store,
/// so a module is downloaded once per node, survives restarts, and follows
/// `artifacts.fetch` (allowed schemes, bucket credentials). `label` names
/// the module's use in the content store (`net plugin module`).
pub struct StagerLoader {
    pub stager: std::sync::Arc<crate::artifacts::stage::ArtifactStager>,
    pub label: &'static str,
}

impl ModuleLoader for StagerLoader {
    fn load(&self, source: &str, checksum: &str) -> Result<Vec<u8>, String> {
        let mut spec = crate::structs::artifact::ArtifactSpec::from_source(source);
        spec.checksum = Some(checksum.to_string());
        let fetched = self
            .stager
            .fetch_into_store(
                &spec,
                crate::content_store::ContentClass::Artifacts,
                crate::content_store::PutOptions::default().label(self.label),
            )
            .map_err(|err| format!("{err:#}"))?;
        let bytes = std::fs::read(&fetched.entry.path)
            .map_err(|err| format!("reading the stored module: {err}"))?;
        verify_checksum(&bytes, checksum)?;
        Ok(bytes)
    }
}

/// Check `bytes` against a `sha256:<hex>` checksum.
pub fn verify_checksum(bytes: &[u8], checksum: &str) -> Result<(), String> {
    use sha2::Digest;
    let got = format!("sha256:{:x}", sha2::Sha256::digest(bytes));
    if !checksum.eq_ignore_ascii_case(&got) {
        return Err(format!(
            "module checksum mismatch: expected {checksum}, got {got}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod module_test {
    use super::*;

    #[test]
    fn checksums_are_verified_test() {
        let good = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert!(verify_checksum(b"hello", good).is_ok());
        assert!(verify_checksum(b"hello", &good.to_uppercase()).is_ok());
        let err = verify_checksum(b"hellO", good).unwrap_err();
        assert!(err.contains("mismatch"), "{err}");
    }
}
