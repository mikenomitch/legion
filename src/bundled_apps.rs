//! The manifests of the apps compiled into this binary (docs/apps.md,
//! "Bundled apps"). The manager seeds a record for each on a new cluster
//! and refuses an installed app that takes one of their names; what each
//! app *runs* is `crate::bundled_apps::MANIFESTS`, in the binary crate, where the
//! worker's `legion app run` looks it up.
//!
//! **Adding a bundled app** is a manifest here (a `.ncl` file in
//! `bundled/` and a row in [`MANIFESTS`]) and a body in
//! `crate::bundled_apps::MANIFESTS`, under the same name.

use std::sync::OnceLock;

use crate::prelude::*;
use crate::structs::app::{
    AppGrant, AppManifest, AppReconcileState, AppRecord, AppSettings, AppSource, Placement,
    BOOTSTRAP_PRINCIPAL,
};
use crate::structs::config::AppSeedConfig;

/// One bundled app's manifest.
pub struct BundledManifest {
    pub name: &'static str,
    /// The manifest, in Nickel, evaluated once per process with imports
    /// refused.
    pub manifest_source: &'static str,
    /// Fills in what the manifest leaves to the app's Rust types (its
    /// kinds' schemas), on the evaluated manifest before it is parsed.
    pub generated: Option<fn(&mut Value)>,
    /// Whether a new cluster seeds the app enabled. The manager config's
    /// `apps.<name>.enabled` overrides it at seed time.
    pub seed_enabled: bool,
}

/// Every bundled app. The first Control leader seeds a record for each
/// (enabled as its `seed_enabled` says, unless the manager config's `apps`
/// block says otherwise), with its manifest's permissions granted by
/// `legion:bootstrap`.
pub const MANIFESTS: &[BundledManifest] = &[
    BundledManifest {
        name: "autoscaler",
        manifest_source: include_str!("bundled_apps/autoscaler.ncl"),
        generated: Some(crate::autoscaler::policy::fill_manifest_kinds),
        seed_enabled: true,
    },
    BundledManifest {
        name: "certs",
        manifest_source: include_str!("bundled_apps/certs.ncl"),
        generated: None,
        seed_enabled: true,
    },
    BundledManifest {
        name: "gitops",
        manifest_source: include_str!("bundled_apps/gitops.ncl"),
        generated: None,
        seed_enabled: true,
    },
    // Seeded off: it takes ports 80 and 443 on the nodes it runs on, which
    // no cluster should do until someone asks (ingress plan calls entry 3).
    BundledManifest {
        name: "ingress",
        manifest_source: include_str!("bundled_apps/ingress.ncl"),
        generated: None,
        seed_enabled: false,
    },
];

/// The bundled manifests, evaluated and validated once.
fn evaluated() -> &'static Result<Vec<AppManifest>, String> {
    static EVALUATED: OnceLock<Result<Vec<AppManifest>, String>> = OnceLock::new();
    EVALUATED.get_or_init(|| {
        MANIFESTS
            .iter()
            .map(|app| {
                let json = crate::config_lang::eval_sandboxed(
                    &format!("bundled/{}.ncl", app.name),
                    app.manifest_source,
                    None,
                )
                .map_err(|err| format!("bundled app {}: {err:#}", app.name))?;
                let json = match app.generated {
                    Some(fill) => {
                        let mut value: Value = serde_json::from_str(&json)
                            .map_err(|err| format!("bundled app {}: {err}", app.name))?;
                        fill(&mut value);
                        value.to_string()
                    }
                    None => json,
                };
                let manifest = AppManifest::from_json(&json)
                    .map_err(|err| format!("bundled app {}: {err}", app.name))?;
                manifest.validate(true)?;
                if manifest.name != app.name {
                    return Err(format!(
                        "bundled app {} has a manifest named {}",
                        app.name, manifest.name
                    ));
                }
                Ok(manifest)
            })
            .collect()
    })
}

/// The manifests compiled into this binary. An error is a build defect,
/// which the unit tests catch; callers log it and carry on without apps.
pub fn bundled_manifests() -> R<&'static [AppManifest]> {
    evaluated()
        .as_ref()
        .map(Vec::as_slice)
        .map_err(|err| anyhow!("{err}"))
}

/// The manifest of bundled app `name`, if this binary has one.
pub fn bundled_manifest(name: &str) -> Option<&'static AppManifest> {
    bundled_manifests()
        .ok()?
        .iter()
        .find(|manifest| manifest.name == name)
}

/// Whether `name` is a bundled app's (installed apps may not take it).
pub fn is_bundled(name: &str) -> bool {
    MANIFESTS.iter().any(|app| app.name == name)
}

/// The jobspec keys the bundled apps declare (the ingress's `ingress` on
/// services), as every cluster registers them by seeding. Parsing a jobspec
/// against it checks those keys without a manager: what the shipped example
/// jobs are tested with.
pub fn bundled_extension_registry() -> &'static crate::structs::extensions::ExtensionRegistry {
    static REGISTRY: OnceLock<crate::structs::extensions::ExtensionRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let records: Vec<crate::structs::app::AppRecord> = bundled_manifests()
            .unwrap_or_default()
            .iter()
            .map(|manifest| seed_record(manifest, None))
            .collect();
        let (registry, problems) =
            crate::structs::extensions::ExtensionRegistry::from_records(&records);
        for problem in problems {
            warn!("bundled jobspec keys: {problem}");
        }
        registry
    })
}

/// Whether bundled app `name` is seeded enabled on a new cluster (`true`
/// for a name that is not bundled, which is never seeded).
pub fn seeds_enabled(name: &str) -> bool {
    MANIFESTS
        .iter()
        .find(|app| app.name == name)
        .is_none_or(|app| app.seed_enabled)
}

/// The record a bundled app is seeded with: its manifest's defaults, enabled
/// as its [`BundledManifest::seed_enabled`] says, the manager
/// config's `apps.<name>` overrides, and its permissions granted by
/// `legion:bootstrap` (calls entry 17).
pub fn seed_record(
    manifest: &crate::structs::app::AppManifest,
    seed: Option<&AppSeedConfig>,
) -> AppRecord {
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let mut settings = AppSettings::from_manifest(manifest);
    let mut config = manifest.default_config();
    // A bundled app says whether it starts on: the autoscaler does, the
    // ingress (which takes ports 80 and 443) waits for `legion app enable`.
    let default_enabled = seeds_enabled(&manifest.name);
    let mut enabled = default_enabled;
    if let Some(seed) = seed {
        enabled = seed.enabled.unwrap_or(default_enabled);
        if let Some(placement) = &seed.placement {
            match Placement::parse(placement) {
                Ok(placement) => settings.placement = placement,
                Err(err) => {
                    warn!(app = %manifest.name, "apps.{}.placement: {err}; using the manifest's", manifest.name)
                }
            }
        }
        if let Some(replicas) = seed.replicas {
            let candidate = AppSettings {
                replicas,
                ..settings.clone()
            };
            match candidate.validate() {
                Ok(()) => settings = candidate,
                Err(err) => {
                    warn!(app = %manifest.name, "apps.{}.replicas: {err}; using the manifest's", manifest.name)
                }
            }
        }
        if let Some(pool) = &seed.pool {
            let candidate = AppSettings {
                pool: Some(pool.trim().to_string()),
                ..settings.clone()
            };
            match candidate.validate() {
                Ok(()) => settings = candidate,
                Err(err) => {
                    warn!(app = %manifest.name, "apps.{}.pool: {err}; using the default pool", manifest.name)
                }
            }
        }
        if let Some(seed_config) = &seed.config {
            match crate::structs::app::validate_app_config(manifest, seed_config) {
                Ok(()) => config = seed_config.clone(),
                Err(err) => warn!(
                    app = %manifest.name,
                    "apps.{}.config does not match the app's config schema ({err}); seeding its default config",
                    manifest.name
                ),
            }
        }
    }
    AppRecord {
        name: manifest.name.clone(),
        source: AppSource::Bundled,
        source_ref: String::new(),
        manifest_digest: manifest.digest(),
        enabled,
        settings,
        config,
        grant: AppGrant {
            rules: manifest.permissions.clone(),
            granted_by: BOOTSTRAP_PRINCIPAL.to_string(),
            granted_at: now.clone(),
        },
        installed_by: BOOTSTRAP_PRINCIPAL.to_string(),
        installed_at: now.clone(),
        updated_at: now,
        state: AppReconcileState::default(),
        manifest: manifest.clone(),
        // The store assigns it when the record is inserted.
        uid: 0,
    }
}
