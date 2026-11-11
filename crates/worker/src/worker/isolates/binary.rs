//! Finding the `legion-isolate` binary and the built-in image components on
//! this node (brief item 12).

use crate::prelude::*;
use crate::structs::isolate::{
    WorkerIsolateConfigFull, DEF_IMAGES_DIR, ENV_ISOLATE_BINARY, ENV_ISOLATE_IMAGES,
    ISOLATE_BINARY_NAME,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How long `engine-info` may take.
const ENGINE_INFO_TIMEOUT: Duration = Duration::from_secs(10);

/// A usable `legion-isolate` binary and what its engine is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolateBinary {
    pub path: PathBuf,
    pub engine_version: String,
    /// The artifact compatibility key for the node's engine settings.
    pub compat_key: String,
}

/// Where the binary should be: `isolate.binary`, else beside the worker's
/// executable, else `LEGION_ISOLATE_BINARY`.
pub fn locate_binary(config: &WorkerIsolateConfigFull) -> Option<PathBuf> {
    if let Some(path) = config.binary.as_deref().filter(|path| !path.is_empty()) {
        return Some(PathBuf::from(path));
    }
    let beside = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(ISOLATE_BINARY_NAME)))
        .filter(|path| path.is_file());
    beside.or_else(|| {
        std::env::var_os(ENV_ISOLATE_BINARY)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    })
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct EngineInfo {
    engine_version: String,
    compat_key: String,
    protocol_version: u32,
}

/// The engine flags `engine-info` and `compile` take for this worker.
pub fn engine_args(config: &WorkerIsolateConfigFull) -> Vec<String> {
    vec![
        "--layout".into(),
        super::plan::layout_flag(config.layout).into(),
        "--slot-bytes".into(),
        config.max_memory_bytes().unwrap_or(4 << 30).to_string(),
    ]
}

/// Probe a binary: run `engine-info` and check its protocol version.
pub fn probe(path: &Path, config: &WorkerIsolateConfigFull) -> R<IsolateBinary> {
    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        bail!("isolates run on x86-64 Linux only for now");
    }
    let mut child = std::process::Command::new(path)
        .arg("engine-info")
        .args(engine_args(config))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("run {} engine-info", path.display()))?;
    let deadline = std::time::Instant::now() + ENGINE_INFO_TIMEOUT;
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("{} engine-info did not answer within 10s", path.display());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!(
            "{} engine-info failed ({}): {}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let info: EngineInfo = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "{} engine-info printed something that is not its JSON",
            path.display()
        )
    })?;
    let ours = crate::structs::isolate_host::PROTOCOL_VERSION;
    if info.protocol_version != ours {
        bail!(
            "{} speaks isolate host protocol {}, this worker speaks {ours}; install the \
             legion-isolate that ships with this worker",
            path.display(),
            info.protocol_version
        );
    }
    Ok(IsolateBinary {
        path: path.to_path_buf(),
        engine_version: info.engine_version,
        compat_key: info.compat_key,
    })
}

/// One built-in image in the images directory's `manifest.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageEntry {
    pub file: String,
    pub sha256: String,
    pub modules: Vec<String>,
    pub extensions: Vec<String>,
    pub initial_memory_bytes: u64,
    pub idle_private_bytes: u64,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
struct ImageManifest {
    images: BTreeMap<String, ImageEntry>,
}

/// The images directory and its manifest.
#[derive(Debug, Clone, Default)]
pub struct ImageCatalog {
    pub dir: PathBuf,
    pub images: BTreeMap<String, ImageEntry>,
}

impl ImageCatalog {
    /// Where the images are: `isolate.images`, else `isolate-images` beside
    /// the binary, else `LEGION_ISOLATE_IMAGES`.
    pub fn locate(config: &WorkerIsolateConfigFull, binary: &Path) -> Option<PathBuf> {
        if let Some(dir) = config.images.as_deref().filter(|dir| !dir.is_empty()) {
            return Some(PathBuf::from(dir));
        }
        let beside = binary
            .parent()
            .map(|dir| dir.join(DEF_IMAGES_DIR))
            .filter(|dir| dir.is_dir());
        beside.or_else(|| {
            std::env::var_os(ENV_ISOLATE_IMAGES)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
    }

    /// Read `<dir>/manifest.json`.
    pub fn load(dir: &Path) -> R<Self> {
        let raw = std::fs::read(dir.join("manifest.json"))
            .with_context(|| format!("read {}/manifest.json", dir.display()))?;
        let manifest: ImageManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("parse {}/manifest.json", dir.display()))?;
        Ok(ImageCatalog {
            dir: dir.to_path_buf(),
            images: manifest.images,
        })
    }

    /// The image `config.image` names, checked against a pinned digest.
    pub fn resolve(&self, image: &str) -> Result<(&ImageEntry, PathBuf), String> {
        let name = crate::structs::isolate::image_name(image);
        let entry = self.images.get(name).ok_or_else(|| {
            format!(
                "this node has no build of image {name:?} in {}",
                self.dir.display()
            )
        })?;
        if let Some((_, pinned)) = image.trim().split_once('@') {
            let ours = normalize_sha(&entry.sha256);
            if normalize_sha(pinned) != ours {
                return Err(format!(
                    "config.image pins {name}@{pinned}, but this node has {name}@{ours}; pin the \
                     build the nodes run, or write {name:?} for the node's current build"
                ));
            }
        }
        Ok((entry, self.dir.join(&entry.file)))
    }
}

fn normalize_sha(raw: &str) -> String {
    let hex = raw.trim().to_ascii_lowercase();
    if hex.starts_with("sha256:") {
        hex
    } else {
        format!("sha256:{hex}")
    }
}

#[cfg(test)]
mod binary_test {
    use super::*;

    const HEX: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    #[test]
    fn manifest_resolves_and_checks_pins() -> R<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::json!({
                "images": { "python": {
                    "file": "python.wasm", "sha256": HEX, "modules": ["json", "re"],
                    "extensions": [".py"], "initialMemoryBytes": 1, "idlePrivateBytes": 2
                } }
            })
            .to_string(),
        )?;
        let catalog = ImageCatalog::load(dir.path())?;
        let (entry, path) = catalog.resolve("python").unwrap();
        assert_eq!(entry.modules, vec!["json", "re"]);
        assert_eq!(path, dir.path().join("python.wasm"));
        assert!(catalog.resolve(&format!("python@sha256:{HEX}")).is_ok());
        let wrong = catalog
            .resolve(&format!("python@sha256:{}", "0".repeat(64)))
            .unwrap_err();
        assert!(
            wrong.contains("but this node has python@sha256:"),
            "{wrong}"
        );
        assert!(catalog
            .resolve("js")
            .unwrap_err()
            .contains("no build of image \"js\""));
        Ok(())
    }

    #[test]
    fn configured_binary_wins() {
        let config = WorkerIsolateConfigFull {
            binary: Some("/opt/legion-isolate".into()),
            ..Default::default()
        };
        assert_eq!(
            locate_binary(&config),
            Some(PathBuf::from("/opt/legion-isolate"))
        );
    }

    #[test]
    fn a_binary_that_is_not_one_is_refused() {
        let config = WorkerIsolateConfigFull::default();
        let err = probe(Path::new("/bin/false"), &config).unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.contains("engine-info failed") || text.contains("x86-64 Linux"),
            "{text}"
        );
    }
}
