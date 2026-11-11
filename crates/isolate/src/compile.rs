//! `legion-isolate compile`: validate one component, compile it with
//! Cranelift, and write the artifact a host maps (plan §6.5).
//!
//! The steps, each with its own failure kind:
//! 1. **Read** the input (`io`).
//! 2. **Validate** with the newest `wasmparser` under the features the host
//!    enables, before Cranelift sees a byte (`invalid`). Core modules are
//!    refused, not wrapped: a component's imports are the contract the host
//!    checks, and a core module has none to check.
//! 3. **Compile** under the engine configuration a host with the same
//!    [`EngineParams`] uses (`invalid` when wasmtime refuses the module).
//! 4. **Link** against the host's own linker, so a component that imports
//!    an interface the host does not provide is refused here, at deploy,
//!    naming the import (`unsupported-import`).
//! 5. **Write** the artifact atomically: a temporary file in the output
//!    directory, then a rename (`io`).

use std::io::Write;
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use wasmparser::{Parser, Validator, WasmFeatures};

use crate::engine::{self, EngineParams};
use crate::module::{self, Exports};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompileReport {
    pub ok: bool,
    pub exports: Exports,
    pub initial_memory_bytes: u64,
    pub imports: Vec<String>,
    /// Size of the written artifact.
    pub bytes: u64,
    pub compile_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileError {
    Invalid(String),
    UnsupportedImport(String),
    Io(String),
    Internal(String),
}

impl CompileError {
    pub fn kind(&self) -> &'static str {
        match self {
            CompileError::Invalid(_) => "invalid",
            CompileError::UnsupportedImport(_) => "unsupported-import",
            CompileError::Io(_) => "io",
            CompileError::Internal(_) => "internal",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            CompileError::Invalid(m)
            | CompileError::UnsupportedImport(m)
            | CompileError::Io(m)
            | CompileError::Internal(m) => m,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "ok": false, "error": self.message(), "kind": self.kind() })
    }
}

/// The Wasm features a guest may use: the parser's defaults without
/// threads and shared memory (plan §6.10 layer 4), without Component Model
/// async (brief item 14), and without stack switching.
pub fn guest_features() -> WasmFeatures {
    WasmFeatures::default()
        .difference(WasmFeatures::THREADS)
        .difference(WasmFeatures::SHARED_EVERYTHING_THREADS)
        .difference(WasmFeatures::CM_ASYNC)
        .difference(WasmFeatures::CM_ASYNC_STACKFUL)
        .difference(WasmFeatures::CM_THREADING)
        .difference(WasmFeatures::STACK_SWITCHING)
}

/// Check bytes are a component that uses only what the host enables.
pub fn validate(bytes: &[u8]) -> Result<(), CompileError> {
    if !Parser::is_component(bytes) {
        return Err(CompileError::Invalid(if Parser::is_core_wasm(bytes) {
            "this is a core WebAssembly module, not a component; build for wasm32-wasip2 \
             or wrap it with `wasm-tools component new`"
                .into()
        } else {
            "this is not a WebAssembly component (bad magic or version)".into()
        }));
    }
    Validator::new_with_features(guest_features())
        .validate_all(bytes)
        .map(|_| ())
        .map_err(|e| CompileError::Invalid(format!("the component does not validate: {e}")))
}

/// Validate, compile, and link-check `bytes`; returns the artifact.
pub fn compile_bytes(
    params: &EngineParams,
    bytes: &[u8],
) -> Result<(Vec<u8>, CompileReport), CompileError> {
    let started = Instant::now();
    validate(bytes)?;
    let engine =
        engine::plain_engine(params).map_err(|e| CompileError::Internal(format!("{e:#}")))?;
    let component = wasmtime::component::Component::new(&engine, bytes)
        .map_err(|e| CompileError::Invalid(format!("the engine refused the component: {e:#}")))?;
    // By name first: wasmtime accepts an import of an empty instance it
    // has no definition for, so the linker alone would let one through.
    let imports = module::imports_of(&engine, &component);
    if let Some(name) = imports.iter().find(|n| !module::provided(n)) {
        return Err(CompileError::UnsupportedImport(format!(
            "the component imports `{name}`, which the isolate host does not provide \
             (it offers WASI 0.2 up to 0.2.12 without wasi:http/incoming-handler as an \
             import, and legion:isolate@0.1.0 control, state, and vm)"
        )));
    }
    let linker = module::linker(&engine).map_err(|e| CompileError::Internal(format!("{e:#}")))?;
    if let Err(e) = linker.instantiate_pre(&component) {
        let msg = format!("{e:#}");
        return Err(match module::missing_import(&msg) {
            Some(name) => CompileError::UnsupportedImport(format!(
                "the component imports `{name}`, which the isolate host does not provide \
                 (it offers WASI 0.2 up to 0.2.12 without wasi:http/incoming-handler as an \
                 import, and legion:isolate@0.1.0 control, state, and vm)"
            )),
            None => CompileError::UnsupportedImport(format!(
                "the component's imports do not match the isolate host: {msg}"
            )),
        });
    }
    let exports = module::exports_of(&engine, &component);
    if !(exports.cli_run || exports.http_handler || exports.runner) {
        return Err(CompileError::Invalid(
            "the component exports none of wasi:cli/run, wasi:http/incoming-handler, or \
             legion:isolate/runner, so an isolate cannot call it"
                .into(),
        ));
    }
    let artifact = component
        .serialize()
        .map_err(|e| CompileError::Internal(format!("{e:#}")))?;
    let report = CompileReport {
        ok: true,
        exports,
        initial_memory_bytes: module::initial_memory_bytes(&component),
        imports,
        bytes: artifact.len() as u64,
        compile_millis: started.elapsed().as_millis() as u64,
    };
    Ok((artifact, report))
}

/// Write `bytes` to `out` through a temporary file in the same directory.
pub fn write_atomic(out: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = out
        .file_name()
        .ok_or_else(|| std::io::Error::other("the output path has no file name"))?;
    let tmp = dir.join(format!(
        ".{}.tmp-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, out)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// The largest component `compile` reads. Legion's own images are about
/// 13 MB (`js`) and 30 MB (`python`); this bounds what a hostile input can
/// make the compiler hold before it has parsed a byte.
pub const MAX_INPUT_BYTES: u64 = 512 << 20;

/// Read a component to compile, at most [`MAX_INPUT_BYTES`]. The
/// `compile` subcommand calls this BEFORE it confines itself: confined, it
/// keeps the caller's kernel uid without its capabilities, so a file the
/// worker can read only as root (under a home directory, say) would be
/// unreadable. Reading is not parsing: the untrusted bytes are still only
/// interpreted inside the confinement.
pub fn read_input(input: &Path) -> Result<Vec<u8>, CompileError> {
    use std::io::Read;
    let io = |e: std::io::Error| CompileError::Io(format!("reading {}: {e}", input.display()));
    let file = std::fs::File::open(input).map_err(io)?;
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() as u64 > MAX_INPUT_BYTES {
        return Err(CompileError::Invalid(format!(
            "{} is larger than the {} MiB a component may be",
            input.display(),
            MAX_INPUT_BYTES >> 20
        )));
    }
    Ok(bytes)
}

/// Validate and compile `bytes`, and write the artifact to `output`.
pub fn compile_to(
    params: &EngineParams,
    bytes: &[u8],
    output: &Path,
) -> Result<CompileReport, CompileError> {
    let (artifact, report) = compile_bytes(params, bytes)?;
    write_atomic(output, &artifact)
        .map_err(|e| CompileError::Io(format!("writing {}: {e}", output.display())))?;
    Ok(report)
}

/// Read, validate, and compile `input` into `output`, unconfined (tests and
/// benches; the `compile` subcommand reads first and confines between).
pub fn compile_file(
    params: &EngineParams,
    input: &Path,
    output: &Path,
) -> Result<CompileReport, CompileError> {
    compile_to(params, &read_input(input)?, output)
}

#[cfg(test)]
mod compile_test {
    use super::*;

    #[test]
    fn refuses_a_core_module_and_garbage_test() {
        let core = wat::parse_str("(module)").unwrap();
        let e = validate(&core).unwrap_err();
        assert_eq!(e.kind(), "invalid");
        assert!(e.message().contains("core WebAssembly module"), "{e:?}");
        let e = validate(b"not wasm").unwrap_err();
        assert!(e.message().contains("not a WebAssembly component"));
    }

    #[test]
    fn refuses_shared_memory_test() {
        let c = wat::parse_str("(component (core module (memory 1 1 shared)))").unwrap();
        let e = validate(&c).unwrap_err();
        assert_eq!(e.kind(), "invalid", "{e:?}");
    }

    #[test]
    fn refuses_an_unknown_import_naming_it_test() {
        let c = wat::parse_str(
            r#"(component
                (import "wasi:nope/thing@0.2.0" (instance))
                (core module $m (func (export "run") (result i32) i32.const 0))
                (core instance $i (instantiate $m))
                (func $run (result (result)) (canon lift (core func $i "run")))
                (instance $r (export "run" (func $run)))
                (export "wasi:cli/run@0.2.0" (instance $r)))"#,
        )
        .unwrap();
        let e = compile_bytes(&EngineParams::default(), &c).unwrap_err();
        assert_eq!(e.kind(), "unsupported-import", "{e:?}");
        assert!(e.message().contains("wasi:nope/thing@0.2.0"), "{e:?}");
        // A provided interface at a version newer than the host's.
        let c = wat::parse_str(
            r#"(component
                (import "wasi:cli/environment@0.2.99" (instance))
                (core module $m (func (export "run") (result i32) i32.const 0))
                (core instance $i (instantiate $m))
                (func $run (result (result)) (canon lift (core func $i "run")))
                (instance $r (export "run" (func $run)))
                (export "wasi:cli/run@0.2.0" (instance $r)))"#,
        )
        .unwrap();
        let e = compile_bytes(&EngineParams::default(), &c).unwrap_err();
        assert!(e.message().contains("wasi:cli/environment@0.2.99"), "{e:?}");
    }
}
