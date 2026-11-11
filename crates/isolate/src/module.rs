//! The host's linker (every import a guest may use) and loaded modules.
//!
//! The compiler builds the same linker to refuse, at deploy time, a
//! component that imports something the host does not provide; a host
//! builds it once per engine and resolves each module's imports into an
//! `InstancePre` when the module is loaded, so a call never resolves names.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use wasmtime::component::{Component, HasSelf, InstancePre, Linker};
use wasmtime::Engine;
use wasmtime_wasi::p2::bindings::{cli, clocks, random, sockets};
use wasmtime_wasi::{
    cli::{WasiCli, WasiCliView},
    clocks::{WasiClocks, WasiClocksView},
    random::WasiRandom,
    sockets::{WasiSockets, WasiSocketsView},
};

use crate::engine::WASM_PAGE;
use crate::guest::Ctx;
use crate::world;

/// Every import the host offers a guest: WASI 0.2 (filesystem over the
/// scratch tree, sockets linked but denying everything), `wasi:http`
/// types and outgoing-handler (to the worker), and `legion:isolate`.
pub fn linker(engine: &Engine) -> anyhow::Result<Linker<Ctx>> {
    let mut l: Linker<Ctx> = Linker::new(engine);
    link(&mut l).map_err(|e| anyhow::anyhow!("linking the host interfaces: {e:#}"))?;
    Ok(l)
}

fn link(l: &mut Linker<Ctx>) -> wasmtime::Result<()> {
    wasmtime_wasi_io::add_to_linker_async(l)?;
    clocks::wall_clock::add_to_linker::<Ctx, WasiClocks>(l, Ctx::clocks)?;
    clocks::monotonic_clock::add_to_linker::<Ctx, WasiClocks>(l, Ctx::clocks)?;
    random::random::add_to_linker::<Ctx, WasiRandom>(l, |t| t.wasi.random())?;
    random::insecure::add_to_linker::<Ctx, WasiRandom>(l, |t| t.wasi.random())?;
    random::insecure_seed::add_to_linker::<Ctx, WasiRandom>(l, |t| t.wasi.random())?;
    cli::exit::add_to_linker::<Ctx, WasiCli>(l, &Default::default(), Ctx::cli)?;
    cli::environment::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::stdin::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::stdout::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::stderr::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::terminal_input::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::terminal_output::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::terminal_stdin::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::terminal_stdout::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    cli::terminal_stderr::add_to_linker::<Ctx, WasiCli>(l, Ctx::cli)?;
    sockets::tcp_create_socket::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    sockets::instance_network::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    sockets::network::add_to_linker::<Ctx, WasiSockets>(l, &Default::default(), Ctx::sockets)?;
    sockets::tcp::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    sockets::udp::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    sockets::udp_create_socket::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    sockets::ip_name_lookup::add_to_linker::<Ctx, WasiSockets>(l, Ctx::sockets)?;
    crate::scratch::add_to_linker(l).map_err(wasmtime::Error::from_anyhow)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(l)?;
    world::control::add_to_linker::<Ctx, HasSelf<Ctx>>(l, |t| t)?;
    world::state::add_to_linker::<Ctx, HasSelf<Ctx>>(l, |t| t)?;
    world::vm::add_to_linker::<Ctx, HasSelf<Ctx>>(l, |t| t)?;
    Ok(())
}

/// What a component exports, which decides the calls it takes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Exports {
    pub cli_run: bool,
    pub http_handler: bool,
    pub runner: bool,
}

/// Interface name without its `@version`.
pub fn unversioned(name: &str) -> &str {
    name.split('@').next().unwrap_or(name)
}

pub fn exports_of(engine: &Engine, c: &Component) -> Exports {
    let mut e = Exports::default();
    for (name, _) in c.component_type().exports(engine) {
        match unversioned(name) {
            "wasi:cli/run" => e.cli_run = true,
            "wasi:http/incoming-handler" => e.http_handler = true,
            "legion:isolate/runner" => e.runner = true,
            _ => {}
        }
    }
    e
}

pub fn imports_of(engine: &Engine, c: &Component) -> Vec<String> {
    c.component_type()
        .imports(engine)
        .map(|(n, _)| n.to_string())
        .collect()
}

/// The largest initial linear memory among the component's modules.
pub fn initial_memory_bytes(c: &Component) -> u64 {
    c.resources_required()
        .and_then(|r| r.max_initial_memory_size)
        .unwrap_or(0)
        .saturating_mul(WASM_PAGE)
}

/// Pull the import name out of wasmtime's "component imports instance
/// `X`, but a matching implementation was not found in the linker".
pub fn missing_import(err: &str) -> Option<String> {
    let rest = err.split("imports ").nth(1)?;
    let start = rest.find('`')? + 1;
    let end = start + rest[start..].find('`')?;
    Some(rest[start..end].to_string())
}

/// A module the host has mapped and linked.
pub struct LoadedModule {
    pub module_id: String,
    pub path: PathBuf,
    pub component: Component,
    pub pre: InstancePre<Ctx>,
    pub exports: Exports,
    pub initial_memory_bytes: u64,
    /// Present when `exports.cli_run`.
    pub command: Option<wasmtime_wasi::p2::bindings::CommandPre<Ctx>>,
    /// Present when `exports.http_handler`.
    pub proxy: Option<wasmtime_wasi_http::p2::bindings::ProxyPre<Ctx>>,
    /// Present when `exports.runner`.
    pub runner: Option<world::HostWorldPre<Ctx>>,
}

impl LoadedModule {
    /// Map an artifact and resolve its imports.
    ///
    /// # Safety
    /// The artifact is executable code: it must come from this binary's
    /// own `compile` (the host checks the path is inside a `read_dirs`
    /// directory before calling this).
    pub unsafe fn load(
        engine: &Engine,
        linker: &Linker<Ctx>,
        module_id: &str,
        path: &Path,
    ) -> anyhow::Result<Arc<Self>> {
        let component = unsafe { Component::deserialize_file(engine, path) }
            .map_err(|e| anyhow::anyhow!("{e:#}"))?;
        let pre = linker
            .instantiate_pre(&component)
            .map_err(|e| anyhow::anyhow!("{e:#}"))?;
        let exports = exports_of(engine, &component);
        let command = if exports.cli_run {
            Some(
                wasmtime_wasi::p2::bindings::CommandPre::new(pre.clone())
                    .map_err(|e| anyhow::anyhow!("{e:#}"))?,
            )
        } else {
            None
        };
        let proxy = if exports.http_handler {
            Some(
                wasmtime_wasi_http::p2::bindings::ProxyPre::new(pre.clone())
                    .map_err(|e| anyhow::anyhow!("{e:#}"))?,
            )
        } else {
            None
        };
        let runner = if exports.runner {
            Some(world::HostWorldPre::new(pre.clone()).map_err(|e| anyhow::anyhow!("{e:#}"))?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            module_id: module_id.to_string(),
            path: path.to_path_buf(),
            initial_memory_bytes: initial_memory_bytes(&component),
            component,
            pre,
            exports,
            command,
            proxy,
            runner,
        }))
    }
}

#[cfg(test)]
mod module_test {
    use super::*;

    #[test]
    fn names_the_missing_import_test() {
        let msg = "component imports instance `wasi:nope/thing@0.2.0`, but a matching \
                   implementation was not found in the linker";
        assert_eq!(
            missing_import(msg).as_deref(),
            Some("wasi:nope/thing@0.2.0")
        );
        assert_eq!(unversioned("wasi:cli/run@0.2.3"), "wasi:cli/run");
    }
}
