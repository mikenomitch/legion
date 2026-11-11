//! Host bindings for `wasi:filesystem/{types,preopens}@0.2.12` over the
//! scratch tree.
//!
//! The WIT is the copy wasmtime-wasi 50.0.1 ships (`wit/deps/`), so the
//! `wasi:io` stream and error resources are wasmtime-wasi-io's own types and
//! a stream this filesystem hands out is an ordinary `wasi:io` stream to the
//! rest of WASI. Every import is synchronous: nothing here waits, so the
//! functions work under both `call` and `call_async`.

wasmtime::component::bindgen!({
    path: [
        "wit/deps/io.wit",
        "wit/deps/clocks.wit",
        "wit/deps/filesystem.wit",
    ],
    inline: "
        package legion:scratch;
        world scratch {
            import wasi:filesystem/types@0.2.12;
            import wasi:filesystem/preopens@0.2.12;
        }
    ",
    world: "legion:scratch/scratch",
    imports: { default: trappable },
    trappable_error_type: {
        "wasi:filesystem/types.error-code" => crate::scratch::host::FsError,
    },
    with: {
        "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
        "wasi:clocks/wall-clock": wasmtime_wasi::p2::bindings::clocks::wall_clock,
        "wasi:filesystem/types.descriptor": crate::scratch::host::Descriptor,
        "wasi:filesystem/types.directory-entry-stream": crate::scratch::host::DirEntries,
    },
    require_store_data_send: true,
});
