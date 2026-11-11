//! Host bindings for `legion:isolate@0.1.0` (`wit/legion-isolate.wit`).
//!
//! Every import is Rust-async on the guest's fiber (the guest sees a
//! blocking call): `vm` waits on the worker, and the rest complete at once.
//! The `runner` export is called with `call_async`.

wasmtime::component::bindgen!({
    path: "wit/legion-isolate.wit",
    inline: "
        package legion:host;
        world host-world {
            import legion:isolate/control@0.1.0;
            import legion:isolate/state@0.1.0;
            import legion:isolate/vm@0.1.0;
            export legion:isolate/runner@0.1.0;
        }
    ",
    world: "legion:host/host-world",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        "legion:isolate/vm.machine": crate::guest::Machine,
    },
    require_store_data_send: true,
});

pub use exports::legion::isolate::runner;
pub use legion::isolate::{control, state, vm};
