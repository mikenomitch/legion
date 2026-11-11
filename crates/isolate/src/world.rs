//! Host bindings for `legion:isolate@0.1.0` (`wit/legion-isolate.wit`).
//!
//! Every import is Rust-async on the guest's fiber (the guest sees a
//! blocking call): `vm` waits on the worker, and the rest complete at once.
//! The `runner` export is called with `call_async`.

// TEMPORARY: an inline copy of wit/legion-isolate.wit with `record info`
// renamed to `isolate-info` (the file as checked in does not parse: a type
// and a function may not share a name). Switch back to `path:` once the
// file carries the rename.
wasmtime::component::bindgen!({
    inline: r#"
/// The interfaces Legion's isolate host offers a guest beyond WASI, and the
/// export a reusable guest answers runs on (docs/task-runtime-isolate.md,
/// "The guest's world").
///
/// A component imports only what it uses. One that imports an interface the
/// host does not provide is refused when its task is prepared on a node, not
/// at the call.
package legion:isolate@0.1.0;

/// Who this isolate is, and the two things a guest can tell the host.
interface control {
  /// How long an instance lives (the task's `config.lifetime`).
  enum lifetime { request, tenant, session }

  record isolate-info {
    /// The job and task this isolate belongs to.
    job: string,
    task: string,
    lifetime: lifetime,
    /// The tenant key the invocation carried, when it carried one.
    tenant: option<string>,
    /// The session id, for a session isolate.
    session: option<string>,
    /// True on the first call after this instance was made from a
    /// hibernation checkpoint rather than started new: its memory is fresh
    /// and `state` holds what it kept.
    resumed: bool,
  }

  info: func() -> isolate-info;

  /// Report that the code needs something an isolate cannot do (a
  /// subprocess, a socket, a native package) and stop the run. Language
  /// images call this from their stubs. Does not return: the invocation is
  /// stopped with `reason = "unsupported"` and `what` as its detail.
  unsupported: func(what: string);

  /// Say this isolate is waiting and may be hibernated now rather than
  /// after its idle time. Takes effect when the current call returns.
  /// A no-op for request and tenant isolates.
  park: func();
}

/// Bytes that outlive the instance: scoped to the session for a session
/// isolate (kept across hibernation, and across nodes with
/// `suspend.durability = "store"`), and to the tenant for a tenant isolate
/// (kept while the tenant's instance is warm, best effort).
interface state {
  variant state-error {
    /// A request isolate has no state to keep.
    not-available,
    /// The write would exceed the quota; carries the quota in bytes.
    quota(u64),
  }

  get: func(key: string) -> result<option<list<u8>>, state-error>;
  set: func(key: string, value: list<u8>) -> result<_, state-error>;
  delete: func(key: string) -> result<_, state-error>;
  keys: func() -> result<list<string>, state-error>;
}

/// Child VMs: a machine of the task's VM task (`config.vm.task`), started
/// as a child session on the same node and driven from the isolate
/// (docs/child-sessions.md).
interface vm {
  variant vm-error {
    /// The task has no VM task.
    not-configured,
    /// Retryable: the VM task's envelope is full.
    resource-exhausted(string),
    /// Stopped, evicted, or its node was lost.
    child-gone(string),
    /// A resize past what the VM can do while running.
    needs-restart(string),
    failed(string),
  }

  record size {
    memory-mib: u32,
    memory-max-mib: option<u32>,
    cpu-mhz: option<u32>,
  }

  record exec-result {
    exit: s32,
    stdout: list<u8>,
    stderr: list<u8>,
    timed-out: bool,
  }

  record http-request {
    method: string,
    /// Path and query.
    path: string,
    headers: list<tuple<string, list<u8>>>,
    body: list<u8>,
  }

  record http-response {
    status: u16,
    headers: list<tuple<string, list<u8>>>,
    body: list<u8>,
  }

  resource machine {
    /// The child's session id. Keep it in `state` to re-attach after
    /// hibernation or a move.
    id: func() -> string;
    exec: func(argv: list<string>, stdin: list<u8>, timeout-ms: u32) -> result<exec-result, vm-error>;
    put: func(path: string, bytes: list<u8>) -> result<_, vm-error>;
    get: func(path: string) -> result<list<u8>, vm-error>;
    /// An HTTP request to a port the machine listens on.
    fetch: func(port: u16, request: http-request) -> result<http-response, vm-error>;
    resize: func(to: size) -> result<_, vm-error>;
    /// End the child now. Dropping the handle does not: a child ends with
    /// its owner, or when released.
    release: func();
  }

  /// Start a child VM.
  acquire: func() -> result<machine, vm-error>;
  /// Re-attach to a child this isolate (or its session) already owns.
  attach: func(id: string) -> result<machine, vm-error>;
}

/// The export a reusable guest answers runs on. Language images export it,
/// and so can any component that wants `lifetime = "tenant"` or `"session"`
/// for runs: `wasi:cli/run` is single-shot, so a command component gets a
/// fresh instance per run.
interface runner {
  record request {
    /// The source file to execute, relative to the run directory
    /// (`/legion/run`). Unset when the caller named none.
    entry: option<string>,
    /// The caller's input, a JSON document. Also at `input.json` in the
    /// run directory.
    input: option<string>,
    args: list<string>,
  }

  /// Run once. `ok` carries an optional JSON result (reported as the
  /// run's `result`); `err` carries an error message, reported on stderr
  /// with exit status 1.
  run: func(req: request) -> result<option<string>, string>;
}

world host-world {
  import control;
  import state;
  import vm;
  export runner;
}

"#,
    world: "legion:isolate/host-world",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        "legion:isolate/vm.machine": crate::guest::Machine,
    },
    require_store_data_send: true,
});

pub use exports::legion::isolate::runner;
pub use legion::isolate::{control, state, vm};
