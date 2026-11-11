//! The scratch filesystem under real guests: a Rust `wasm32-wasip2` command
//! exercising `std::fs` through wasi-libc (`tests/fixtures/fs.wasm`, built
//! by `tests/guests/build.sh`), linked with every other WASI p2 interface
//! from wasmtime-wasi one by one and the filesystem from
//! `legion_isolate::scratch`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use legion_isolate::account::MemoryAccount;
use legion_isolate::scratch::{self, Datetime, Scratch, ScratchCtx, ScratchView, RUN_DIR};

/// The coarse, turn-frozen wall clock an embedder passes: whole
/// milliseconds, so no file time has a sub-millisecond part.
const NOW: Datetime = Datetime {
    seconds: 1_800_000_000,
    nanoseconds: 123_000_000,
};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Engine, Store};
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

struct St {
    wasi: WasiCtx,
    table: ResourceTable,
    http: wasmtime_wasi_http::WasiHttpCtx,
    hooks: NoHttp,
    scratch: Scratch,
}

/// Outgoing HTTP is refused: the test only needs `wasi:http` linked.
struct NoHttp;

type HttpFuture<T> = Box<dyn std::future::Future<Output = wasmtime_wasi_http::Result<T>> + Send>;

impl wasmtime_wasi_http::WasiHttpHooks for NoHttp {
    fn send_request(
        &mut self,
        _request: http::Request<wasmtime_wasi_http::WasiBody>,
        _options: Option<wasmtime_wasi_http::RequestOptions>,
        _fut: HttpFuture<()>,
    ) -> HttpFuture<(http::Response<wasmtime_wasi_http::WasiBody>, HttpFuture<()>)> {
        Box::new(async { Err(wasmtime_wasi_http::Error::HttpRequestDenied) })
    }
}

impl wasmtime_wasi_http::WasiHttpView for St {
    fn http(&mut self) -> wasmtime_wasi_http::WasiHttpCtxView<'_> {
        wasmtime_wasi_http::WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}

impl WasiView for St {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl wasmtime_wasi_io::IoView for St {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl ScratchView for St {
    fn scratch(&mut self) -> ScratchCtx<'_> {
        ScratchCtx {
            scratch: &mut self.scratch,
            table: &mut self.table,
            cwd: RUN_DIR,
            now: NOW,
        }
    }
}

/// Every WASI p2 interface except the filesystem from wasmtime-wasi, and
/// the filesystem from the scratch tree.
fn linker(engine: &Engine) -> Linker<St> {
    use wasmtime_wasi::cli::{WasiCli, WasiCliView};
    use wasmtime_wasi::clocks::{WasiClocks, WasiClocksView};
    use wasmtime_wasi::p2::bindings::{cli, clocks, random, sockets};
    use wasmtime_wasi::random::WasiRandom;
    use wasmtime_wasi::sockets::{WasiSockets, WasiSocketsView};

    let mut l = Linker::new(engine);
    let l2 = &mut l;
    wasmtime_wasi_io::add_to_linker_async(l2).unwrap();
    clocks::wall_clock::add_to_linker::<St, WasiClocks>(l2, St::clocks).unwrap();
    clocks::monotonic_clock::add_to_linker::<St, WasiClocks>(l2, St::clocks).unwrap();
    random::random::add_to_linker::<St, WasiRandom>(l2, |t| t.ctx().ctx.random()).unwrap();
    random::insecure::add_to_linker::<St, WasiRandom>(l2, |t| t.ctx().ctx.random()).unwrap();
    random::insecure_seed::add_to_linker::<St, WasiRandom>(l2, |t| t.ctx().ctx.random()).unwrap();
    cli::exit::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::environment::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::stdin::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::stdout::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::stderr::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::terminal_input::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::terminal_output::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::terminal_stdin::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::terminal_stdout::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    cli::terminal_stderr::add_to_linker::<St, WasiCli>(l2, St::cli).unwrap();
    sockets::tcp_create_socket::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    sockets::instance_network::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    sockets::network::add_to_linker::<St, WasiSockets>(l2, &Default::default(), St::sockets)
        .unwrap();
    sockets::tcp::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    sockets::udp::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    sockets::udp_create_socket::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    sockets::ip_name_lookup::add_to_linker::<St, WasiSockets>(l2, St::sockets).unwrap();
    scratch::add_to_linker(l2).unwrap();
    l
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

struct Ran {
    stdout: String,
    stderr: String,
    exit: Result<(), String>,
    store: Store<St>,
}

fn store(
    engine: &Engine,
    scratch: Scratch,
    stdout: &MemoryOutputPipe,
    stderr: &MemoryOutputPipe,
) -> Store<St> {
    let mut b = WasiCtx::builder();
    b.stdout(stdout.clone()).stderr(stderr.clone());
    // What a host sets; wasi-libc does not read it yet (see below).
    b.initial_cwd(RUN_DIR);
    Store::new(
        engine,
        St {
            wasi: b.build(),
            table: ResourceTable::new(),
            http: wasmtime_wasi_http::WasiHttpCtx::new(),
            hooks: NoHttp,
            scratch,
        },
    )
}

/// Run a command component with `scratch` as its filesystem.
async fn run(component: &Path, scratch: Scratch) -> Ran {
    let engine = Engine::default();
    let component = Component::from_file(&engine, component).unwrap();
    let stdout = MemoryOutputPipe::new(64 * 1024);
    let stderr = MemoryOutputPipe::new(64 * 1024);
    let mut store = store(&engine, scratch, &stdout, &stderr);
    let linker = linker(&engine);
    let command =
        wasmtime_wasi::p2::bindings::Command::instantiate_async(&mut store, &component, &linker)
            .await
            .unwrap();
    let exit = match command.wasi_cli_run().call_run(&mut store).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(())) => Err("run returned an error".to_string()),
        Err(e) => match e.downcast_ref::<wasmtime_wasi::I32Exit>() {
            Some(x) if x.0 == 0 => Ok(()),
            Some(x) => Err(format!("exit {}", x.0)),
            None => Err(format!("{e:?}")),
        },
    };
    Ran {
        stdout: String::from_utf8_lossy(&stdout.contents()).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.contents()).into_owned(),
        exit,
        store,
    }
}

const LIMIT: u64 = 4 << 20;

fn staged(account: &Arc<MemoryAccount>) -> Scratch {
    let mut s = Scratch::new(account.clone());
    s.set_now(NOW);
    s.create_dir_all(RUN_DIR).unwrap();
    s.write_file("/legion/run/input.json", br#"{"n":1}"#)
        .unwrap();
    s.write_file("/legion/data/staged.txt", b"staged").unwrap();
    s
}

#[tokio::test]
async fn rust_std_fs_guest_runs_on_scratch_test() {
    let account = Arc::new(MemoryAccount::new(LIMIT));
    let ran = run(&fixture("fs.wasm"), staged(&account)).await;
    println!("{}", ran.stdout);
    assert!(ran.stderr.is_empty(), "stderr: {}", ran.stderr);
    let fails: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|l| !l.starts_with("ok ") && !l.starts_with("info "))
        .collect();
    assert_eq!(fails, vec!["done 16/16"], "stdout:\n{}", ran.stdout);
    assert_eq!(ran.exit, Ok(()));
    // wasi-libc ignores `initial-cwd` (the store sets it) and starts in `/`; the
    // `/` preopen, listed last, wins its tie with `.`, so relative paths
    // resolve from `/` until the guest changes directory. When a toolchain
    // update starts honoring `initial-cwd`, this line changes.
    assert!(
        ran.stdout.lines().any(|l| l == "info initial_cwd=/"),
        "stdout:\n{}",
        ran.stdout
    );
    // File times are exactly the clock the embedder passed: nothing finer
    // reaches the guest. The guest prints them as `secs.nanos`.
    for what in ["mtime", "staged_mtime"] {
        let line = format!("info {what}=1800000000.123000000");
        assert!(
            ran.stdout.lines().any(|l| l == line),
            "{line} missing; stdout:\n{}",
            ran.stdout
        );
    }
    assert_eq!(NOW.nanoseconds % 1_000_000, 0);
    // A `write` past the ceiling: wasi-libc reports every stream failure
    // as EIO (29); `set_len` past it is ENOSPC (checked by the guest).
    assert!(
        ran.stdout
            .lines()
            .any(|l| l == "info write_past_ceiling_errno=29"
                || l == "info write_past_ceiling_errno=51"),
        "stdout:\n{}",
        ran.stdout
    );

    // What the guest left, read back by the host.
    let s = &ran.store.data().scratch;
    assert_eq!(
        s.read_file("/legion/run/result.json", 1 << 20),
        Some((br#"{"ok":true}"#.to_vec(), false))
    );
    assert_eq!(
        s.read_file("/legion/run/out/report.txt", 3),
        Some((b"rep".to_vec(), true))
    );
    assert_eq!(
        s.read_file("/legion/run/input.json", 100),
        Some((br#"{"n":1}"#.to_vec(), false))
    );
    // Removed by the guest.
    assert_eq!(s.read_file("/legion/run/a/b/c/f2", 100), None);
    assert_eq!(s.read_file("/legion/run/big.bin", 100), None);
    // The ENOSPC check hit the ceiling and the bytes came back.
    assert!(account.refused());
    assert!(account.peak() <= LIMIT);
    assert_eq!(account.scratch(), s.used_bytes());
    assert!(s.used_bytes() < 1024, "used {}", s.used_bytes());
    drop(ran.store);
    // Dropping the store (its descriptors and the tree) gives every byte
    // back.
    assert_eq!(account.scratch(), 0);
}

/// The language images from the isolate runtime's Phase 0 spike
/// (componentize-py and StarlingMonkey), which import
/// `wasi:filesystem/{types,preopens}`: they instantiate against this
/// filesystem plus wasmtime-wasi's other interfaces (and `wasi:http`, which
/// the JS image imports for `fetch`), and Python's file I/O runs on the
/// scratch tree. Set `LEGION_ISOLATE_IMAGES` to the directory holding
/// `runner-py.wasm` and `runner-js.wasm` (each exports
/// `run: func(source: string, input: string) -> result<string, string>`).
#[tokio::test]
#[ignore = "needs the spike's image components"]
async fn language_images_run_on_scratch_test() {
    let dir = PathBuf::from(std::env::var("LEGION_ISOLATE_IMAGES").expect(
        "set LEGION_ISOLATE_IMAGES to the directory with runner-py.wasm and runner-js.wasm",
    ));
    let engine = Engine::default();
    let mut linker = linker(&engine);
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();

    let account = Arc::new(MemoryAccount::new(LIMIT));
    let py = Component::from_file(&engine, dir.join("runner-py.wasm")).unwrap();
    let pre = linker
        .instantiate_pre(&py)
        .expect("the Python image's imports are satisfied");
    let (out, err) = (
        MemoryOutputPipe::new(1 << 16),
        MemoryOutputPipe::new(1 << 16),
    );
    let mut st = store(&engine, staged(&account), &out, &err);
    let inst = pre.instantiate_async(&mut st).await.unwrap();
    let run = inst
        .get_typed_func::<(String, String), (Result<String, String>,)>(&mut st, "run")
        .unwrap();
    let source = r#"
import os
start = os.getcwd()
os.chdir("/legion/run")
with open("note.txt", "w") as f:
    f.write("from python")
os.makedirs("out/deep", exist_ok=True)
os.rename("note.txt", "out/deep/note.txt")
print(start, os.getcwd(), sorted(os.listdir(".")), os.path.getsize("input.json"),
      open("/legion/run/out/deep/note.txt").read(), open("../data/staged.txt").read())
try:
    open("missing")
except FileNotFoundError:
    print("FileNotFoundError")
try:
    os.rmdir("out")
except OSError as e:
    print("rmdir", e.errno)
"#;
    let (result,) = run
        .call_async(&mut st, (source.to_string(), String::new()))
        .await
        .unwrap();
    let printed = result.unwrap_or_else(|e| {
        panic!(
            "python failed: {e}\nstderr: {}",
            String::from_utf8_lossy(&err.contents())
        )
    });
    println!("python: {printed}");
    assert_eq!(
        printed,
        "/ /legion/run ['input.json', 'out'] 7 from python staged\nFileNotFoundError\nrmdir 55\n"
    );
    assert_eq!(
        st.data()
            .scratch
            .read_file("/legion/run/out/deep/note.txt", 100),
        Some((b"from python".to_vec(), false))
    );

    let js = Component::from_file(&engine, dir.join("runner-js.wasm")).unwrap();
    let pre = linker
        .instantiate_pre(&js)
        .expect("the JS image's imports are satisfied");
    let (out, err) = (
        MemoryOutputPipe::new(1 << 16),
        MemoryOutputPipe::new(1 << 16),
    );
    let mut st = store(&engine, staged(&account), &out, &err);
    let inst = pre.instantiate_async(&mut st).await.unwrap();
    let run = inst
        .get_typed_func::<(String, String), (Result<String, String>,)>(&mut st, "run")
        .unwrap();
    let (result,) = run
        .call_async(&mut st, ("return input + 1".to_string(), "4".to_string()))
        .await
        .unwrap();
    assert_eq!(result, Ok("41".to_string()));
}
