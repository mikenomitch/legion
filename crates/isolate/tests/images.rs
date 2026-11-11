//! The `js` and `python` language images (crates/isolate/images) under the
//! host's own guest context: `legion_isolate::module::linker` (every
//! import a host offers), `guest::Ctx` (the scratch filesystem, egress and
//! `legion:isolate` to a fake worker), and the `runner` export called the
//! way a host calls it.
//!
//! The images are not checked in and CI does not build them, so every test
//! here is ignored. Build them, then run:
//!
//! ```text
//! crates/isolate/images/build.sh
//! cargo test -p legion-isolate -j 3 --test images -- --ignored --nocapture
//! ```
//!
//! `LEGION_ISOLATE_IMAGES` names the directory holding `python.wasm`,
//! `js.wasm` and `manifest.json` (default `target/isolate-images` in the
//! workspace). `build.sh` also runs `manifest_write` here to fill in the
//! parts of `manifest.json` that need the image running: the Python module
//! list and the idle memory per instance.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use legion_isolate::engine::{self, EngineParams, PoolSize};
use legion_isolate::guest::{Ctx, GuestSetup, Ident, Invocation, Stop};
use legion_isolate::module;
use legion_isolate::scratch::RUN_DIR;
use legion_isolate::worker_api::WorkerApi;
use legion_isolate::world::{runner, Image as HostWorld, ImagePre as HostWorldPre};
use legion_proto::isolate_host::{
    vm_request, CheckpointHeader, EgressError, EgressErrorKind, EgressReply, EgressRequest,
    Header, HttpOutcome, Lifetime, LogBatch, Origin, SaveCheckpointReply, VmError, VmErrorKind,
    VmExecResult, VmReply, VmRequest,
};
use sha2::{Digest, Sha256};
use tonic::Status;
use wasmtime::component::Component;
use wasmtime::{Engine, Store};

const IGNORE: &str = "needs built images: crates/isolate/images/build.sh";

// ---------------------------------------------------------------------------
// Loading the images
// ---------------------------------------------------------------------------

fn images_dir() -> PathBuf {
    match std::env::var_os("LEGION_ISOLATE_IMAGES") {
        Some(d) => PathBuf::from(d),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/isolate-images"),
    }
}

/// Live instances the test engine's pool holds at once (the idle-memory
/// measurement keeps `IDLE_N` of one image alive).
const SLOTS: u32 = 400;
const IDLE_N: usize = 200;

fn engine() -> &'static Engine {
    static E: OnceLock<Engine> = OnceLock::new();
    E.get_or_init(|| {
        engine::host_engine(
            &EngineParams::default(),
            PoolSize {
                slots: SLOTS,
                // The CPython image needs a deeper stack than a host's
                // 128 KiB default for its first run's imports.
                async_stack_bytes: 1 << 20,
            },
        )
        .expect("the test engine")
    })
}

pub struct Image {
    pub name: &'static str,
    pub wasm: PathBuf,
    pub component: Component,
    pub pre: HostWorldPre<Ctx>,
}

/// Compile an image once per engine configuration and keep the artifact
/// beside it (`.cache/`), as a node keeps its derived `.cwasm`.
fn load(name: &'static str) -> Image {
    let dir = images_dir();
    let wasm = dir.join(format!("{name}.wasm"));
    let bytes = std::fs::read(&wasm).unwrap_or_else(|e| {
        panic!(
            "{}: {e}. Build the images first (crates/isolate/images/build.sh) or set \
             LEGION_ISOLATE_IMAGES to the directory that holds them",
            wasm.display()
        )
    });
    let engine = engine();
    let key = format!(
        "{:x}",
        Sha256::new()
            .chain_update(&bytes)
            .chain_update(engine::compat_key_for(engine, &EngineParams::default()))
            .finalize()
    );
    let cache = dir.join(".cache");
    std::fs::create_dir_all(&cache).unwrap();
    let cwasm = cache.join(format!("{name}-{}.cwasm", &key[..16]));
    if !cwasm.exists() {
        let t = Instant::now();
        let art = engine
            .precompile_component(&bytes)
            .unwrap_or_else(|e| panic!("compiling {name}: {e:#}"));
        legion_isolate::compile::write_atomic(&cwasm, &art).unwrap();
        eprintln!("compiled {name}.wasm in {:?}", t.elapsed());
    }
    // SAFETY: the artifact was produced above by this engine configuration.
    let component = unsafe { Component::deserialize_file(engine, &cwasm) }.unwrap();
    let linker = module::linker(engine).unwrap();
    let pre = linker
        .instantiate_pre(&component)
        .unwrap_or_else(|e| panic!("{name}: imports not satisfied by the host linker: {e:#}"));
    let pre = HostWorldPre::new(pre).unwrap();
    Image {
        name,
        wasm,
        component,
        pre,
    }
}

fn python() -> &'static Image {
    static I: OnceLock<Image> = OnceLock::new();
    I.get_or_init(|| load("python"))
}

fn js() -> &'static Image {
    static I: OnceLock<Image> = OnceLock::new();
    I.get_or_init(|| load("js"))
}

// ---------------------------------------------------------------------------
// A fake worker: egress and child VMs
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct FakeWorker {
    pub egress: Mutex<Vec<EgressRequest>>,
    pub vm: Mutex<Vec<String>>,
}

#[async_trait]
impl WorkerApi for FakeWorker {
    async fn egress(&self, req: EgressRequest) -> Result<EgressReply, Status> {
        self.egress.lock().unwrap().push(req.clone());
        if req.url.contains("denied.example") {
            return Ok(EgressReply {
                error: Some(EgressError {
                    kind: EgressErrorKind::EgressDenied as i32,
                    message: "not in the egress policy".into(),
                }),
                ..Default::default()
            });
        }
        let echo = serde_json::json!({
            "method": req.method,
            "url": req.url,
            "body": String::from_utf8_lossy(&req.body),
            "headers": req.headers.iter().map(|h| (h.name.clone(), String::from_utf8_lossy(&h.value).into_owned())).collect::<BTreeMap<_, _>>(),
        });
        let status = if req.url.contains("/missing") {
            404
        } else {
            200
        };
        Ok(EgressReply {
            status,
            headers: vec![
                Header {
                    name: "content-type".into(),
                    value: b"application/json".to_vec(),
                },
                Header {
                    name: "x-upstream".into(),
                    value: b"fake".to_vec(),
                },
            ],
            body: serde_json::to_vec(&echo).unwrap(),
            error: None,
        })
    }

    async fn vm(&self, req: VmRequest) -> Result<VmReply, Status> {
        let op = req.op.expect("an op");
        let mut log = self.vm.lock().unwrap();
        Ok(match op {
            vm_request::Op::Acquire(_) => {
                log.push("acquire".into());
                VmReply {
                    id: "child-1".into(),
                    ..Default::default()
                }
            }
            vm_request::Op::Attach(a) => {
                log.push(format!("attach {}", a.id));
                if a.id == "child-1" {
                    VmReply {
                        id: a.id,
                        ..Default::default()
                    }
                } else {
                    VmReply {
                        error: Some(VmError {
                            kind: VmErrorKind::VmChildGone as i32,
                            message: "no such child".into(),
                        }),
                        ..Default::default()
                    }
                }
            }
            vm_request::Op::Exec(e) => {
                log.push(format!("exec {} {}", e.id, e.argv.join(" ")));
                VmReply {
                    exec: Some(VmExecResult {
                        exit: 7,
                        stdout: format!("ran {}", e.argv.join(" ")).into_bytes(),
                        stderr: e.stdin,
                        timed_out: false,
                    }),
                    ..Default::default()
                }
            }
            vm_request::Op::Put(p) => {
                log.push(format!("put {} {}", p.path, String::from_utf8_lossy(&p.content)));
                VmReply::default()
            }
            vm_request::Op::Get(g) => {
                log.push(format!("get {}", g.path));
                VmReply {
                    content: b"file from vm".to_vec(),
                    ..Default::default()
                }
            }
            vm_request::Op::Fetch(f) => {
                log.push(format!("fetch {}", f.port));
                VmReply {
                    response: Some(HttpOutcome {
                        status: 201,
                        headers: vec![Header {
                            name: "x-vm".into(),
                            value: b"yes".to_vec(),
                        }],
                        body: b"vm body".to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            }
            vm_request::Op::Resize(r) => {
                log.push(format!("resize {}", r.memory_mib));
                VmReply::default()
            }
            vm_request::Op::Release(r) => {
                log.push(format!("release {}", r.id));
                VmReply::default()
            }
        })
    }

    async fn save_checkpoint(
        &self,
        _header: CheckpointHeader,
        _body: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status> {
        Err(Status::unimplemented("not in these tests"))
    }

    async fn load_checkpoint(
        &self,
        _origin: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status> {
        Ok(None)
    }

    async fn log(&self, _batch: LogBatch) -> Result<(), Status> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Instances and runs
// ---------------------------------------------------------------------------

pub struct Opts {
    pub lifetime: Lifetime,
    pub has_vm: bool,
    pub egress: bool,
    pub resumed: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            lifetime: Lifetime::Request,
            has_vm: false,
            egress: true,
            resumed: false,
        }
    }
}

pub struct Inst {
    pub store: Store<Ctx>,
    pub world: HostWorld,
    pub worker: Arc<FakeWorker>,
}

#[derive(Debug, Default)]
pub struct Ran {
    /// None when the run was stopped or trapped.
    pub exit: Option<i32>,
    pub result: Option<String>,
    pub result_json: Option<String>,
    pub stdout: String,
    pub stderr: String,
    pub stop: Option<Stop>,
    pub trap: Option<String>,
    pub micros: u128,
}

impl Ran {
    #[track_caller]
    fn ok(&self) -> &Self {
        assert_eq!(
            self.exit,
            Some(0),
            "run failed: stop={:?} trap={:?}\nstdout:\n{}\nstderr:\n{}",
            self.stop,
            self.trap,
            self.stdout,
            self.stderr
        );
        self
    }
}

pub async fn instance(image: &Image, opts: Opts) -> Inst {
    let worker = Arc::new(FakeWorker::default());
    let stateful = opts.lifetime != Lifetime::Request;
    let ctx = Ctx::new(GuestSetup {
        ident: Ident {
            job: "code-runner".into(),
            task: "run".into(),
            lifetime: opts.lifetime,
            tenant: (opts.lifetime == Lifetime::Tenant).then(|| "acme".to_string()),
            session: (opts.lifetime == Lifetime::Session).then(|| "s-1".to_string()),
        },
        env: vec![
            ("LEGION_RUN_DIR".into(), RUN_DIR.into()),
            ("IMAGE_TEST".into(), "from-the-host".into()),
        ],
        args: vec![image.name.to_string()],
        memory_max: 256 << 20,
        has_vm: opts.has_vm,
        egress: opts.egress,
        max_body: 1 << 20,
        state_quota: 4096,
        state: stateful.then(Default::default),
        resumed: opts.resumed,
        worker: worker.clone(),
    });
    let mut store = Store::new(engine(), ctx);
    store.limiter(|c| &mut c.limiter);
    store.set_epoch_deadline(u64::MAX / 2);
    let world = image
        .pre
        .instantiate_async(&mut store)
        .await
        .unwrap_or_else(|e| panic!("instantiating {}: {e:#}", image.name));
    Inst {
        store,
        world,
        worker,
    }
}

impl Inst {
    /// One run: empty the run directory, stage `files` (and `input.json`),
    /// call `runner.run`.
    pub async fn run(
        &mut self,
        files: &[(&str, &str)],
        entry: Option<&str>,
        input: Option<&str>,
        args: &[&str],
    ) -> Ran {
        {
            let ctx = self.store.data_mut();
            ctx.scratch.clear();
            ctx.scratch.create_dir_all(RUN_DIR).unwrap();
            for (name, body) in files {
                let path = format!("{RUN_DIR}/{name}");
                if let Some((dir, _)) = path.rsplit_once('/') {
                    ctx.scratch.create_dir_all(dir).unwrap();
                }
                ctx.scratch.write_file(&path, body.as_bytes()).unwrap();
            }
            if let Some(i) = input {
                ctx.scratch
                    .write_file(&format!("{RUN_DIR}/input.json"), i.as_bytes())
                    .unwrap();
            }
            let inv = Arc::new(Invocation::unlimited(Origin {
                job_id: "code-runner".into(),
                task_name: "run".into(),
                isolate_id: "i-1".into(),
                ..Default::default()
            }));
            ctx.begin(inv, 1 << 20);
        }
        let req = runner::Request {
            entry: entry.map(str::to_string),
            input: input.map(str::to_string),
            args: args.iter().map(|s| s.to_string()).collect(),
        };
        let t = Instant::now();
        let got = self
            .world
            .legion_isolate_runner()
            .call_run(&mut self.store, &req)
            .await;
        let micros = t.elapsed().as_micros();
        let ctx = self.store.data_mut();
        let printed = ctx.out.take();
        let mut ran = Ran {
            stdout: String::from_utf8_lossy(&printed.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&printed.stderr).into_owned(),
            micros,
            ..Default::default()
        };
        match got {
            Ok(outcome) => {
                ran.exit = Some(outcome.exit);
                ran.result = outcome.result;
            }
            Err(e) => {
                ran.stop = ctx.inv.stopped();
                if ran.stop.is_none() {
                    ran.trap = Some(format!("{e:?}"));
                }
            }
        }
        ran.result_json = ctx
            .scratch
            .read_file(&format!("{RUN_DIR}/result.json"), 1 << 20)
            .map(|(b, _)| String::from_utf8_lossy(&b).into_owned());
        ran
    }

    /// Run `source` as the entry `name`.
    pub async fn src(&mut self, name: &str, source: &str) -> Ran {
        self.run(&[(name, source)], Some(name), None, &[]).await
    }

    pub fn state(&self) -> BTreeMap<String, Vec<u8>> {
        self.store
            .data()
            .state
            .as_ref()
            .map(|s| s.map.clone())
            .unwrap_or_default()
    }
}

#[track_caller]
fn assert_stopped(ran: &Ran, detail: &str) {
    let stop = ran.stop.as_ref().unwrap_or_else(|| {
        panic!(
            "expected an unsupported stop ({detail}); got exit {:?} trap {:?}\nstdout:\n{}\nstderr:\n{}",
            ran.exit, ran.trap, ran.stdout, ran.stderr
        )
    });
    assert_eq!(stop.reason, "unsupported", "{stop:?}");
    assert_eq!(stop.detail, detail);
}

fn p50(mut v: Vec<u128>) -> (u128, u128) {
    v.sort_unstable();
    (v[v.len() / 2], v[v.len() * 99 / 100])
}

// ---------------------------------------------------------------------------
// Python
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_hello_and_exit_codes_test() {
    let _ = IGNORE;
    let mut i = instance(python(), Opts::default()).await;
    let ran = i.src("main.py", "print('hello from python')\n").await;
    ran.ok();
    assert_eq!(ran.stdout, "hello from python\n");
    assert_eq!(ran.result, None);

    let ran = i.src("main.py", "import sys\nprint('bye')\nsys.exit(3)\n").await;
    assert_eq!(ran.exit, Some(3));
    assert_eq!(ran.stdout, "bye\n");

    let ran = i.src("main.py", "raise SystemExit('bad input')\n").await;
    assert_eq!(ran.exit, Some(1));
    assert_eq!(ran.stderr, "bad input\n");

    let ran = i.src("main.py", "import sys\nsys.exit(None)\n").await;
    assert_eq!(ran.exit, Some(0));

    let src = "def f(x):\n    return 1 / x\n\nprint('before')\nf(0)\n";
    let ran = i.src("snippet.py", src).await;
    assert_eq!(ran.exit, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "before\n");
    assert!(
        ran.stderr.starts_with("Traceback (most recent call last):\n"),
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains("File \"snippet.py\", line 5, in <module>"),
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains("File \"snippet.py\", line 2, in f"),
        "{}",
        ran.stderr
    );
    assert!(ran.stderr.contains("    return 1 / x"), "{}", ran.stderr);
    assert!(
        ran.stderr.ends_with("ZeroDivisionError: division by zero\n"),
        "{}",
        ran.stderr
    );
    assert!(!ran.stderr.contains("_legion_runner"), "{}", ran.stderr);

    let ran = i.src("main.py", "x = (\n").await;
    assert_eq!(ran.exit, Some(1));
    assert!(ran.stderr.contains("SyntaxError"), "{}", ran.stderr);
    assert!(ran.stderr.contains("main.py"), "{}", ran.stderr);

    let ran = i.run(&[], None, None, &[]).await;
    assert_eq!(ran.exit, Some(2));
    assert!(ran.stderr.contains("no entry file was given"), "{}", ran.stderr);

    let ran = i.run(&[], Some("missing.py"), None, &[]).await;
    assert_eq!(ran.exit, Some(2));
    assert!(ran.stderr.contains("can't open file"), "{}", ran.stderr);
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_input_result_and_files_test() {
    let mut i = instance(python(), Opts::default()).await;
    let src = r#"
import json, os, sys, legion
result = {"n": legion.input["n"] * 2, "argv": sys.argv, "cwd": os.getcwd(),
          "env": os.environ.get("IMAGE_TEST"), "run_dir": os.environ.get("LEGION_RUN_DIR"),
          "from_file": json.load(open("input.json"))["n"]}
"#;
    let ran = i
        .run(&[("main.py", src)], Some("main.py"), Some(r#"{"n": 21}"#), &["a", "b"])
        .await;
    ran.ok();
    let v: serde_json::Value = serde_json::from_str(ran.result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"n": 42, "argv": ["main.py", "a", "b"], "cwd": "/legion/run",
                           "env": "from-the-host", "run_dir": "/legion/run", "from_file": 21})
    );

    // No `result` variable: the run writes result.json, which the host
    // falls back to.
    let src = r#"
import json, os
os.makedirs("out/deep", exist_ok=True)
with open("out/deep/note.txt", "w") as f:
    f.write("written")
print(open("out/deep/note.txt").read(), sorted(os.listdir(".")))
with open("result.json", "w") as f:
    json.dump({"ok": True}, f)
"#;
    let ran = i.src("main.py", src).await;
    ran.ok();
    assert_eq!(ran.result, None);
    assert_eq!(ran.result_json.as_deref(), Some(r#"{"ok": true}"#));
    assert_eq!(ran.stdout, "written ['main.py', 'out', 'result.json']\n");

    // A helper module in the run directory imports, and the next run sees
    // the next run's file, not the cached module.
    let ran = i
        .run(
            &[("helper.py", "X = 1\n"), ("main.py", "import helper\nresult = helper.X\n")],
            Some("main.py"),
            None,
            &[],
        )
        .await;
    assert_eq!(ran.ok().result.as_deref(), Some("1"));
    let ran = i
        .run(
            &[("helper.py", "X = 2\n"), ("main.py", "import helper\nresult = helper.X\n")],
            Some("main.py"),
            None,
            &[],
        )
        .await;
    assert_eq!(ran.ok().result.as_deref(), Some("2"));

    // A result that is not JSON is not reported, with a note on stderr.
    let ran = i.src("main.py", "result = object()\n").await;
    ran.ok();
    assert_eq!(ran.result, None);
    assert!(ran.stderr.contains("not JSON-serializable"), "{}", ran.stderr);
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_globals_persist_per_instance_test() {
    let src = "try:\n    count += 1\nexcept NameError:\n    count = 1\nresult = count\n";
    let mut a = instance(
        python(),
        Opts {
            lifetime: Lifetime::Tenant,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(a.src("main.py", src).await.ok().result.as_deref(), Some("1"));
    assert_eq!(a.src("main.py", src).await.ok().result.as_deref(), Some("2"));
    // A run that sets no `result` reports none, not the previous run's.
    assert_eq!(a.src("main.py", "x = 1\n").await.ok().result, None);
    let mut b = instance(python(), Opts::default()).await;
    assert_eq!(b.src("main.py", src).await.ok().result.as_deref(), Some("1"));
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_unsupported_stops_the_run_test() {
    let cases = [
        ("import subprocess\nsubprocess.run(['ls'])\n", "subprocess.run: processes are not available in an isolate"),
        ("import subprocess\nsubprocess.check_output(['ls'])\n", "subprocess.check_output: processes are not available in an isolate"),
        ("import subprocess\nsubprocess.Popen(['ls'])\n", "subprocess.Popen: processes are not available in an isolate"),
        ("from subprocess import call\ncall('ls')\n", "subprocess.call: processes are not available in an isolate"),
        ("import os\nos.system('ls')\n", "os.system: processes are not available in an isolate"),
        ("import os\nos.popen('ls')\n", "os.popen: processes are not available in an isolate"),
        ("import os\nos.fork()\n", "os.fork: processes are not available in an isolate"),
        ("import os\nos.execv('/bin/ls', ['ls'])\n", "os.execv: processes are not available in an isolate"),
        ("import os\nos.spawnv(os.P_WAIT if hasattr(os, 'P_WAIT') else 0, '/bin/ls', ['ls'])\n", "os.spawnv: processes are not available in an isolate"),
        ("import multiprocessing\nmultiprocessing.Process(target=print).start()\n", "multiprocessing.Process.start: processes are not available in an isolate"),
        ("import multiprocessing\nmultiprocessing.Pool(2)\n", "multiprocessing.Pool: processes are not available in an isolate"),
        ("from concurrent.futures import ProcessPoolExecutor\nProcessPoolExecutor()\n", "concurrent.futures.ProcessPoolExecutor: processes are not available in an isolate"),
        ("import threading\nthreading.Thread(target=print).start()\n", "threading.Thread.start: threads are not available in an isolate"),
        ("import ctypes\nctypes.CDLL('libc.so.6')\n", "ctypes.CDLL: native libraries are not available in an isolate"),
        ("import socket\nsocket.socket()\n", "socket.socket: sockets are not available in an isolate; use legion.fetch or run with --tier vm"),
        ("import socket\nsocket.create_connection(('example.com', 80))\n", "socket.create_connection: sockets are not available in an isolate; use legion.fetch or run with --tier vm"),
        ("import socket\nsocket.getaddrinfo('example.com', 80)\n", "socket.getaddrinfo: sockets are not available in an isolate; use legion.fetch or run with --tier vm"),
        ("import http.client\nc = http.client.HTTPConnection('example.com')\nc.putrequest('GET', '/')\nc.endheaders()\n", "http.client.HTTPConnection.connect: sockets are not available in an isolate; use legion.fetch or run with --tier vm"),
    ];
    for (src, detail) in cases {
        let mut i = instance(python(), Opts::default()).await;
        let ran = i.src("main.py", &format!("print('started')\n{src}print('not reached')\n")).await;
        assert_stopped(&ran, detail);
        assert_eq!(ran.stdout, "started\n", "{src}");
    }
    // Importing is fine; only use stops the run.
    let mut i = instance(python(), Opts::default()).await;
    let ran = i
        .src(
            "main.py",
            "import subprocess, socket, multiprocessing, threading, ctypes, os\n\
             import urllib.request, http.client, asyncio, concurrent.futures\n\
             print(hasattr(os, 'fork'), threading.current_thread().name)\n",
        )
        .await;
    ran.ok();
    assert_eq!(ran.stdout, "True MainThread\n");
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_state_test() {
    let src = r#"
import legion
before = legion.state.get("count")
n = int(before or b"0") + 1
legion.state.set("count", str(n))
legion.state.set("blob", b"\x00\x01")
legion.state.delete("missing")
info = legion.info()
result = {"n": n, "keys": sorted(legion.state.keys()), "blob": list(legion.state.get("blob")),
          "lifetime": info["lifetime"], "session": info["session"], "resumed": info["resumed"]}
try:
    legion.state.set("big", b"x" * 10000)
except legion.StateError as e:
    result["quota"] = [e.kind, e.quota]
"#;
    let mut i = instance(
        python(),
        Opts {
            lifetime: Lifetime::Session,
            ..Default::default()
        },
    )
    .await;
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.py", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"n": 1, "keys": ["blob", "count"], "blob": [0, 1], "lifetime": "session",
                           "session": "s-1", "resumed": false, "quota": ["quota", 4096]})
    );
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.py", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(v["n"], 2);
    assert_eq!(i.state().get("count").map(Vec::as_slice), Some(&b"2"[..]));

    // A request isolate has no state.
    let mut r = instance(python(), Opts::default()).await;
    let ran = r
        .src(
            "main.py",
            "import legion\ntry:\n    legion.state.get('k')\nexcept legion.StateError as e:\n    result = [e.kind, str(e)]\n",
        )
        .await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(v[0], "not-available");
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_fetch_test() {
    let src = r#"
import legion, json, urllib.request, urllib.error, http.client
r = legion.fetch("http://payments/v1/charge?x=1", method="POST", headers={"X-Test": "1"}, body='{"a":1}')
echo = r.json()
out = {"status": r.status, "upstream": r.header("x-upstream"), "method": echo["method"],
       "url": echo["url"], "body": echo["body"], "hdr": echo["headers"].get("x-test")}
with urllib.request.urlopen("https://api.example.com/items") as resp:
    out["urllib"] = [resp.status, json.load(resp)["url"], resp.headers["x-upstream"]]
try:
    urllib.request.urlopen("http://api.example.com/missing")
except urllib.error.HTTPError as e:
    out["urllib_404"] = e.code
c = http.client.HTTPConnection("api.example.com", 8080)
c.request("PUT", "/thing", body=b"payload", headers={"Content-Type": "text/plain"})
resp = c.getresponse()
out["http_client"] = [resp.status, json.loads(resp.read())["url"]]
try:
    legion.fetch("https://denied.example/")
except legion.FetchError as e:
    out["denied"] = [e.kind, str(e)]
result = out
"#;
    let mut i = instance(python(), Opts::default()).await;
    let ran = i.src("main.py", src).await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(v["status"], 200, "{v}");
    assert_eq!(v["upstream"], "fake");
    assert_eq!(v["method"], "POST");
    assert_eq!(v["url"], "http://payments/v1/charge?x=1");
    assert_eq!(v["body"], r#"{"a":1}"#);
    assert_eq!(v["hdr"], "1");
    assert_eq!(
        v["urllib"],
        serde_json::json!([200, "https://api.example.com/items", "fake"])
    );
    assert_eq!(v["urllib_404"], 404);
    assert_eq!(
        v["http_client"],
        serde_json::json!([200, "http://api.example.com:8080/thing"])
    );
    assert_eq!(v["denied"][0], "http-request-denied", "{v}");
    let sent = i.worker.egress.lock().unwrap();
    assert_eq!(sent.len(), 5, "{sent:?}");
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_vm_test() {
    let src = r#"
import legion
m = legion.vm.acquire()
r = m.exec(["echo", "hi"], stdin=b"in")
m.put("/work/a.txt", "data")
got = m.get("/work/a.txt")
resp = m.fetch(8080, "POST", "/x", {"a": "b"}, b"body")
m.resize(512)
again = legion.vm.attach(m.id)
try:
    legion.vm.attach("gone")
except legion.VmError as e:
    gone = [e.kind, str(e)]
m.release()
result = {"id": m.id, "exit": r.exit, "stdout": r.stdout.decode(), "stderr": r.stderr.decode(),
          "timed_out": r.timed_out, "got": got.decode(), "fetch": [resp.status, resp.text(), resp.header("x-vm")],
          "again": again.id, "gone": gone}
"#;
    let mut i = instance(
        python(),
        Opts {
            has_vm: true,
            ..Default::default()
        },
    )
    .await;
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.py", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"id": "child-1", "exit": 7, "stdout": "ran echo hi", "stderr": "in",
            "timed_out": false, "got": "file from vm", "fetch": [201, "vm body", "yes"],
            "again": "child-1", "gone": ["child-gone", "legion.vm.attach: child-gone: no such child"]})
    );
    assert_eq!(
        *i.worker.vm.lock().unwrap(),
        vec![
            "acquire",
            "exec child-1 echo hi",
            "put /work/a.txt data",
            "get /work/a.txt",
            "fetch 8080",
            "resize 512",
            "attach child-1",
            "attach gone",
            "release child-1"
        ]
    );

    let mut n = instance(python(), Opts::default()).await;
    let ran = n
        .src(
            "main.py",
            "import legion\ntry:\n    legion.vm.acquire()\nexcept legion.VmError as e:\n    result = [e.kind, str(e)]\n",
        )
        .await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!(["not-configured", "legion.vm.acquire: this task has no VM task (set config.vm.task)"])
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_restore_hygiene_test() {
    let src = "import random, os, uuid, secrets\n\
               result = [random.random(), os.urandom(8).hex(), str(uuid.uuid4()), secrets.token_hex(8), random.getrandbits(64)]\n";
    let mut seen = Vec::new();
    for _ in 0..2 {
        let mut i = instance(python(), Opts::default()).await;
        let v: Vec<serde_json::Value> =
            serde_json::from_str(i.src("main.py", src).await.ok().result.as_deref().unwrap())
                .unwrap();
        seen.push(v);
    }
    for k in 0..seen[0].len() {
        assert_ne!(seen[0][k], seen[1][k], "value {k} repeated across instances: {seen:?}");
    }
}

// ---------------------------------------------------------------------------
// Experiments that decide what the runner stubs (kept as checks)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_probe_test() {
    let mut i = instance(python(), Opts::default()).await;
    let ran = i
        .src(
            "main.py",
            "import asyncio, sys, os\n\
             async def main():\n    await asyncio.sleep(0)\n    return 5\n\
             print('asyncio', asyncio.run(main()))\n\
             print(sys.version)\n",
        )
        .await;
    println!("{ran:#?}");
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_probe_test() {
    for _ in 0..2 {
        let mut i = instance(js(), Opts::default()).await;
        let ran = i
            .run(
                &[
                    ("mod.mjs", "export const x = 41;\n"),
                    (
                        "main.js",
                        "console.log('hello', Math.random(), crypto.randomUUID());\nconsole.error('to stderr');\n\
                         let m; try { m = await import('/legion/run/mod.mjs'); console.log('import abs', m.x); } catch (e) { console.log('import failed', String(e)); }\n\
                         try { m = await import('./mod.mjs'); console.log('import rel', m.x); } catch (e) { console.log('import rel failed', String(e)); }\n\
                         console.log(typeof setTimeout, typeof fetch, typeof Worker, Date.now());\n\
                         function f() { throw new Error('boom'); }\nf();\n",
                    ),
                ],
                Some("main.js"),
                Some("{\"a\":1}"),
                &[],
            )
            .await;
        println!("{ran:#?}");
    }
}
