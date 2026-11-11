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
    vm_request, CheckpointHeader, EgressError, EgressErrorKind, EgressReply, EgressRequest, Header,
    HttpOutcome, Lifetime, LogBatch, Origin, SaveCheckpointReply, VmError, VmErrorKind,
    VmExecResult, VmReply, VmRequest,
};
use sha2::{Digest, Sha256};
use tonic::Status;
use wasmtime::component::Component;
use wasmtime::{Engine, Store};

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
        // Artifacts of earlier builds of this image are stale.
        for old in std::fs::read_dir(&cache).unwrap().flatten() {
            let n = old.file_name().to_string_lossy().into_owned();
            if n.starts_with(&format!("{name}-")) && n.ends_with(".cwasm") {
                let _ = std::fs::remove_file(old.path());
            }
        }
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
                log.push(format!(
                    "put {} {}",
                    p.path,
                    String::from_utf8_lossy(&p.content)
                ));
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
    let _g = shared();
    let mut i = instance(python(), Opts::default()).await;
    let ran = i.src("main.py", "print('hello from python')\n").await;
    ran.ok();
    assert_eq!(ran.stdout, "hello from python\n");
    assert_eq!(ran.result, None);

    let ran = i
        .src("main.py", "import sys\nprint('bye')\nsys.exit(3)\n")
        .await;
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
        ran.stderr
            .starts_with("Traceback (most recent call last):\n"),
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr
            .contains("File \"snippet.py\", line 5, in <module>"),
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
        ran.stderr
            .ends_with("ZeroDivisionError: division by zero\n"),
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
    assert!(
        ran.stderr.contains("no entry file was given"),
        "{}",
        ran.stderr
    );

    let ran = i.run(&[], Some("missing.py"), None, &[]).await;
    assert_eq!(ran.exit, Some(2));
    assert!(ran.stderr.contains("can't open file"), "{}", ran.stderr);
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_input_result_and_files_test() {
    let _g = shared();
    let mut i = instance(python(), Opts::default()).await;
    let src = r#"
import json, os, sys, legion
result = {"n": legion.input["n"] * 2, "argv": sys.argv, "cwd": os.getcwd(),
          "env": os.environ.get("IMAGE_TEST"), "run_dir": os.environ.get("LEGION_RUN_DIR"),
          "from_file": json.load(open("input.json"))["n"]}
"#;
    let ran = i
        .run(
            &[("main.py", src)],
            Some("main.py"),
            Some(r#"{"n": 21}"#),
            &["a", "b"],
        )
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
    assert_eq!(ran.stdout, "written ['main.py', 'out']\n");

    // A helper module in the run directory imports, and the next run sees
    // the next run's file, not the cached module.
    let ran = i
        .run(
            &[
                ("helper.py", "X = 1\n"),
                ("main.py", "import helper\nresult = helper.X\n"),
            ],
            Some("main.py"),
            None,
            &[],
        )
        .await;
    assert_eq!(ran.ok().result.as_deref(), Some("1"));
    let ran = i
        .run(
            &[
                ("helper.py", "X = 2\n"),
                ("main.py", "import helper\nresult = helper.X\n"),
            ],
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
    assert!(
        ran.stderr.contains("not JSON-serializable"),
        "{}",
        ran.stderr
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_globals_persist_per_instance_test() {
    let _g = shared();
    let src = "try:\n    count += 1\nexcept NameError:\n    count = 1\nresult = count\n";
    let mut a = instance(
        python(),
        Opts {
            lifetime: Lifetime::Tenant,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        a.src("main.py", src).await.ok().result.as_deref(),
        Some("1")
    );
    assert_eq!(
        a.src("main.py", src).await.ok().result.as_deref(),
        Some("2")
    );
    // A run that sets no `result` reports none, not the previous run's.
    assert_eq!(a.src("main.py", "x = 1\n").await.ok().result, None);
    let mut b = instance(python(), Opts::default()).await;
    assert_eq!(
        b.src("main.py", src).await.ok().result.as_deref(),
        Some("1")
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_unsupported_stops_the_run_test() {
    let _g = shared();
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
        let ran = i
            .src(
                "main.py",
                &format!("print('started')\n{src}print('not reached')\n"),
            )
            .await;
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
    let _g = shared();
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
    let _g = shared();
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
    let _g = shared();
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
        serde_json::json!([
            "not-configured",
            "legion.vm.acquire: this task has no VM task (set config.vm.task)"
        ])
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_restore_hygiene_test() {
    let _g = shared();
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
        assert_ne!(
            seen[0][k], seen[1][k],
            "value {k} repeated across instances: {seen:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Experiments that decide what the runner stubs (kept as checks)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_stdlib_test() {
    let _g = shared();
    let mut i = instance(python(), Opts::default()).await;
    // Modules the standard library imports lazily inside functions must be
    // in the image too (time.strptime imports _strptime on first use).
    let src = r#"
import asyncio, datetime, time, xml.etree.ElementTree as ET, email.mime.text, sqlite3, zlib, decimal
async def tick(n):
    await asyncio.sleep(0.002)
    return n
async def main():
    a, b = await asyncio.gather(tick(2), tick(3))
    return a + b
db = sqlite3.connect(":memory:")
db.execute("create table t (x)")
db.execute("insert into t values (41)")
result = {
    "asyncio": asyncio.run(main()),
    "strptime": datetime.datetime.strptime("2026-11-11", "%Y-%m-%d").year,
    "time_strptime": time.strptime("2026", "%Y").tm_year,
    "xml": ET.fromstring("<a><b>x</b></a>").find("b").text,
    "email": email.mime.text.MIMEText("hi")["Content-Type"],
    "sqlite": db.execute("select x + 1 from t").fetchone()[0],
    "zlib": len(zlib.compress(b"a" * 100)) < 100,
    "decimal": str(decimal.Decimal("1.1") + decimal.Decimal("2.2")),
}
"#;
    let ran = i.src("main.py", src).await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"asyncio": 5, "strptime": 2026, "time_strptime": 2026, "xml": "x",
            "email": "text/plain; charset=\"us-ascii\"", "sqlite": 42, "zlib": true, "decimal": "3.3"})
    );
}

// ---------------------------------------------------------------------------
// JavaScript
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_hello_and_exit_codes_test() {
    let _g = shared();
    let mut i = instance(js(), Opts::default()).await;
    let ran = i.src("main.js", "console.log('hello from js');\n").await;
    ran.ok();
    assert_eq!(ran.stdout, "hello from js\n");
    assert_eq!(ran.result, None);

    let ran = i
        .src("main.js", "console.log('bye');\nconsole.error('oops');\nprocess.exit(3);\nconsole.log('not reached');\n")
        .await;
    assert_eq!(ran.exit, Some(3));
    assert_eq!(ran.stdout, "bye\n");
    assert_eq!(ran.stderr, "oops\n");

    let src =
        "function f(x) {\n  throw new Error('boom ' + x);\n}\nconsole.log('before');\nf(1);\n";
    let ran = i.src("snippet.js", src).await;
    assert_eq!(ran.exit, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "before\n");
    let lines: Vec<&str> = ran.stderr.lines().collect();
    assert_eq!(lines.len(), 3, "{}", ran.stderr);
    assert_eq!(lines[0], "Uncaught Error: boom 1");
    assert_eq!(lines[1], "    at f (snippet.js:2:9)");
    assert!(
        lines[2].starts_with("    at <anonymous> (snippet.js:5:"),
        "{}",
        ran.stderr
    );

    // A rejected await is an uncaught error too.
    let ran = i
        .src("main.js", "await Promise.reject(new TypeError('nope'));\n")
        .await;
    assert_eq!(ran.exit, Some(1));
    assert!(
        ran.stderr.starts_with("Uncaught TypeError: nope\n"),
        "{}",
        ran.stderr
    );

    let ran = i.src("main.js", "throw 'a string';\n").await;
    assert_eq!(ran.exit, Some(1));
    assert_eq!(ran.stderr, "Uncaught a string\n");

    let ran = i.src("main.js", "let x = (;\n").await;
    assert_eq!(ran.exit, Some(1));
    assert!(
        ran.stderr.starts_with("Uncaught SyntaxError: "),
        "{}",
        ran.stderr
    );
    assert!(ran.stderr.contains("main.js:1:"), "{}", ran.stderr);

    let ran = i.run(&[], None, None, &[]).await;
    assert_eq!(ran.exit, Some(2));
    assert!(
        ran.stderr.contains("no entry file was given"),
        "{}",
        ran.stderr
    );
    let ran = i.run(&[], Some("missing.js"), None, &[]).await;
    assert_eq!(ran.exit, Some(2));
    assert!(ran.stderr.contains("can't open file"), "{}", ran.stderr);
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_input_result_and_files_test() {
    let _g = shared();
    let mut i = instance(js(), Opts::default()).await;
    // `return` from the entry is the result; top-level await works.
    let src = "await new Promise((r) => setTimeout(r, 0));\n\
               const fromFile = JSON.parse(legion.fs.readText('input.json')).n;\n\
               return { n: legion.input.n * 2, argv: process.argv, env: process.env.IMAGE_TEST,\n\
                        runDir: process.env.LEGION_RUN_DIR, fromFile };\n";
    let ran = i
        .run(
            &[("main.js", src)],
            Some("main.js"),
            Some(r#"{"n": 21}"#),
            &["a", "b"],
        )
        .await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"n": 42, "argv": ["js", "/legion/run/main.js", "a", "b"],
                           "env": "from-the-host", "runDir": "/legion/run", "fromFile": 21})
    );
    // A top-level `result` binding, and `globalThis.result`.
    let ran = i.src("main.js", "const result = { a: [1, 2] };\n").await;
    assert_eq!(ran.ok().result.as_deref(), Some(r#"{"a":[1,2]}"#));
    let ran = i.src("main.js", "globalThis.result = 'g';\n").await;
    assert_eq!(ran.ok().result.as_deref(), Some(r#""g""#));
    // No result at all: the host falls back to result.json.
    let src = "legion.fs.mkdir('out/deep', { recursive: true });\n\
               legion.fs.writeFile('out/deep/note.txt', 'written');\n\
               legion.fs.writeFile('/legion/run/result.json', JSON.stringify({ ok: true }));\n\
               legion.fs.writeFile('gone.txt', new Uint8Array([1, 2]));\n\
               legion.fs.remove('gone.txt');\n\
               let missing;\n\
               try { legion.fs.readFile('nope.txt'); } catch (e) { missing = [e.name, e.code]; }\n\
               console.log(legion.fs.readText('out/deep/note.txt'), JSON.stringify(legion.fs.list()),\n\
                           legion.fs.exists('gone.txt'), JSON.stringify(missing));\n";
    let ran = i.src("main.js", src).await;
    ran.ok();
    assert_eq!(ran.result, None);
    assert_eq!(ran.result_json.as_deref(), Some(r#"{"ok":true}"#));
    assert_eq!(
        ran.stdout,
        "written [\"main.js\",\"out\",\"result.json\"] false [\"LegionFsError\",\"no-entry\"]\n"
    );
    // A result that is not JSON is not reported.
    let ran = i.src("main.js", "return 10n;\n").await;
    ran.ok();
    assert_eq!(ran.result, None);
    assert!(
        ran.stderr.contains("not JSON-serializable"),
        "{}",
        ran.stderr
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_globals_persist_per_instance_test() {
    let _g = shared();
    let src = "globalThis.count = (globalThis.count ?? 0) + 1;\nreturn count;\n";
    let mut a = instance(
        js(),
        Opts {
            lifetime: Lifetime::Tenant,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        a.src("main.js", src).await.ok().result.as_deref(),
        Some("1")
    );
    assert_eq!(
        a.src("main.js", src).await.ok().result.as_deref(),
        Some("2")
    );
    assert_eq!(a.src("main.js", "let x = 1;\n").await.ok().result, None);
    let mut b = instance(js(), Opts::default()).await;
    assert_eq!(
        b.src("main.js", src).await.ok().result.as_deref(),
        Some("1")
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_unsupported_stops_the_run_test() {
    let _g = shared();
    let cases = [
        ("require('child_process').execSync('ls');", "require('child_process'): Node builtins are not available in an isolate"),
        ("require('node:fs').readFileSync('x');", "require('fs'): Node builtins are not available in an isolate"),
        ("const net = require('net');", "require('net'): Node builtins are not available in an isolate"),
        ("require('node:worker_threads');", "require('worker_threads'): Node builtins are not available in an isolate"),
        ("require('os').cpus();", "require('os'): Node builtins are not available in an isolate"),
        ("new Worker('w.js');", "new Worker: workers are not available in an isolate"),
        ("import fs from 'node:fs';\nfs.readFileSync('x');", "ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm"),
        ("export const x = 1;", "ES module syntax (import/export): modules are not available in an isolate; write the entry as a script or run with --tier vm"),
    ];
    for (src, detail) in cases {
        let mut i = instance(js(), Opts::default()).await;
        let ran = i
            .src(
                "main.js",
                &format!("console.log('started');\n{src}\nconsole.log('not reached');\n"),
            )
            .await;
        assert_stopped(&ran, detail);
        let expect = if src.starts_with("import") || src.starts_with("export") {
            ""
        } else {
            "started\n"
        };
        assert_eq!(ran.stdout, expect, "{src}");
    }
    // A package is a normal error the code can catch.
    let mut i = instance(js(), Opts::default()).await;
    let ran = i
        .src(
            "main.js",
            "try { require('lodash'); } catch (e) { return e.message; }\n",
        )
        .await;
    assert_eq!(
        ran.ok().result.as_deref(),
        Some("\"Cannot find module 'lodash' (packages are not installed in an isolate; bundle them or run with --tier vm)\"")
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_state_test() {
    let _g = shared();
    let src = "const before = legion.state.get('count');\n\
               const n = Number(before ? new TextDecoder().decode(before) : '0') + 1;\n\
               legion.state.set('count', String(n));\n\
               legion.state.set('blob', new Uint8Array([0, 1]));\n\
               legion.state.delete('missing');\n\
               const info = legion.info();\n\
               const out = { n, keys: legion.state.keys().sort(), blob: [...legion.state.get('blob')],\n\
                             lifetime: info.lifetime, session: info.session, tenant: info.tenant, resumed: info.resumed };\n\
               try { legion.state.set('big', new Uint8Array(10000)); } catch (e) { out.quota = [e.name, e.kind, e.quota]; }\n\
               return out;\n";
    let mut i = instance(
        js(),
        Opts {
            lifetime: Lifetime::Session,
            ..Default::default()
        },
    )
    .await;
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.js", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"n": 1, "keys": ["blob", "count"], "blob": [0, 1], "lifetime": "session",
                           "session": "s-1", "tenant": null, "resumed": false,
                           "quota": ["LegionStateError", "quota", 4096]})
    );
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.js", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(v["n"], 2);
    assert_eq!(i.state().get("count").map(Vec::as_slice), Some(&b"2"[..]));

    let mut r = instance(js(), Opts::default()).await;
    let ran = r
        .src("main.js", "try { legion.state.get('k'); } catch (e) { return [e.kind, e instanceof LegionStateError]; }\n")
        .await;
    assert_eq!(
        ran.ok().result.as_deref(),
        Some(r#"["not-available",true]"#)
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_fetch_test() {
    let _g = shared();
    let src = "const r = await fetch('http://payments/v1/charge?x=1', { method: 'POST', headers: { 'X-Test': '1' }, body: '{\"a\":1}' });\n\
               const echo = await r.json();\n\
               const out = { status: r.status, upstream: r.headers.get('x-upstream'), method: echo.method, url: echo.url,\n\
                             body: echo.body, hdr: echo.headers['x-test'] };\n\
               const missing = await fetch('https://api.example.com/missing');\n\
               out.missing = missing.status;\n\
               try { await fetch('https://denied.example/'); } catch (e) { out.denied = e.name; }\n\
               return out;\n";
    let mut i = instance(js(), Opts::default()).await;
    let ran = i.src("main.js", src).await;
    let v: serde_json::Value = serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(v["status"], 200, "{v}");
    assert_eq!(v["upstream"], "fake");
    assert_eq!(v["method"], "POST");
    assert_eq!(v["url"], "http://payments/v1/charge?x=1");
    assert_eq!(v["body"], r#"{"a":1}"#);
    assert_eq!(v["hdr"], "1");
    assert_eq!(v["missing"], 404);
    assert_eq!(v["denied"], "TypeError", "{v}");
    assert_eq!(i.worker.egress.lock().unwrap().len(), 3);
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_vm_test() {
    let _g = shared();
    let src = "const m = legion.vm.acquire();\n\
               const r = m.exec(['echo', 'hi'], { stdin: 'in' });\n\
               m.put('/work/a.txt', 'data');\n\
               const got = new TextDecoder().decode(m.get('/work/a.txt'));\n\
               const resp = m.fetch(8080, { method: 'post', path: '/x', headers: { a: 'b' }, body: 'body' });\n\
               m.resize({ memoryMib: 512 });\n\
               const again = legion.vm.attach(m.id);\n\
               let gone;\n\
               try { legion.vm.attach('gone'); } catch (e) { gone = [e.kind, e.message, e instanceof LegionVmError]; }\n\
               m.release();\n\
               const d = new TextDecoder();\n\
               return { id: m.id, exit: r.exit, stdout: d.decode(r.stdout), stderr: d.decode(r.stderr), timedOut: r.timedOut,\n\
                        got, fetch: [resp.status, await resp.text(), resp.headers.get('x-vm')], again: again.id, gone };\n";
    let mut i = instance(
        js(),
        Opts {
            has_vm: true,
            ..Default::default()
        },
    )
    .await;
    let v: serde_json::Value =
        serde_json::from_str(i.src("main.js", src).await.ok().result.as_deref().unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"id": "child-1", "exit": 7, "stdout": "ran echo hi", "stderr": "in",
            "timedOut": false, "got": "file from vm", "fetch": [201, "vm body", "yes"],
            "again": "child-1", "gone": ["child-gone", "legion.vm.attach: child-gone: no such child", true]})
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
    let mut n = instance(js(), Opts::default()).await;
    let ran = n
        .src(
            "main.js",
            "try { legion.vm.acquire(); } catch (e) { return [e.kind, e.message]; }\n",
        )
        .await;
    assert_eq!(
        ran.ok().result.as_deref(),
        Some(
            r#"["not-configured","legion.vm.acquire: this task has no VM task (set config.vm.task)"]"#
        )
    );
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_restore_hygiene_test() {
    let _g = shared();
    let src = "return [Math.random(), Math.random(), [...crypto.getRandomValues(new Uint8Array(8))].join(','), crypto.randomUUID()];\n";
    let mut seen = Vec::new();
    for _ in 0..2 {
        let mut i = instance(js(), Opts::default()).await;
        let v: Vec<serde_json::Value> =
            serde_json::from_str(i.src("main.js", src).await.ok().result.as_deref().unwrap())
                .unwrap();
        seen.push(v);
    }
    for k in 0..seen[0].len() {
        assert_ne!(
            seen[0][k], seen[1][k],
            "value {k} repeated across instances: {seen:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Measurements (container numbers: a shared 4-vCPU VM, debug host build)
// ---------------------------------------------------------------------------

/// Tests share the process; the memory measurement must not.
fn shared() -> std::sync::RwLockReadGuard<'static, ()> {
    LOCK.read().unwrap_or_else(|e| e.into_inner())
}

static LOCK: std::sync::RwLock<()> = std::sync::RwLock::new(());

fn private_dirty_kib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap();
    s.lines()
        .find(|l| l.starts_with("Private_Dirty:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap()
}

/// About 10 KB of small function definitions and calls.
fn ten_kb(js: bool) -> String {
    let mut s = String::new();
    let mut calls = Vec::new();
    let mut n = 0;
    while s.len() < 10_000 {
        if js {
            s.push_str(&format!(
                "function f{n}(x) {{ const y = x * {n} + {}; return y % 1000; }}\n",
                n % 7
            ));
        } else {
            s.push_str(&format!(
                "def f{n}(x):\n    y = x * {n} + {}\n    return y % 1000\n",
                n % 7
            ));
        }
        calls.push(format!("f{n}({n})"));
        n += 1;
    }
    if js {
        s.push_str(&format!("const result = {};\n", calls.join(" + ")));
    } else {
        s.push_str(&format!("result = {}\n", calls.join(" + ")));
    }
    s
}

struct Timing {
    fresh_p50: u128,
    fresh_p99: u128,
    warm_p50: u128,
}

async fn time_runs(image: &Image, name: &str, src: &str, runs: usize) -> Timing {
    let mut fresh = Vec::new();
    for _ in 0..runs {
        let t = Instant::now();
        let mut i = instance(image, Opts::default()).await;
        i.src(name, src).await.ok();
        fresh.push(t.elapsed().as_micros());
    }
    let mut warm = Vec::new();
    let mut i = instance(image, Opts::default()).await;
    i.src(name, src).await.ok();
    for _ in 0..runs {
        warm.push(i.src(name, src).await.ok().micros);
    }
    let (fresh_p50, fresh_p99) = p50(fresh);
    Timing {
        fresh_p50,
        fresh_p99,
        warm_p50: p50(warm).0,
    }
}

/// Private memory per idle instance: `IDLE_N` instances, each called once
/// with a hello-world, held alive; the `Private_Dirty` delta over N.
async fn idle_private_bytes(image: &Image, name: &str, hello: &str) -> u64 {
    // Warm up: the first instance pays for lazily built host state.
    let mut w = instance(image, Opts::default()).await;
    w.src(name, hello).await.ok();
    drop(w);
    let before = private_dirty_kib();
    let mut held = Vec::with_capacity(IDLE_N);
    for _ in 0..IDLE_N {
        let mut i = instance(image, Opts::default()).await;
        i.src(name, hello).await.ok();
        // Empty the scratch tree: the measure is the instance, not its files.
        i.store.data_mut().scratch.clear();
        held.push(i);
    }
    let after = private_dirty_kib();
    drop(held);
    (after.saturating_sub(before) * 1024) / IDLE_N as u64
}

struct Measured {
    hello: Timing,
    ten_kb: Timing,
    imports: Option<Timing>,
    idle_private_bytes: u64,
}

/// A run that imports the commonly used standard-library modules.
const PY_IMPORTS: &str = "import json, re, datetime, collections, math, random, base64, hashlib, csv, io, \
                          itertools, functools, string, textwrap, dataclasses, typing, decimal, statistics, uuid, time\n\
                          print(json.dumps({'n': len(re.findall('a', 'banana'))}))\n";

async fn measure(image: &Image) -> Measured {
    let _g = LOCK.write().unwrap_or_else(|e| e.into_inner());
    let js = image.name == "js";
    let (name, hello) = if js {
        ("main.js", "console.log('hello');\n")
    } else {
        ("main.py", "print('hello')\n")
    };
    let hello_t = time_runs(image, name, hello, 50).await;
    let ten = ten_kb(js);
    let ten_t = time_runs(image, name, &ten, 50).await;
    let imports = if js {
        None
    } else {
        Some(time_runs(image, name, PY_IMPORTS, 50).await)
    };
    let idle = idle_private_bytes(image, name, hello).await;
    let m = Measured {
        hello: hello_t,
        ten_kb: ten_t,
        imports,
        idle_private_bytes: idle,
    };
    println!(
        "[container measurement] {} image: hello world fresh instance p50 {} µs p99 {} µs, warm instance p50 {} µs; \
         {}-byte snippet fresh p50 {} µs p99 {} µs, warm p50 {} µs (50 runs each); \
         idle private memory {} KiB per instance (N={IDLE_N})",
        image.name,
        m.hello.fresh_p50,
        m.hello.fresh_p99,
        m.hello.warm_p50,
        ten.len(),
        m.ten_kb.fresh_p50,
        m.ten_kb.fresh_p99,
        m.ten_kb.warm_p50,
        m.idle_private_bytes / 1024
    );
    if let Some(t) = &m.imports {
        println!(
            "[container measurement] {} image: importing 20 common stdlib modules, fresh instance p50 {} µs p99 {} µs, warm p50 {} µs",
            image.name, t.fresh_p50, t.fresh_p99, t.warm_p50
        );
    }
    m
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn python_measurements_test() {
    measure(python()).await;
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn js_measurements_test() {
    measure(js()).await;
}

// ---------------------------------------------------------------------------
// manifest.json
// ---------------------------------------------------------------------------

/// The image's own list of importable top-level modules: every name in
/// `sys.stdlib_module_names`, `sys.builtin_module_names` and
/// `pkgutil.iter_modules()` (outside the image's internals), kept when
/// `import name` succeeds in a fresh instance of the image.
async fn python_modules() -> Vec<String> {
    let list = r#"
import sys, pkgutil
names = set(sys.stdlib_module_names) | set(sys.builtin_module_names)
for m in pkgutil.iter_modules():
    if getattr(m.module_finder, "path", "").startswith("/python"):
        names.add(m.name)
result = sorted(n for n in names if "." not in n)
"#;
    let mut i = instance(python(), Opts::default()).await;
    let names: Vec<String> =
        serde_json::from_str(i.src("main.py", list).await.ok().result.as_deref().unwrap()).unwrap();
    // `ctypes` is a stub the runner installs so that using it stops the run
    // by name; it is not a module code can use, so it is not listed. Nor is
    // componentize-py's own runtime module.
    let stubs = ["ctypes"];
    let mut ok = vec!["legion".to_string()];
    for n in names {
        if stubs.contains(&n.as_str()) || n.starts_with("componentize_py") {
            continue;
        }
        let mut i = instance(python(), Opts::default()).await;
        let ran = i.src("main.py", &format!("import {n}\n")).await;
        if ran.exit == Some(0) {
            ok.push(n);
        }
    }
    ok.sort();
    ok
}

#[tokio::test]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn manifest_write_test() {
    let dir = images_dir();
    let write = std::env::var_os("LEGION_ISOLATE_MANIFEST_WRITE").is_some();
    let mut images = serde_json::Map::new();
    for name in ["python", "js"] {
        let wasm = dir.join(format!("{name}.wasm"));
        if !wasm.exists() {
            continue;
        }
        let image = if name == "python" { python() } else { js() };
        let bytes = std::fs::read(&wasm).unwrap();
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let measured = measure(image).await;
        let (extensions, vm_entry, modules, runtime) = if name == "python" {
            let modules = python_modules().await;
            println!(
                "python image: {} importable top-level modules",
                modules.len()
            );
            (
                vec![".py"],
                "main.py",
                modules,
                format!(
                    "CPython {} (componentize-py 0.25.1)",
                    python_version().await
                ),
            )
        } else {
            // The js image resolves no module specifiers by itself: no
            // packages, no Node builtins, and no ES modules from source.
            (
                vec![".js", ".mjs"],
                "main.mjs",
                Vec::new(),
                "StarlingMonkey (componentize-js 0.23.0)".to_string(),
            )
        };
        images.insert(
            name.to_string(),
            serde_json::json!({
                "file": format!("{name}.wasm"),
                "sha256": sha,
                "extensions": extensions,
                "vmEntry": vm_entry,
                "modules": modules,
                "initialMemoryBytes": module::initial_memory_bytes(&image.component),
                "idlePrivateBytes": measured.idle_private_bytes,
                "runtime": runtime,
            }),
        );
    }
    assert!(!images.is_empty(), "no images in {}", dir.display());
    let manifest = serde_json::json!({ "images": images });
    let text = serde_json::to_string_pretty(&manifest).unwrap() + "\n";
    if write {
        legion_isolate::compile::write_atomic(&dir.join("manifest.json"), text.as_bytes()).unwrap();
        println!("wrote {}", dir.join("manifest.json").display());
    } else {
        println!("manifest (not written; set LEGION_ISOLATE_MANIFEST_WRITE=1):\n{text}");
    }
}

async fn python_version() -> String {
    let mut i = instance(python(), Opts::default()).await;
    let ran = i
        .src(
            "main.py",
            "import sys\nresult = '%d.%d.%d' % sys.version_info[:3]\n",
        )
        .await;
    serde_json::from_str(ran.ok().result.as_deref().unwrap()).unwrap()
}
