//! `cargo bench -p legion-isolate --bench isolate_bench`
//!
//! - `instantiate_run`: a fresh instance of a trivial command per call
//!   (admission, slot, instantiate, `wasi:cli/run`, outputs).
//! - `warm_runner`: one call on a resident tenant instance.
//! - `await_round_trip`: one call whose guest awaits a host import that goes
//!   to the worker (`vm.acquire` + `exec` on an in-process fake worker).
//!
//! Each iteration is one full `Host::invoke` through a shard; the numbers
//! include the hop from the calling runtime to the shard and back.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use legion_isolate::compile;
use legion_isolate::engine::EngineParams;
use legion_isolate::host::{Host, HostSetup};
use legion_isolate::worker_api::WorkerApi;
use legion_proto::isolate_host::*;
use tonic::Status;

struct Worker;

#[async_trait]
impl WorkerApi for Worker {
    async fn egress(&self, _r: EgressRequest) -> Result<EgressReply, Status> {
        Ok(EgressReply::default())
    }
    async fn vm(&self, r: VmRequest) -> Result<VmReply, Status> {
        Ok(match r.op {
            Some(vm_request::Op::Acquire(_)) => VmReply {
                id: "c".into(),
                ..Default::default()
            },
            _ => VmReply {
                exec: Some(VmExecResult::default()),
                ..Default::default()
            },
        })
    }
    async fn save_checkpoint(
        &self,
        _h: CheckpointHeader,
        _b: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status> {
        Ok(SaveCheckpointReply::default())
    }
    async fn load_checkpoint(
        &self,
        _o: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status> {
        Ok(None)
    }
    async fn log(&self, _b: LogBatch) -> Result<(), Status> {
        Ok(())
    }
}

fn setup(rt: &tokio::runtime::Runtime, dir: &std::path::Path) -> Host {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for m in ["cmd", "runner"] {
        compile::compile_file(
            &EngineParams::default(),
            &fixtures.join(format!("{m}.wasm")),
            &dir.join(format!("{m}.cwasm")),
        )
        .unwrap();
    }
    let host = Host::new(HostSetup {
        config: HostConfig {
            protocol_version: PROTOCOL_VERSION,
            shards: 1,
            slots: 64,
            read_dirs: vec![dir.display().to_string()],
            async_stack_bytes: 512 << 10,
            require_spectre_mitigations: true,
            ..Default::default()
        },
        worker: Arc::new(Worker),
        rt: rt.handle().clone(),
        confinement: Confinement::default(),
        statm: None,
    })
    .unwrap();
    for m in ["cmd", "runner"] {
        host.load_module(LoadModuleRequest {
            module_id: m.into(),
            artifact_path: dir.join(format!("{m}.cwasm")).display().to_string(),
        })
        .unwrap();
    }
    let spec = |task: &str, module: &str, life: Lifetime| TaskSpec {
        job_id: "j".into(),
        task_name: task.into(),
        module_id: module.into(),
        lifetime: life as i32,
        memory_max_bytes: 64 << 20,
        cpu_weight: 1,
        queue_timeout_millis: 5000,
        has_vm: true,
        ..Default::default()
    };
    host.configure_task(spec("cmd", "cmd", Lifetime::Request))
        .unwrap();
    host.configure_task(spec("runner", "runner", Lifetime::Tenant))
        .unwrap();
    host
}

fn invoke(task: &str, args: &[&str]) -> InvokeRequest {
    InvokeRequest {
        job_id: "j".into(),
        task_name: task.into(),
        tenant: "t".into(),
        call: Some(invoke_request::Call::Run(RunCall {
            args: args.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let host = setup(&rt, dir.path());
    let mut g = c.benchmark_group("isolate");
    g.measurement_time(Duration::from_secs(5));
    g.bench_function("instantiate_run", |b| {
        b.to_async(&rt).iter(|| async {
            let r = host.invoke(invoke("cmd", &["exit", "0"])).await.unwrap();
            assert!(r.stopped.is_none());
        })
    });
    g.bench_function("warm_runner", |b| {
        b.to_async(&rt).iter(|| async {
            let r = host.invoke(invoke("runner", &["count"])).await.unwrap();
            assert_eq!(r.start, "warm");
        })
    });
    g.bench_function("await_round_trip", |b| {
        b.to_async(&rt).iter(|| async {
            let r = host.invoke(invoke("runner", &["vm"])).await.unwrap();
            assert!(r.stopped.is_none());
        })
    });
    g.finish();
    host.shutdown();
}

criterion_group!(benches, bench);
criterion_main!(benches);
