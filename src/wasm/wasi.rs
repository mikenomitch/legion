//! WASI preview1 (`wasi_snapshot_preview1`), as much as a module built for
//! `wasm32-wasip1` touches when it has no filesystem and no sockets.
//!
//! The embedder implements [`WasiHost`] on its store data: what the module's
//! arguments and environment are, and where its standard output and error
//! go. A netd plugin has neither arguments nor environment and logs its
//! output; a task would take its argv and env from its spec and write its
//! output to its log files. Every other WASI function the module imports is
//! stubbed by [`stub_unknown_imports`] to answer `ENOSYS`, so a module whose
//! runtime links the whole of preview1 still instantiates.

use super::memory::{read, write, write_u32};
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Instant;
use wasmi::{Caller, Linker, Module, Val};

/// The WASI import module name.
pub const MODULE: &str = "wasi_snapshot_preview1";

/// `errno` values the shim returns.
pub const ESUCCESS: i32 = 0;
pub const EBADF: i32 = 8;
pub const ENOSYS: i32 = 52;

/// Which standard stream a module wrote to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Stdout,
    Stderr,
}

/// What an embedder tells the shim about one instance.
pub trait WasiHost {
    /// `argv`, including the program name a command module expects first.
    fn wasi_args(&self) -> &[String] {
        &[]
    }
    /// `KEY=value` pairs.
    fn wasi_env(&self) -> &[(String, String)] {
        &[]
    }
    /// A write to file descriptor 1 or 2, as one `fd_write` call's bytes.
    fn wasi_output(&self, stream: Output, bytes: &[u8]);
}

/// Add the WASI functions to `linker`, recording each in `defined` so
/// [`stub_unknown_imports`] skips it.
pub fn link<T: WasiHost + 'static>(
    linker: &mut Linker<T>,
    defined: &mut HashSet<(String, String)>,
) -> Result<(), String> {
    macro_rules! wasi {
        ($name:literal, $func:expr) => {{
            linker
                .func_wrap(MODULE, $name, $func)
                .map_err(|err| err.to_string())?;
            defined.insert((MODULE.to_string(), $name.to_string()));
        }};
    }

    wasi!("fd_write", |mut caller: Caller<'_, T>,
                       fd: i32,
                       iovs: i32,
                       count: i32,
                       written_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let stream = match fd {
            1 => Output::Stdout,
            2 => Output::Stderr,
            _ => return Ok(EBADF),
        };
        let mut bytes = Vec::new();
        for i in 0..count.max(0) {
            let iov = read(&caller, iovs + i * 8, 8)?;
            let ptr = u32::from_le_bytes(iov[0..4].try_into().unwrap()) as i32;
            let len = u32::from_le_bytes(iov[4..8].try_into().unwrap()) as i32;
            bytes.extend(read(&caller, ptr, len)?);
        }
        write_u32(&mut caller, written_ptr, bytes.len() as u32)?;
        caller.data().wasi_output(stream, &bytes);
        Ok(ESUCCESS)
    });
    wasi!("args_sizes_get", |mut caller: Caller<'_, T>,
                             count_ptr: i32,
                             size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let strings = c_strings(caller.data().wasi_args().iter().map(String::as_str));
        sizes(&mut caller, &strings, count_ptr, size_ptr)
    });
    wasi!("args_get", |mut caller: Caller<'_, T>,
                       argv: i32,
                       buf: i32|
     -> Result<i32, wasmi::Error> {
        let strings = c_strings(caller.data().wasi_args().iter().map(String::as_str));
        lay_out(&mut caller, &strings, argv, buf)
    });
    wasi!("environ_sizes_get", |mut caller: Caller<'_, T>,
                                count_ptr: i32,
                                size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let strings = env_strings(caller.data().wasi_env());
        sizes(&mut caller, &strings, count_ptr, size_ptr)
    });
    wasi!("environ_get", |mut caller: Caller<'_, T>,
                          environ: i32,
                          buf: i32|
     -> Result<i32, wasmi::Error> {
        let strings = env_strings(caller.data().wasi_env());
        lay_out(&mut caller, &strings, environ, buf)
    });
    wasi!("clock_time_get", |mut caller: Caller<'_, T>,
                             id: i32,
                             _precision: i64,
                             ptr: i32|
     -> Result<i32, wasmi::Error> {
        // Clock 0 is the realtime clock; every other is monotonic.
        let now = if id == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default()
        } else {
            monotonic_nanos()
        };
        write(&mut caller, ptr, &now.to_le_bytes())?;
        Ok(ESUCCESS)
    });
    wasi!("random_get", |mut caller: Caller<'_, T>,
                         ptr: i32,
                         len: i32|
     -> Result<i32, wasmi::Error> {
        let mut bytes = vec![0u8; len.clamp(0, 65_536) as usize];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
            .map_err(|_| wasmi::Error::new("no randomness"))?;
        write(&mut caller, ptr, &bytes)?;
        Ok(ESUCCESS)
    });
    wasi!("proc_exit", |_caller: Caller<'_, T>,
                        code: i32|
     -> Result<(), wasmi::Error> {
        Err(wasmi::Error::i32_exit(code))
    });
    wasi!("sched_yield", |_caller: Caller<'_, T>| -> i32 { ESUCCESS });
    Ok(())
}

/// Stub every function `module` imports that `defined` does not name: a
/// function from a module in `answers` returns that value (an embedder's
/// "unimplemented" status, [`ENOSYS`] for WASI), and one from any other
/// module traps when called, so an unused import never stops a module from
/// loading. A non-function import is refused.
pub fn stub_unknown_imports<T: 'static>(
    linker: &mut Linker<T>,
    module: &Module,
    defined: &mut HashSet<(String, String)>,
    answers: &[(&str, i64)],
) -> Result<(), String> {
    for import in module.imports() {
        let key = (import.module().to_string(), import.name().to_string());
        if defined.contains(&key) {
            continue;
        }
        let wasmi::ExternType::Func(ty) = import.ty() else {
            return Err(format!(
                "the module imports {}.{}, which is not a function; a module may import only \
                 functions",
                key.0, key.1
            ));
        };
        let answer = answers
            .iter()
            .find(|(name, _)| *name == key.0)
            .map(|(_, answer)| *answer);
        let label = format!("{}.{}", key.0, key.1);
        linker
            .func_new(
                &key.0,
                &key.1,
                ty.clone(),
                move |_caller, _params, results| {
                    let Some(answer) = answer else {
                        return Err(wasmi::Error::new(format!(
                            "called {label}, which this host does not provide"
                        )));
                    };
                    for result in results.iter_mut() {
                        *result = match result {
                            Val::I64(_) => Val::I64(answer),
                            _ => Val::I32(answer as i32),
                        };
                    }
                    Ok(())
                },
            )
            .map_err(|err| err.to_string())?;
        defined.insert(key);
    }
    Ok(())
}

/// Each string NUL-terminated, as `args_get` and `environ_get` lay them out.
fn c_strings<'a>(strings: impl Iterator<Item = &'a str>) -> Vec<Vec<u8>> {
    strings
        .map(|s| {
            let mut bytes = s.as_bytes().to_vec();
            bytes.push(0);
            bytes
        })
        .collect()
}

fn env_strings(env: &[(String, String)]) -> Vec<Vec<u8>> {
    let pairs: Vec<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    c_strings(pairs.iter().map(String::as_str))
}

/// `*_sizes_get`: the count, and the bytes all the strings need.
fn sizes<T>(
    caller: &mut Caller<'_, T>,
    strings: &[Vec<u8>],
    count_ptr: i32,
    size_ptr: i32,
) -> Result<i32, wasmi::Error> {
    let size: usize = strings.iter().map(Vec::len).sum();
    write_u32(caller, count_ptr, strings.len() as u32)?;
    write_u32(caller, size_ptr, size as u32)?;
    Ok(ESUCCESS)
}

/// `args_get` / `environ_get`: the strings back to back at `buf`, and a
/// pointer to each in the array at `pointers`.
fn lay_out<T>(
    caller: &mut Caller<'_, T>,
    strings: &[Vec<u8>],
    pointers: i32,
    buf: i32,
) -> Result<i32, wasmi::Error> {
    let mut at = buf;
    for (i, string) in strings.iter().enumerate() {
        write_u32(caller, pointers + (i as i32) * 4, at as u32)?;
        write(caller, at, string)?;
        at += string.len() as i32;
    }
    Ok(ESUCCESS)
}

fn monotonic_nanos() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[cfg(test)]
mod wasi_test {
    use super::*;
    use crate::wasm::engine;
    use std::sync::Mutex;
    use wasmi::Store;

    struct Host {
        args: Vec<String>,
        env: Vec<(String, String)>,
        out: Mutex<Vec<(Output, Vec<u8>)>>,
    }

    impl WasiHost for Host {
        fn wasi_args(&self) -> &[String] {
            &self.args
        }
        fn wasi_env(&self) -> &[(String, String)] {
            &self.env
        }
        fn wasi_output(&self, stream: Output, bytes: &[u8]) {
            self.out.lock().unwrap().push((stream, bytes.to_vec()));
        }
    }

    const MODULE_WAT: &str = r#"
      (module
        (import "wasi_snapshot_preview1" "args_sizes_get" (func $args_sizes (param i32 i32) (result i32)))
        (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
        (import "wasi_snapshot_preview1" "environ_sizes_get" (func $env_sizes (param i32 i32) (result i32)))
        (import "wasi_snapshot_preview1" "environ_get" (func $env_get (param i32 i32) (result i32)))
        (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
        (import "wasi_snapshot_preview1" "path_open"
          (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 200) "hi\n")
        (func (export "run") (result i32)
          (drop (call $args_sizes (i32.const 0) (i32.const 4)))
          (drop (call $args_get (i32.const 16) (i32.const 64)))
          (drop (call $env_sizes (i32.const 8) (i32.const 12)))
          (drop (call $env_get (i32.const 32) (i32.const 128)))
          (i32.store (i32.const 300) (i32.const 200))
          (i32.store (i32.const 304) (i32.const 3))
          (drop (call $fd_write (i32.const 1) (i32.const 300) (i32.const 1) (i32.const 308)))
          (call $path_open (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0)
                (i64.const 0) (i64.const 0) (i32.const 0) (i32.const 0))))
    "#;

    /// A command module sees its argv and environment laid out as WASI
    /// lays them out, its output reaches the host, and a WASI call the shim
    /// does not provide answers ENOSYS instead of failing to link.
    #[test]
    fn args_env_and_output_reach_the_module_and_the_host_test() {
        let module = Module::new(engine(), MODULE_WAT).unwrap();
        let mut linker = Linker::new(engine());
        let mut defined = HashSet::new();
        link(&mut linker, &mut defined).unwrap();
        stub_unknown_imports(
            &mut linker,
            &module,
            &mut defined,
            &[(MODULE, ENOSYS as i64)],
        )
        .unwrap();
        let host = Host {
            args: vec!["prog".into(), "--flag".into()],
            env: vec![("A".into(), "b".into())],
            out: Mutex::default(),
        };
        let mut store = Store::new(engine(), host);
        store.set_fuel(1_000_000).unwrap();
        let instance = linker.instantiate_and_start(&mut store, &module).unwrap();
        let run = instance.get_typed_func::<(), i32>(&store, "run").unwrap();
        assert_eq!(run.call(&mut store, ()).unwrap(), ENOSYS);

        let memory = instance.get_memory(&store, "memory").unwrap();
        let u32_at = |at: usize| {
            let mut word = [0u8; 4];
            memory.read(&store, at, &mut word).unwrap();
            u32::from_le_bytes(word) as usize
        };
        let c_str = |at: usize| {
            let data = memory.data(&store);
            let end = data[at..].iter().position(|b| *b == 0).unwrap();
            String::from_utf8(data[at..at + end].to_vec()).unwrap()
        };
        assert_eq!((u32_at(0), u32_at(4)), (2, "prog\0--flag\0".len()));
        assert_eq!(c_str(u32_at(16)), "prog");
        assert_eq!(c_str(u32_at(20)), "--flag");
        assert_eq!((u32_at(8), u32_at(12)), (1, "A=b\0".len()));
        assert_eq!(c_str(u32_at(32)), "A=b");
        assert_eq!(u32_at(308), 3, "fd_write reports the bytes written");
        assert_eq!(
            *store.data().out.lock().unwrap(),
            vec![(Output::Stdout, b"hi\n".to_vec())]
        );
    }
}
