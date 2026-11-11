//! Guest-escape regression corpus (plan §12): known-bad accesses run under
//! the engine configuration the host uses, for every memory layout this
//! machine supports. Each must trap, and a neighbouring instance's memory
//! (and the faulting instance's own) must be unchanged. Run against every
//! engine upgrade.

use legion_isolate::engine::{host_engine, mpk_available, EngineParams, PoolSize};
use legion_proto::isolate_host::SlotLayout;
use wasmtime::{Engine, Instance, Memory, Module, Store, Trap};

const PAGE: u32 = 65536;

fn layouts() -> Vec<EngineParams> {
    let mut v = vec![
        EngineParams {
            layout: SlotLayout::GuardPages,
            slot_bytes: 1 << 20,
            spectre: true,
        },
        EngineParams {
            layout: SlotLayout::Small,
            slot_bytes: 1 << 20,
            spectre: true,
        },
    ];
    if mpk_available() {
        v.push(EngineParams {
            layout: SlotLayout::Mpk,
            slot_bytes: 1 << 20,
            spectre: true,
        });
    } else {
        eprintln!("MPK is not available on this machine; the mpk layout is not exercised");
    }
    v
}

fn engine(p: &EngineParams) -> Engine {
    host_engine(
        p,
        PoolSize {
            slots: 8,
            async_stack_bytes: 256 << 10,
        },
    )
    .unwrap()
}

/// A module with one page of memory, a two-entry table, and probes.
const PROBE: &str = r#"
(module
  (memory (export "mem") 1 1)
  (table 2 funcref)
  (elem (i32.const 0) $zero $zero)
  (type $t (func (result i32)))
  (func $zero (result i32) i32.const 0)
  (func (export "load8") (param i32) (result i32) (i32.load8_u (local.get 0)))
  (func (export "load16") (param i32) (result i32) (i32.load16_u (local.get 0)))
  (func (export "load32") (param i32) (result i32) (i32.load (local.get 0)))
  (func (export "load64") (param i32) (result i64) (i64.load (local.get 0)))
  (func (export "load128") (param i32) (result i32)
    (i32x4.extract_lane 0 (v128.load (local.get 0))))
  (func (export "store8") (param i32) (i32.store8 (local.get 0) (i32.const 0x5a)))
  (func (export "store16") (param i32) (i32.store16 (local.get 0) (i32.const 0x5a5a)))
  (func (export "store32") (param i32) (i32.store (local.get 0) (i32.const 0x5a5a5a5a)))
  (func (export "store64") (param i32) (i64.store (local.get 0) (i64.const 0x5a5a5a5a5a5a5a5a)))
  (func (export "store128") (param i32) (v128.store (local.get 0) (v128.const i64x2 -1 -1)))
  ;; A huge static offset on top of a small dynamic address.
  (func (export "load_offset") (param i32) (result i32)
    (i32.load offset=0xfffffff0 (local.get 0)))
  (func (export "store_offset") (param i32)
    (i32.store offset=0xfffffff0 (local.get 0) (i32.const 1)))
  (func (export "call_indirect") (param i32) (result i32)
    (call_indirect (type $t) (local.get 0)))
  (func (export "copy") (param i32 i32 i32)
    (memory.copy (local.get 0) (local.get 1) (local.get 2)))
  (func (export "fill") (param i32 i32)
    (memory.fill (local.get 0) (i32.const 0x5a) (local.get 1)))
  ;; CVE-2023-26489: an address computed by a shift Cranelift folded into
  ;; a 64-bit addressing mode. The i32 shift wraps, so 0x2000_0000 << 3 is
  ;; address 0, not 4 GiB.
  (func (export "shifted") (param i32) (result i64)
    (i64.load (i32.shl (local.get 0) (i32.const 3))))
)
"#;

struct Probe {
    store: Store<()>,
    inst: Instance,
    mem: Memory,
}

fn probe(engine: &Engine, module: &Module, fill: u8) -> Probe {
    let mut store = Store::new(engine, ());
    store.set_epoch_deadline(u64::MAX / 2);
    let inst = Instance::new(&mut store, module, &[]).unwrap();
    let mem = inst.get_memory(&mut store, "mem").unwrap();
    mem.data_mut(&mut store).fill(fill);
    Probe { store, inst, mem }
}

impl Probe {
    fn digest(&self) -> (usize, u64) {
        let d = self.mem.data(&self.store);
        (
            d.len(),
            d.iter()
                .fold(0xcbf29ce484222325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x100000001b3)),
        )
    }

    fn trap(&mut self, f: &str, args: &[wasmtime::Val]) -> Trap {
        let func = self.inst.get_func(&mut self.store, f).unwrap();
        let mut results = vec![wasmtime::Val::I32(0); func.ty(&self.store).results().len()];
        let e = func
            .call(&mut self.store, args, &mut results)
            .expect_err(&format!("{f}({args:?}) must trap"));
        *e.downcast_ref::<Trap>()
            .unwrap_or_else(|| panic!("{f}({args:?}): not a trap: {e:?}"))
    }
}

fn i32v(x: u32) -> wasmtime::Val {
    wasmtime::Val::I32(x as i32)
}

#[test]
fn out_of_bounds_accesses_trap_and_touch_nothing_test() {
    for p in layouts() {
        let engine = engine(&p);
        let module = Module::new(&engine, wat::parse_str(PROBE).unwrap()).unwrap();
        let mut neighbour = probe(&engine, &module, 0xab);
        let before_n = neighbour.digest();
        let widths: [(&str, &str, u32); 5] = [
            ("load8", "store8", 1),
            ("load16", "store16", 2),
            ("load32", "store32", 4),
            ("load64", "store64", 8),
            ("load128", "store128", 16),
        ];
        for (load, store, w) in widths {
            // Just past the end, at the end, far past it, and negative
            // (an i32 address of -1 is 4 GiB - 1).
            for addr in [PAGE - w + 1, PAGE, PAGE + 4096, 0x8000_0000, 0xffff_ffff, u32::MAX - w + 1] {
                let mut me = probe(&engine, &module, 0x11);
                let before = me.digest();
                assert_eq!(
                    me.trap(load, &[i32v(addr)]),
                    Trap::MemoryOutOfBounds,
                    "{:?} {load} at {addr:#x}",
                    p.layout
                );
                assert_eq!(
                    me.trap(store, &[i32v(addr)]),
                    Trap::MemoryOutOfBounds,
                    "{:?} {store} at {addr:#x}",
                    p.layout
                );
                assert_eq!(me.digest(), before, "{:?} {store} at {addr:#x} wrote", p.layout);
            }
        }
        let mut me = probe(&engine, &module, 0x11);
        for addr in [0x10u32, PAGE] {
            assert_eq!(me.trap("load_offset", &[i32v(addr)]), Trap::MemoryOutOfBounds);
            let mut me2 = probe(&engine, &module, 0x11);
            assert_eq!(me2.trap("store_offset", &[i32v(addr)]), Trap::MemoryOutOfBounds);
        }
        for idx in [2u32, 0x7fff_ffff, 0xffff_ffff] {
            let mut me = probe(&engine, &module, 0x11);
            assert_eq!(
                me.trap("call_indirect", &[i32v(idx)]),
                Trap::TableOutOfBounds,
                "{:?} call_indirect {idx:#x}",
                p.layout
            );
        }
        // Bulk operations past the end trap before writing anything.
        for (dst, src, len) in [(PAGE - 8, 0, 16), (0, PAGE - 8, 16), (0, 0, PAGE + 1), (0xffff_fff0, 0, 32)] {
            let mut me = probe(&engine, &module, 0x11);
            let before = me.digest();
            assert_eq!(
                me.trap("copy", &[i32v(dst), i32v(src), i32v(len)]),
                Trap::MemoryOutOfBounds
            );
            assert_eq!(me.digest(), before, "memory.copy wrote before trapping");
        }
        for (dst, len) in [(PAGE - 8, 16), (1, PAGE), (0xffff_ff00, 0x100)] {
            let mut me = probe(&engine, &module, 0x11);
            let before = me.digest();
            assert_eq!(me.trap("fill", &[i32v(dst), i32v(len)]), Trap::MemoryOutOfBounds);
            assert_eq!(me.digest(), before, "memory.fill wrote before trapping");
        }
        assert_eq!(neighbour.digest(), before_n, "{:?}: the neighbour changed", p.layout);
        // In-bounds accesses still work (the corpus is not vacuous).
        let f = neighbour
            .inst
            .get_typed_func::<u32, u32>(&mut neighbour.store, "load8")
            .unwrap();
        assert_eq!(f.call(&mut neighbour.store, PAGE - 1).unwrap(), 0xab);
    }
}

#[test]
fn cve_2023_26489_shifted_address_wraps_in_32_bits_test() {
    for p in layouts() {
        let engine = engine(&p);
        let module = Module::new(&engine, wat::parse_str(PROBE).unwrap()).unwrap();
        let mut me = probe(&engine, &module, 0);
        me.mem.data_mut(&mut me.store)[..8].copy_from_slice(&0x1122334455667788u64.to_le_bytes());
        let f = me
            .inst
            .get_typed_func::<u32, u64>(&mut me.store, "shifted")
            .unwrap();
        assert_eq!(
            f.call(&mut me.store, 0x2000_0000).unwrap(),
            0x1122334455667788,
            "{:?}",
            p.layout
        );
        assert_eq!(f.call(&mut me.store, 0x1000).unwrap(), 0);
        // Past the end after the shift: a trap, not a read beyond.
        let e = f.call(&mut me.store, 0x2000).unwrap_err();
        assert_eq!(e.downcast_ref::<Trap>(), Some(&Trap::MemoryOutOfBounds));
    }
}
