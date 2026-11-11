//! Unit tests for the scratch tree and its `wasi:filesystem` host functions.

use std::sync::Arc;

use wasmtime::component::{Resource, ResourceTable};
use wasmtime_wasi_io::bytes::Bytes;
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream, StreamError};

use super::bindings::wasi::filesystem::types::{
    DescriptorFlags, DescriptorType, ErrorCode, HostDescriptor, HostDirectoryEntryStream,
    NewTimestamp, OpenFlags, PathFlags,
};
use super::bindings::wasi::filesystem::{preopens, types};
use super::tree::{lock, Node, OpenOptions, Tree, MAX_NODES, ROOT};
use super::*;

const MIB: u64 = 1 << 20;

fn tree(limit: u64) -> (Tree, Arc<MemoryAccount>) {
    let account = Arc::new(MemoryAccount::new(limit));
    (Tree::new(account.clone()), account)
}

fn dir_ino(t: &Tree, base: tree::Ino, path: &str) -> tree::Ino {
    match t.lookup(base, path).unwrap() {
        Node::Dir(ino) => ino,
        Node::File(_) => panic!("{path} is a file"),
    }
}

fn file(t: &Tree, path: &str) -> tree::FileRef {
    match t.lookup(ROOT, path).unwrap() {
        Node::File(f) => f,
        Node::Dir(_) => panic!("{path} is a directory"),
    }
}

fn code<T: std::fmt::Debug>(r: Result<T, ErrorCode>) -> ErrorCode {
    r.expect_err("expected an error")
}

fn create() -> OpenOptions {
    OpenOptions {
        create: true,
        write: true,
        ..Default::default()
    }
}

// ---------------------------------------------------------------- paths

#[test]
fn dot_and_dotdot_resolve_posix_style_test() {
    let (mut t, _) = tree(MIB);
    let b = t.create_dir_all(ROOT, "/a/b").unwrap();
    let a = dir_ino(&t, ROOT, "a");
    assert_eq!(dir_ino(&t, ROOT, "a/b/../b/./"), b);
    assert_eq!(dir_ino(&t, b, ".."), a);
    assert_eq!(dir_ino(&t, b, "../.."), ROOT);
    assert_eq!(dir_ino(&t, b, "."), b);
    assert_eq!(dir_ino(&t, ROOT, "a//b"), b);
    // `..` above the descriptor's directory is fine inside the tree.
    t.write_file(ROOT, "/a/x", b"x").unwrap();
    assert!(matches!(t.lookup(b, "../x").unwrap(), Node::File(_)));
}

#[test]
fn dotdot_never_escapes_the_root_test() {
    let (mut t, _) = tree(MIB);
    let b = t.create_dir_all(ROOT, "/a/b").unwrap();
    assert_eq!(dir_ino(&t, ROOT, ".."), ROOT);
    assert_eq!(dir_ino(&t, b, "../../../../.."), ROOT);
    assert_eq!(dir_ino(&t, b, "../../../../a/b"), b);
    t.write_file(ROOT, "/../../../top", b"t").unwrap();
    assert!(matches!(t.lookup(ROOT, "top").unwrap(), Node::File(_)));
    assert_eq!(t.list(ROOT).unwrap().len(), 2);
}

#[test]
fn path_errors_test() {
    let (mut t, _) = tree(MIB);
    t.write_file(ROOT, "/f", b"data").unwrap();
    assert_eq!(code(t.lookup(ROOT, "")), ErrorCode::NoEntry);
    assert_eq!(code(t.lookup(ROOT, "missing")), ErrorCode::NoEntry);
    assert_eq!(code(t.lookup(ROOT, "missing/x")), ErrorCode::NoEntry);
    assert_eq!(code(t.lookup(ROOT, "f/x")), ErrorCode::NotDirectory);
    assert_eq!(code(t.lookup(ROOT, "f/")), ErrorCode::NotDirectory);
    assert_eq!(code(t.lookup(ROOT, "f/..")), ErrorCode::NotDirectory);
    let long = "n".repeat(256);
    assert_eq!(code(t.lookup(ROOT, &long)), ErrorCode::NameTooLong);
    assert_eq!(code(t.create_dir(ROOT, &long)), ErrorCode::NameTooLong);
    assert!(t.create_dir(ROOT, &"n".repeat(255)).is_ok());
    assert_eq!(code(t.lookup(ROOT, "a\0b")), ErrorCode::Invalid);
}

#[test]
fn trailing_slash_names_a_directory_test() {
    let (mut t, _) = tree(MIB);
    t.create_dir(ROOT, "d/").unwrap();
    assert!(matches!(t.lookup(ROOT, "d/").unwrap(), Node::Dir(_)));
    t.write_file(ROOT, "/f", b"").unwrap();
    assert_eq!(
        code(t.open(ROOT, "f/", OpenOptions::default())),
        ErrorCode::NotDirectory
    );
    assert_eq!(code(t.open(ROOT, "new/", create())), ErrorCode::IsDirectory);
    assert_eq!(code(t.unlink(ROOT, "f/")), ErrorCode::NotDirectory);
    assert_eq!(
        code(t.rename(ROOT, "f", ROOT, "g/")),
        ErrorCode::NotDirectory
    );
}

// ---------------------------------------------------------------- open

#[test]
fn open_flags_test() {
    let (mut t, _) = tree(MIB);
    t.create_dir(ROOT, "d").unwrap();
    let f = t.open(ROOT, "f", create()).unwrap();
    assert!(matches!(f, Node::File(_)));
    // Opening again without exclusive gets the same file.
    assert!(t.open(ROOT, "f", create()).unwrap().is_same(&f));
    let excl = OpenOptions {
        exclusive: true,
        ..create()
    };
    assert_eq!(code(t.open(ROOT, "f", excl)), ErrorCode::Exist);
    assert_eq!(code(t.open(ROOT, "d", excl)), ErrorCode::Exist);
    assert_eq!(
        code(t.open(ROOT, "missing", OpenOptions::default())),
        ErrorCode::NoEntry
    );
    let dir = OpenOptions {
        directory: true,
        ..Default::default()
    };
    assert_eq!(code(t.open(ROOT, "f", dir)), ErrorCode::NotDirectory);
    assert!(matches!(t.open(ROOT, "d", dir).unwrap(), Node::Dir(_)));
    assert!(matches!(t.open(ROOT, ".", dir).unwrap(), Node::Dir(ROOT)));
    let create_dir = OpenOptions {
        directory: true,
        ..create()
    };
    assert_eq!(code(t.open(ROOT, "nd", create_dir)), ErrorCode::Invalid);
    let write = OpenOptions {
        write: true,
        ..Default::default()
    };
    assert_eq!(code(t.open(ROOT, "d", write)), ErrorCode::IsDirectory);
    let trunc = OpenOptions {
        truncate: true,
        ..Default::default()
    };
    assert_eq!(code(t.open(ROOT, "d", trunc)), ErrorCode::IsDirectory);
}

#[test]
fn truncate_on_open_empties_and_gives_back_test() {
    let (mut t, account) = tree(MIB);
    t.write_file(ROOT, "/f", b"0123456789").unwrap();
    assert_eq!(account.scratch(), 10);
    let opts = OpenOptions {
        truncate: true,
        write: true,
        ..Default::default()
    };
    let Node::File(f) = t.open(ROOT, "f", opts).unwrap() else {
        panic!()
    };
    assert_eq!(lock(&f).len(), 0);
    assert_eq!(account.scratch(), 0);
    assert_eq!(t.used_bytes(), 0);
}

// ---------------------------------------------------------------- files

#[test]
fn write_read_and_sparse_growth_test() {
    let (mut t, account) = tree(MIB);
    let Node::File(f) = t.open(ROOT, "f", create()).unwrap() else {
        panic!()
    };
    let mut file = lock(&f);
    assert_eq!(file.write_at(0, b"hello").unwrap(), 5);
    assert_eq!(file.write_at(8, b"world").unwrap(), 5);
    assert_eq!(file.bytes(), b"hello\0\0\0world");
    assert_eq!(file.read_at(3, 4), b"lo\0\0");
    assert_eq!(file.read_at(100, 4), b"");
    assert_eq!(file.read_at(10, u64::MAX), b"rld");
    assert_eq!(file.append(b"!").unwrap(), 1);
    assert_eq!(file.len(), 14);
    drop(file);
    assert_eq!(account.scratch(), 14);
    assert_eq!(t.used_bytes(), 14);
}

#[test]
fn set_size_grows_zeroed_and_shrinks_test() {
    let (mut t, account) = tree(MIB);
    t.write_file(ROOT, "/f", b"abc").unwrap();
    let f = file(&t, "/f");
    lock(&f).set_size(6).unwrap();
    assert_eq!(lock(&f).bytes(), b"abc\0\0\0");
    assert_eq!(account.scratch(), 6);
    lock(&f).set_size(1).unwrap();
    assert_eq!(lock(&f).bytes(), b"a");
    assert_eq!(account.scratch(), 1);
}

#[test]
fn offsets_past_the_maximum_are_file_too_large_test() {
    let (mut t, _) = tree(MIB);
    t.write_file(ROOT, "/f", b"").unwrap();
    let f = file(&t, "/f");
    assert_eq!(
        code(lock(&f).write_at(u64::MAX, b"x")),
        ErrorCode::FileTooLarge
    );
    assert_eq!(code(lock(&f).set_size(u64::MAX)), ErrorCode::FileTooLarge);
}

// ---------------------------------------------------------------- accounting

#[test]
fn refused_charge_leaves_the_file_unchanged_test() {
    let (mut t, account) = tree(10);
    t.write_file(ROOT, "/f", b"12345678").unwrap();
    let f = file(&t, "/f");
    // Growing past the ceiling: refused, nothing written.
    assert_eq!(
        code(lock(&f).write_at(6, b"abcdef")),
        ErrorCode::InsufficientSpace
    );
    assert_eq!(lock(&f).bytes(), b"12345678");
    assert_eq!(code(lock(&f).set_size(11)), ErrorCode::InsufficientSpace);
    assert_eq!(lock(&f).len(), 8);
    // Overwriting in place costs nothing.
    lock(&f).write_at(0, b"ABCDEFGH").unwrap();
    assert_eq!(account.scratch(), 8);
    assert!(account.refused());
    // Host staging: an overwrite that does not fit keeps the old contents,
    // and a new file that does not fit is not made.
    assert_eq!(
        t.write_file(ROOT, "/f", b"0123456789A"),
        Err(ErrorCode::InsufficientSpace)
    );
    assert_eq!(lock(&f).bytes(), b"ABCDEFGH");
    assert_eq!(
        t.write_file(ROOT, "/g", b"xyz"),
        Err(ErrorCode::InsufficientSpace)
    );
    assert_eq!(code(t.lookup(ROOT, "g")), ErrorCode::NoEntry);
    assert_eq!(account.scratch(), 8);
    assert_eq!(t.used_bytes(), 8);
}

#[test]
fn linear_memory_and_files_share_the_ceiling_test() {
    let (mut t, account) = tree(100);
    assert!(account.set_linear(95));
    assert_eq!(
        t.write_file(ROOT, "/f", &[0; 6]),
        Err(ErrorCode::InsufficientSpace)
    );
    t.write_file(ROOT, "/f", &[0; 5]).unwrap();
    assert_eq!(account.total(), 100);
}

#[test]
fn overwrite_unlink_and_clear_give_bytes_back_test() {
    let (mut t, account) = tree(MIB);
    t.write_file(ROOT, "/a/f", &[1; 100]).unwrap();
    t.write_file(ROOT, "/a/f", &[1; 40]).unwrap();
    assert_eq!(account.scratch(), 40);
    t.write_file(ROOT, "/a/g", &[1; 10]).unwrap();
    t.unlink(ROOT, "a/g").unwrap();
    assert_eq!(account.scratch(), 40);
    t.write_file(ROOT, "/b/c/h", &[1; 7]).unwrap();
    let nodes = t.nodes();
    assert_eq!(nodes, 6); // root, a, a/f, b, b/c, b/c/h
    t.clear();
    assert_eq!(account.scratch(), 0);
    assert_eq!(t.used_bytes(), 0);
    assert_eq!(t.nodes(), 1);
    assert!(t.list(ROOT).unwrap().is_empty());
    // The tree is usable again.
    t.write_file(ROOT, "/legion/run/x", b"x").unwrap();
    assert_eq!(account.scratch(), 1);
}

#[test]
fn an_unlinked_open_file_keeps_its_bytes_until_dropped_test() {
    let (mut t, account) = tree(MIB);
    t.write_file(ROOT, "/f", b"still here").unwrap();
    let held = file(&t, "/f");
    t.unlink(ROOT, "f").unwrap();
    assert_eq!(code(t.lookup(ROOT, "f")), ErrorCode::NoEntry);
    assert_eq!(lock(&held).bytes(), b"still here");
    assert_eq!(t.stat(&Node::File(held.clone())).unwrap().link_count, 0);
    assert_eq!(account.scratch(), 10);
    drop(held);
    assert_eq!(account.scratch(), 0);
    assert_eq!(t.nodes(), 1);
}

#[test]
fn node_cap_is_insufficient_space_test() {
    let (mut t, _) = tree(MIB);
    // The root is one node.
    for i in 0..MAX_NODES - 1 {
        t.open(ROOT, &format!("f{i}"), create()).unwrap();
    }
    assert_eq!(t.nodes(), MAX_NODES);
    assert_eq!(
        code(t.open(ROOT, "one-more", create())),
        ErrorCode::InsufficientSpace
    );
    assert_eq!(
        code(t.create_dir(ROOT, "dir")),
        ErrorCode::InsufficientSpace
    );
    assert_eq!(
        t.write_file(ROOT, "/x", b""),
        Err(ErrorCode::InsufficientSpace)
    );
    // Opening an existing file needs no node.
    t.open(ROOT, "f0", create()).unwrap();
    t.unlink(ROOT, "f0").unwrap();
    t.create_dir(ROOT, "dir").unwrap();
}

// ---------------------------------------------------------------- directories

#[test]
fn create_and_remove_directories_test() {
    let (mut t, _) = tree(MIB);
    t.create_dir(ROOT, "d").unwrap();
    assert_eq!(code(t.create_dir(ROOT, "d")), ErrorCode::Exist);
    assert_eq!(code(t.create_dir(ROOT, ".")), ErrorCode::Exist);
    assert_eq!(code(t.create_dir(ROOT, "missing/d")), ErrorCode::NoEntry);
    t.write_file(ROOT, "/d/f", b"").unwrap();
    assert_eq!(
        code(t.create_dir_all(ROOT, "/d/f/g")),
        ErrorCode::NotDirectory
    );
    assert_eq!(code(t.remove_dir(ROOT, "d")), ErrorCode::NotEmpty);
    assert_eq!(code(t.remove_dir(ROOT, "d/f")), ErrorCode::NotDirectory);
    assert_eq!(code(t.remove_dir(ROOT, "missing")), ErrorCode::NoEntry);
    assert_eq!(code(t.remove_dir(ROOT, ".")), ErrorCode::Invalid);
    assert_eq!(code(t.unlink(ROOT, "d")), ErrorCode::IsDirectory);
    let d = dir_ino(&t, ROOT, "d");
    t.unlink(ROOT, "d/f").unwrap();
    t.remove_dir(ROOT, "d").unwrap();
    // A descriptor for a removed directory names nothing.
    assert_eq!(code(t.lookup(d, ".")), ErrorCode::NoEntry);
    assert_eq!(code(t.open(d, "x", create())), ErrorCode::NoEntry);
    assert_eq!(code(t.list(d)), ErrorCode::NoEntry);
}

#[test]
fn list_and_stat_test() {
    let (mut t, _) = tree(MIB);
    t.write_file(ROOT, "/d/b.txt", b"12").unwrap();
    t.create_dir(ROOT, "d/a").unwrap();
    let d = dir_ino(&t, ROOT, "d");
    assert_eq!(
        t.list(d).unwrap(),
        vec![
            ("a".to_string(), DescriptorType::Directory),
            ("b.txt".to_string(), DescriptorType::RegularFile)
        ]
    );
    let stat = t.stat(&Node::Dir(d)).unwrap();
    assert_eq!(stat.type_, DescriptorType::Directory);
    assert_eq!(stat.link_count, 3);
    let stat = t.stat(&t.lookup(ROOT, "d/b.txt").unwrap()).unwrap();
    assert_eq!(stat.type_, DescriptorType::RegularFile);
    assert_eq!((stat.size, stat.link_count), (2, 1));
    assert!(stat.data_modification_timestamp.is_some());
}

// ---------------------------------------------------------------- rename

#[test]
fn rename_file_semantics_test() {
    let (mut t, account) = tree(MIB);
    t.write_file(ROOT, "/a/f", b"from").unwrap();
    t.write_file(ROOT, "/b/g", b"replaced!").unwrap();
    t.create_dir(ROOT, "c").unwrap();
    // Across directories, replacing a file: the old target's bytes go back.
    t.rename(ROOT, "a/f", ROOT, "b/g").unwrap();
    assert_eq!(code(t.lookup(ROOT, "a/f")), ErrorCode::NoEntry);
    assert_eq!(lock(&file(&t, "/b/g")).bytes(), b"from");
    assert_eq!(account.scratch(), 4);
    assert_eq!(
        code(t.rename(ROOT, "b/g", ROOT, "c")),
        ErrorCode::IsDirectory
    );
    assert_eq!(
        code(t.rename(ROOT, "missing", ROOT, "x")),
        ErrorCode::NoEntry
    );
    assert_eq!(
        code(t.rename(ROOT, "b/g", ROOT, "missing/x")),
        ErrorCode::NoEntry
    );
    assert_eq!(code(t.rename(ROOT, "b/g", ROOT, ".")), ErrorCode::Invalid);
    // Onto itself is a no-op.
    t.rename(ROOT, "b/g", ROOT, "b/../b/g").unwrap();
    assert_eq!(lock(&file(&t, "/b/g")).bytes(), b"from");
}

#[test]
fn rename_directory_semantics_test() {
    let (mut t, _) = tree(MIB);
    t.write_file(ROOT, "/a/inner/f", b"x").unwrap();
    t.create_dir_all(ROOT, "/empty").unwrap();
    t.write_file(ROOT, "/full/f", b"y").unwrap();
    t.write_file(ROOT, "/file", b"z").unwrap();
    assert_eq!(
        code(t.rename(ROOT, "a", ROOT, "a/inner/sub")),
        ErrorCode::Invalid
    );
    assert_eq!(code(t.rename(ROOT, "a", ROOT, "full")), ErrorCode::NotEmpty);
    assert_eq!(
        code(t.rename(ROOT, "a", ROOT, "file")),
        ErrorCode::NotDirectory
    );
    let nodes = t.nodes();
    t.rename(ROOT, "a", ROOT, "empty").unwrap();
    // The replaced empty directory is gone and freed its node.
    assert_eq!(t.nodes(), nodes - 1);
    let inner = dir_ino(&t, ROOT, "empty/inner");
    // `..` follows the move.
    assert_eq!(dir_ino(&t, inner, "../.."), ROOT);
    t.rename(ROOT, "empty", ROOT, "full/moved").unwrap();
    assert_eq!(dir_ino(&t, inner, "../.."), dir_ino(&t, ROOT, "full"));
    assert_eq!(lock(&file(&t, "/full/moved/inner/f")).bytes(), b"x");
}

#[test]
fn set_times_test() {
    let (mut t, _) = tree(MIB);
    t.write_file(ROOT, "/f", b"").unwrap();
    let node = t.lookup(ROOT, "f").unwrap();
    let when = Datetime {
        seconds: 1_000_000,
        nanoseconds: 5,
    };
    t.set_times(&node, Some(when), None).unwrap();
    let stat = t.stat(&node).unwrap();
    assert_eq!(stat.data_access_timestamp.unwrap().seconds, 1_000_000);
    assert_ne!(stat.data_modification_timestamp.unwrap().seconds, 1_000_000);
    t.set_times(&Node::Dir(ROOT), None, Some(when)).unwrap();
    let stat = t.stat(&Node::Dir(ROOT)).unwrap();
    assert_eq!(stat.data_modification_timestamp.unwrap().nanoseconds, 5);
}

use super::bindings::wasi::clocks::wall_clock::Datetime;

/// Coarse (whole-millisecond) times an embedder would pass.
const T1: Datetime = Datetime {
    seconds: 1_800_000_000,
    nanoseconds: 123_000_000,
};
const T2: Datetime = Datetime {
    seconds: 1_800_000_001,
    nanoseconds: 7_000_000,
};

fn times(stat: &types::DescriptorStat) -> [(u64, u32); 3] {
    [
        stat.data_access_timestamp.unwrap(),
        stat.data_modification_timestamp.unwrap(),
        stat.status_change_timestamp.unwrap(),
    ]
    .map(|t| (t.seconds, t.nanoseconds))
}

const fn pair(t: Datetime) -> (u64, u32) {
    (t.seconds, t.nanoseconds)
}

#[test]
fn the_tree_never_reads_a_clock_test() {
    let (mut t, _) = tree(MIB);
    // Until the embedder sets a time, everything is the Unix epoch.
    t.write_file(ROOT, "/f", b"x").unwrap();
    let f = t.lookup(ROOT, "f").unwrap();
    assert_eq!(times(&t.stat(&f).unwrap()), [(0, 0); 3]);
    assert_eq!(times(&t.stat(&Node::Dir(ROOT)).unwrap()), [(0, 0); 3]);

    t.set_now(T1);
    t.create_dir_all(ROOT, "/d/e").unwrap();
    let e = t.lookup(ROOT, "d/e").unwrap();
    assert_eq!(times(&t.stat(&e).unwrap()), [pair(T1); 3]);
    // Writing through a file handle (as a stream does) uses the tree's time.
    lock(&file(&t, "/f")).write_at(1, b"y").unwrap();
    let stat = t.stat(&f).unwrap();
    assert_eq!(times(&stat), [(0, 0), pair(T1), pair(T1)]);

    t.set_now(T2);
    t.rename(ROOT, "f", ROOT, "d/g").unwrap();
    let stat = t.stat(&f).unwrap();
    assert_eq!(times(&stat), [(0, 0), pair(T1), pair(T2)]);
    let d = t.lookup(ROOT, "d").unwrap();
    assert_eq!(pair(t.stat(&d).unwrap().data_modification_timestamp.unwrap()), pair(T2));
    t.set_times(&f, None, None).unwrap();
    assert_eq!(pair(t.stat(&f).unwrap().status_change_timestamp.unwrap()), pair(T2));
}

#[test]
fn scratch_host_staging_uses_set_now_test() {
    let account = Arc::new(MemoryAccount::new(MIB));
    let mut s = Scratch::new(account);
    assert_eq!(pair(s.now()), (0, 0));
    s.set_now(T1);
    s.write_file("/legion/run/in.txt", b"in").unwrap();
    let node = s.tree.lookup(ROOT, "/legion/run/in.txt").unwrap();
    assert_eq!(times(&s.tree.stat(&node).unwrap()), [pair(T1); 3]);
}

// ---------------------------------------------------------------- Scratch

#[test]
fn scratch_host_api_test() {
    let account = Arc::new(MemoryAccount::new(64));
    let mut s = Scratch::new(account.clone());
    s.write_file("/legion/run/main.py", b"print(1)").unwrap();
    assert_eq!(
        s.read_file("/legion/run/main.py", 100),
        Some((b"print(1)".to_vec(), false))
    );
    assert_eq!(
        s.read_file("/legion/run/main.py", 5),
        Some((b"print".to_vec(), true))
    );
    assert_eq!(s.read_file("/legion/run", 5), None);
    assert_eq!(s.read_file("/legion/run/missing", 5), None);
    s.create_dir_all("/legion/run/out").unwrap();
    s.create_dir_all("/legion/run/out").unwrap();
    let e = s.write_file("/legion/run", b"x").unwrap_err();
    assert_eq!(e.code, ErrorCode::IsDirectory);
    let e = s.write_file("/legion/big", &[0; 100]).unwrap_err();
    assert_eq!(e.code, ErrorCode::InsufficientSpace);
    assert!(e.to_string().contains("/legion/big"), "{e}");
    assert_eq!(s.used_bytes(), 8);
    s.clear();
    assert_eq!(s.used_bytes(), 0);
    assert_eq!(account.scratch(), 0);
}

// ---------------------------------------------------------------- host functions

struct Ctx {
    scratch: Scratch,
    table: ResourceTable,
    account: Arc<MemoryAccount>,
}

impl Ctx {
    fn new(limit: u64) -> Self {
        let account = Arc::new(MemoryAccount::new(limit));
        let mut scratch = Scratch::new(account.clone());
        scratch.create_dir_all(RUN_DIR).unwrap();
        Self {
            scratch,
            table: ResourceTable::new(),
            account,
        }
    }

    fn view(&mut self) -> ScratchCtx<'_> {
        ScratchCtx {
            scratch: &mut self.scratch,
            table: &mut self.table,
            cwd: RUN_DIR,
            now: T1,
        }
    }
}

fn borrow<T: 'static>(r: &Resource<T>) -> Resource<T> {
    Resource::new_borrow(r.rep())
}

fn fs_code<T>(r: Result<T, FsError>) -> ErrorCode {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e.code().expect("an error code, not a trap"),
    }
}

/// The preopens, by name.
fn preopen(ctx: &mut ScratchCtx<'_>, name: &str) -> Resource<Descriptor> {
    let dirs = preopens::Host::get_directories(ctx).unwrap();
    dirs.into_iter()
        .find(|(_, n)| n == name)
        .map(|(fd, _)| fd)
        .unwrap_or_else(|| panic!("no preopen {name}"))
}

fn open(
    ctx: &mut ScratchCtx<'_>,
    dir: &Resource<Descriptor>,
    path: &str,
    oflags: OpenFlags,
    flags: DescriptorFlags,
) -> Result<Resource<Descriptor>, FsError> {
    ctx.open_at(
        borrow(dir),
        PathFlags::empty(),
        path.to_string(),
        oflags,
        flags,
    )
}

#[test]
fn preopens_are_cwd_then_root_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let dirs = preopens::Host::get_directories(&mut v).unwrap();
    let names: Vec<&str> = dirs.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(names, vec![".", "/"]);
    let cwd = borrow(&dirs[0].0);
    let root = borrow(&dirs[1].0);
    assert_eq!(v.get_type(borrow(&cwd)).unwrap(), DescriptorType::Directory);
    let run = open(
        &mut v,
        &root,
        "legion/run",
        OpenFlags::DIRECTORY,
        DescriptorFlags::READ,
    )
    .unwrap();
    assert!(v.is_same_object(borrow(&run), borrow(&cwd)).unwrap());
    assert!(!v.is_same_object(borrow(&run), borrow(&root)).unwrap());
    let (a, b) = (
        v.metadata_hash(borrow(&run)).unwrap(),
        v.metadata_hash(borrow(&cwd)).unwrap(),
    );
    assert_eq!((a.lower, a.upper), (b.lower, b.upper));
    let flags = v.get_flags(borrow(&run)).unwrap();
    assert!(flags.contains(DescriptorFlags::MUTATE_DIRECTORY));
}

#[test]
fn preopens_without_a_run_dir_offer_only_the_root_test() {
    let account = Arc::new(MemoryAccount::new(MIB));
    let mut scratch = Scratch::new(account);
    let mut table = ResourceTable::new();
    let mut v = ScratchCtx {
        scratch: &mut scratch,
        table: &mut table,
        cwd: RUN_DIR,
        now: T1,
    };
    let dirs = preopens::Host::get_directories(&mut v).unwrap();
    assert_eq!(dirs.len(), 1);
    assert_eq!(dirs[0].1, "/");
}

#[test]
fn absolute_paths_are_not_permitted_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let root = preopen(&mut v, "/");
    let r = open(
        &mut v,
        &root,
        "/legion",
        OpenFlags::empty(),
        DescriptorFlags::READ,
    );
    assert_eq!(fs_code(r), ErrorCode::NotPermitted);
    let r = v.stat_at(borrow(&root), PathFlags::empty(), "/legion".into());
    assert_eq!(fs_code(r), ErrorCode::NotPermitted);
}

#[test]
fn positional_read_write_and_descriptor_modes_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let cwd = preopen(&mut v, ".");
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    let f = open(&mut v, &cwd, "f", OpenFlags::CREATE, rw).unwrap();
    assert_eq!(v.write(borrow(&f), b"hello world".to_vec(), 0).unwrap(), 11);
    assert_eq!(v.read(borrow(&f), 5, 6).unwrap(), (b"world".to_vec(), true));
    assert_eq!(v.read(borrow(&f), 3, 0).unwrap(), (b"hel".to_vec(), false));
    assert_eq!(v.read(borrow(&f), 3, 50).unwrap(), (vec![], true));
    v.set_size(borrow(&f), 5).unwrap();
    assert_eq!(v.stat(borrow(&f)).unwrap().size, 5);
    assert_eq!(v.get_type(borrow(&f)).unwrap(), DescriptorType::RegularFile);
    v.sync(borrow(&f)).unwrap();
    v.sync_data(borrow(&f)).unwrap();
    v.advise(borrow(&f), 0, 0, types::Advice::Sequential)
        .unwrap();
    v.set_times(borrow(&f), NewTimestamp::Now, NewTimestamp::NoChange)
        .unwrap();

    let ro = open(&mut v, &cwd, "f", OpenFlags::empty(), DescriptorFlags::READ).unwrap();
    assert_eq!(
        fs_code(v.write(borrow(&ro), b"x".to_vec(), 0)),
        ErrorCode::BadDescriptor
    );
    assert_eq!(
        fs_code(v.set_size(borrow(&ro), 0)),
        ErrorCode::BadDescriptor
    );
    assert_eq!(
        fs_code(v.write_via_stream(borrow(&ro), 0)),
        ErrorCode::BadDescriptor
    );
    let wo = open(
        &mut v,
        &cwd,
        "f",
        OpenFlags::empty(),
        DescriptorFlags::WRITE,
    )
    .unwrap();
    assert_eq!(fs_code(v.read(borrow(&wo), 1, 0)), ErrorCode::BadDescriptor);
    assert_eq!(
        fs_code(v.read_via_stream(borrow(&wo), 0)),
        ErrorCode::BadDescriptor
    );
    // Directory descriptors are not files and vice versa.
    assert_eq!(fs_code(v.read(borrow(&cwd), 1, 0)), ErrorCode::IsDirectory);
    assert_eq!(
        fs_code(v.read_directory(borrow(&f))),
        ErrorCode::NotDirectory
    );
    let r = open(&mut v, &f, "x", OpenFlags::empty(), DescriptorFlags::READ);
    assert_eq!(fs_code(r), ErrorCode::NotDirectory);
    HostDescriptor::drop(&mut v, f).unwrap();
}

#[test]
fn directory_functions_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let cwd = preopen(&mut v, ".");
    v.create_directory_at(borrow(&cwd), "d".into()).unwrap();
    assert_eq!(
        fs_code(v.create_directory_at(borrow(&cwd), "d".into())),
        ErrorCode::Exist
    );
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    open(&mut v, &cwd, "d/f", OpenFlags::CREATE, rw).unwrap();
    open(&mut v, &cwd, "../sibling", OpenFlags::CREATE, rw).unwrap();
    let stream = v.read_directory(borrow(&cwd)).unwrap();
    let mut names = vec![];
    while let Some(e) = v.read_directory_entry(borrow(&stream)).unwrap() {
        names.push((e.name, e.type_));
    }
    HostDirectoryEntryStream::drop(&mut v, stream).unwrap();
    assert_eq!(names, vec![("d".to_string(), DescriptorType::Directory)]);
    let stat = v
        .stat_at(borrow(&cwd), PathFlags::empty(), "../sibling".into())
        .unwrap();
    assert_eq!(stat.type_, DescriptorType::RegularFile);
    assert_eq!(
        fs_code(v.remove_directory_at(borrow(&cwd), "d".into())),
        ErrorCode::NotEmpty
    );
    v.rename_at(borrow(&cwd), "d/f".into(), borrow(&cwd), "g".into())
        .unwrap();
    v.unlink_file_at(borrow(&cwd), "g".into()).unwrap();
    v.remove_directory_at(borrow(&cwd), "d".into()).unwrap();
    assert_eq!(
        fs_code(v.stat_at(borrow(&cwd), PathFlags::empty(), "d".into())),
        ErrorCode::NoEntry
    );
    v.set_times_at(
        borrow(&cwd),
        PathFlags::empty(),
        "../sibling".into(),
        NewTimestamp::NoChange,
        NewTimestamp::Now,
    )
    .unwrap();
    let a = v
        .metadata_hash_at(borrow(&cwd), PathFlags::empty(), "../sibling".into())
        .unwrap();
    let root = preopen(&mut v, "/");
    let b = v
        .metadata_hash_at(borrow(&root), PathFlags::empty(), "legion/sibling".into())
        .unwrap();
    assert_eq!((a.lower, a.upper), (b.lower, b.upper));
}

#[test]
fn links_are_not_supported_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let cwd = preopen(&mut v, ".");
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    open(&mut v, &cwd, "f", OpenFlags::CREATE, rw).unwrap();
    let r = v.symlink_at(borrow(&cwd), "f".into(), "l".into());
    assert_eq!(fs_code(r), ErrorCode::NotPermitted);
    let r = v.symlink_at(borrow(&cwd), "x".into(), "f".into());
    assert_eq!(fs_code(r), ErrorCode::Exist);
    let r = v.link_at(
        borrow(&cwd),
        PathFlags::empty(),
        "f".into(),
        borrow(&cwd),
        "h".into(),
    );
    assert_eq!(fs_code(r), ErrorCode::NotPermitted);
    let r = v.link_at(
        borrow(&cwd),
        PathFlags::empty(),
        "nope".into(),
        borrow(&cwd),
        "h".into(),
    );
    assert_eq!(fs_code(r), ErrorCode::NoEntry);
    assert_eq!(
        fs_code(v.readlink_at(borrow(&cwd), "f".into())),
        ErrorCode::Invalid
    );
    assert_eq!(
        fs_code(v.readlink_at(borrow(&cwd), "nope".into())),
        ErrorCode::NoEntry
    );
}

#[tokio::test]
async fn streams_read_live_contents_and_append_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let cwd = preopen(&mut v, ".");
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    let f = open(&mut v, &cwd, "f", OpenFlags::CREATE, rw).unwrap();
    let w = v.write_via_stream(borrow(&f), 0).unwrap();
    let r = v.read_via_stream(borrow(&f), 0).unwrap();
    let a = v.append_via_stream(borrow(&f)).unwrap();
    let w: DynOutputStream = v.table.delete(w).unwrap();
    let r: DynInputStream = v.table.delete(r).unwrap();
    let a: DynOutputStream = v.table.delete(a).unwrap();
    let (mut w, mut r, mut a) = (w, r, a);
    // An empty file reads as closed (end of file).
    assert!(matches!(r.read(10), Err(StreamError::Closed)));
    assert!(w.check_write().unwrap() > 0);
    w.write(Bytes::from_static(b"abc")).unwrap();
    w.flush().unwrap();
    w.blocking_write_and_flush(Bytes::from_static(b"def"))
        .await
        .unwrap();
    a.write(Bytes::from_static(b"!")).unwrap();
    // The reader sees writes made after it was opened.
    assert_eq!(&r.read(4).unwrap()[..], b"abcd");
    assert_eq!(&r.blocking_read(10).await.unwrap()[..], b"ef!");
    assert!(matches!(r.read(1), Err(StreamError::Closed)));
    // The positional writer continues where it left off, over the append.
    w.write(Bytes::from_static(b"G")).unwrap();
    assert_eq!(v.read(borrow(&f), 100, 0).unwrap().0, b"abcdefG");
}

#[tokio::test]
async fn stream_write_past_the_ceiling_is_insufficient_space_test() {
    let mut c = Ctx::new(10);
    let account = c.account.clone();
    let mut v = c.view();
    let cwd = preopen(&mut v, ".");
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    let f = open(&mut v, &cwd, "f", OpenFlags::CREATE, rw).unwrap();
    let w = v.write_via_stream(borrow(&f), 0).unwrap();
    let mut w: DynOutputStream = v.table.delete(w).unwrap();
    w.write(Bytes::from_static(b"12345678")).unwrap();
    let err = match w.write(Bytes::from_static(b"9ABC")) {
        Err(StreamError::LastOperationFailed(e)) => e,
        other => panic!("expected last-operation-failed, got {other:?}"),
    };
    // The guest learns the code through filesystem-error-code.
    let err = v.table.push(err).unwrap();
    assert_eq!(
        types::Host::filesystem_error_code(&mut v, borrow(&err)).unwrap(),
        Some(ErrorCode::InsufficientSpace)
    );
    // The failed write left the file as it was, and closed the stream.
    assert_eq!(v.read(borrow(&f), 100, 0).unwrap().0, b"12345678");
    assert!(matches!(w.check_write(), Err(StreamError::Closed)));
    assert_eq!(account.scratch(), 8);
    // A positional write that fits still works.
    assert_eq!(v.write(borrow(&f), b"9A".to_vec(), 8).unwrap(), 2);
    assert_eq!(
        fs_code(v.write(borrow(&f), b"B".to_vec(), 10)),
        ErrorCode::InsufficientSpace
    );
}

#[test]
fn bad_handles_trap_test() {
    let mut c = Ctx::new(MIB);
    let mut v = c.view();
    let bogus = Resource::<Descriptor>::new_borrow(9999);
    let e = v.stat(bogus).unwrap_err();
    assert!(e.code().is_none());
    assert!(types::Host::convert_error_code(&mut v, e).is_err());
    assert_eq!(
        types::Host::convert_error_code(&mut v, ErrorCode::Exist.into()).unwrap(),
        ErrorCode::Exist
    );
}

impl ScratchView for Ctx {
    fn scratch(&mut self) -> ScratchCtx<'_> {
        self.view()
    }
}

#[test]
fn guest_calls_stamp_with_the_context_clock_test() {
    let mut c = Ctx::new(MIB);
    // What the linker does on every filesystem call.
    let mut v = super::scratch_ctx(&mut c);
    let cwd = preopen(&mut v, ".");
    let rw = DescriptorFlags::READ | DescriptorFlags::WRITE;
    let f = open(&mut v, &cwd, "f", OpenFlags::CREATE, rw).unwrap();
    v.write(borrow(&f), b"abc".to_vec(), 0).unwrap();
    assert_eq!(times(&v.stat(borrow(&f)).unwrap()), [pair(T1); 3]);

    // The next call carries a later turn's time.
    v.now = T2;
    let mut v = {
        let ScratchCtx { scratch, .. } = v;
        scratch.set_now(T2);
        ScratchCtx {
            scratch,
            table: &mut c.table,
            cwd: RUN_DIR,
            now: T2,
        }
    };
    v.set_times(borrow(&f), NewTimestamp::Now, NewTimestamp::NoChange)
        .unwrap();
    assert_eq!(
        times(&v.stat(borrow(&f)).unwrap()),
        [pair(T2), pair(T1), pair(T2)]
    );
    // A stream write after it uses the tree's time too.
    let w = v.write_via_stream(borrow(&f), 3).unwrap();
    let mut w: DynOutputStream = v.table.delete(w).unwrap();
    w.write(Bytes::from_static(b"d")).unwrap();
    assert_eq!(
        pair(v.stat(borrow(&f)).unwrap().data_modification_timestamp.unwrap()),
        pair(T2)
    );
}
