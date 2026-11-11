//! Exercises `std::fs` (Rust std -> wasi-libc -> `wasi:filesystem`) against
//! the isolate host's in-memory scratch filesystem.
//!
//! The host stages `/legion/run/input.json` and `/legion/data/staged.txt`
//! before the run and gives the scratch tree a small memory ceiling (the
//! ENOSPC check writes past it). Prints `ok <check>` or `FAIL <check>: why`
//! per check, then `done <passed>/<total>`, and exits 1 if any check failed.
//! It leaves `result.json` and `out/report.txt` in the run directory for the
//! host to read back.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};

type Check = Result<(), String>;

fn ensure(cond: bool, what: impl Into<String>) -> Check {
    if cond {
        Ok(())
    } else {
        Err(what.into())
    }
}

fn io<T>(r: std::io::Result<T>, what: &str) -> Result<T, String> {
    r.map_err(|e| format!("{what}: {e} ({:?})", e.kind()))
}

fn expect_err<T>(r: std::io::Result<T>, kind: ErrorKind, what: &str) -> Check {
    match r {
        Ok(_) => Err(format!("{what}: succeeded, expected {kind:?}")),
        Err(e) if e.kind() == kind => Ok(()),
        Err(e) => Err(format!(
            "{what}: got {e} ({:?}), expected {kind:?}",
            e.kind()
        )),
    }
}

fn read(path: &str) -> Result<String, String> {
    io(fs::read_to_string(path), path)
}

const INPUT: &str = r#"{"n":1}"#;

/// wasi-libc (as of the Rust 1.97 toolchain) starts every program in `/`:
/// it does not read `wasi:cli/environment.initial-cwd`, and a `.` preopen
/// cannot stand in for it (wasi-libc strips `.` and `/` preopen names to
/// the same empty prefix). So relative paths resolve from `/` until the
/// program changes directory, which is what a language image's runner does
/// first. Report what we started in, check it is consistent, then `chdir`.
fn cwd() -> Check {
    let start = io(std::env::current_dir(), "current_dir")?;
    println!("info initial_cwd={}", start.display());
    if start.to_str() == Some("/") {
        ensure(read("legion/run/input.json")? == INPUT, "relative to /")?;
    }
    io(std::env::set_current_dir("/legion/run"), "chdir")?;
    let dir = io(std::env::current_dir(), "current_dir")?;
    ensure(
        dir.to_str() == Some("/legion/run"),
        format!("cwd is {dir:?}"),
    )?;
    expect_err(
        std::env::set_current_dir("/legion/missing"),
        ErrorKind::NotFound,
        "chdir missing",
    )
}

fn read_staged() -> Check {
    ensure(read("input.json")? == INPUT, "relative read")?;
    ensure(read("./input.json")? == INPUT, "./ read")?;
    ensure(read("/legion/run/input.json")? == INPUT, "absolute read")?;
    ensure(
        read("/legion/data/staged.txt")? == "staged",
        "absolute other dir",
    )
}

fn dotdot() -> Check {
    ensure(read("../run/input.json")? == INPUT, "../run")?;
    ensure(read("../data/staged.txt")? == "staged", "../data")?;
    ensure(
        read("../../legion/data/staged.txt")? == "staged",
        "../../legion",
    )?;
    // `..` at the root stays at the root.
    ensure(read("/../../legion/data/staged.txt")? == "staged", "/../..")?;
    ensure(
        read("../../../../../legion/run/input.json")? == INPUT,
        "many ..",
    )
}

fn write_relative_read_absolute() -> Check {
    io(fs::write("out.txt", "hello"), "write out.txt")?;
    ensure(read("/legion/run/out.txt")? == "hello", "read back")
}

fn append() -> Check {
    let mut f = io(
        OpenOptions::new().append(true).open("out.txt"),
        "open append",
    )?;
    io(f.write_all(b" world"), "append")?;
    drop(f);
    ensure(read("out.txt")? == "hello world", "appended contents")
}

fn create_new() -> Check {
    expect_err(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open("out.txt"),
        ErrorKind::AlreadyExists,
        "create_new on existing",
    )?;
    let mut f = io(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open("fresh.txt"),
        "create_new",
    )?;
    io(f.write_all(b"fresh"), "write fresh")?;
    ensure(read("fresh.txt")? == "fresh", "fresh contents")
}

fn truncate() -> Check {
    let mut f = io(
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("out.txt"),
        "open truncate",
    )?;
    io(f.write_all(b"x"), "write")?;
    drop(f);
    ensure(read("out.txt")? == "x", "truncated contents")?;
    ensure(io(fs::metadata("out.txt"), "metadata")?.len() == 1, "len 1")
}

fn dirs() -> Check {
    io(fs::create_dir_all("a/b/c"), "create_dir_all")?;
    io(fs::create_dir_all("a/b/c"), "create_dir_all again")?;
    io(fs::write("a/b/c/f1", "one"), "write f1")?;
    io(fs::write("/legion/run/a/b/c/f2", "two"), "write f2")?;
    io(fs::create_dir("a/b/c/sub"), "create_dir")?;
    expect_err(
        fs::create_dir("a/b/c/sub"),
        ErrorKind::AlreadyExists,
        "create_dir twice",
    )?;
    expect_err(
        fs::create_dir("nope/x"),
        ErrorKind::NotFound,
        "create_dir no parent",
    )?;
    let mut names = vec![];
    for entry in io(fs::read_dir("a/b/c"), "read_dir")? {
        let entry = io(entry, "entry")?;
        let ty = io(entry.file_type(), "file_type")?;
        names.push(format!(
            "{}:{}",
            entry.file_name().to_string_lossy(),
            if ty.is_dir() { "d" } else { "f" }
        ));
    }
    names.sort();
    ensure(
        names == ["f1:f", "f2:f", "sub:d"],
        format!("entries {names:?}"),
    )
}

fn metadata() -> Check {
    let m = io(fs::metadata("a/b/c/f1"), "metadata f1")?;
    ensure(m.is_file() && m.len() == 3, "f1 is a 3-byte file")?;
    ensure(io(fs::metadata("a"), "metadata a")?.is_dir(), "a is a dir")?;
    // The host stamps files with the wall clock it gives the guest; report
    // the exact value so the host can check nothing finer leaked.
    for (what, path) in [("mtime", "a/b/c/f1"), ("staged_mtime", "input.json")] {
        let modified = io(io(fs::metadata(path), path)?.modified(), "modified")?;
        let d = modified
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?;
        println!("info {what}={}.{:09}", d.as_secs(), d.subsec_nanos());
        ensure(d.as_secs() > 1_600_000_000, format!("{what} {d:?}"))?;
    }
    expect_err(
        fs::metadata("missing"),
        ErrorKind::NotFound,
        "metadata missing",
    )
}

fn rename() -> Check {
    io(fs::rename("a/b/c/f1", "/legion/run/moved"), "rename file")?;
    expect_err(fs::read("a/b/c/f1"), ErrorKind::NotFound, "old name")?;
    ensure(read("moved")? == "one", "moved contents")?;
    // Over an existing file.
    io(fs::rename("moved", "fresh.txt"), "rename over")?;
    ensure(read("fresh.txt")? == "one", "replaced contents")?;
    io(fs::rename("a/b", "a2"), "rename dir")?;
    ensure(read("a2/c/f2")? == "two", "file under moved dir")
}

fn remove() -> Check {
    // ENOTEMPTY (55 on WASI); Rust std does not classify it as
    // `DirectoryNotEmpty` on this target, so check the errno.
    match fs::remove_dir("a2") {
        Err(e) if e.raw_os_error() == Some(55) => {}
        other => {
            return Err(format!(
                "remove_dir non-empty: {other:?}, expected ENOTEMPTY"
            ))
        }
    }
    ensure(
        fs::remove_file("a2").is_err(),
        "remove_file on a dir must fail",
    )?;
    io(fs::remove_file("fresh.txt"), "remove_file")?;
    expect_err(fs::read("fresh.txt"), ErrorKind::NotFound, "removed file")?;
    io(fs::remove_dir_all("a2"), "remove_dir_all")?;
    io(fs::remove_dir("a"), "remove_dir")?;
    ensure(!fs::exists("a").unwrap_or(true), "a is gone")
}

fn errors() -> Check {
    expect_err(fs::read("nope"), ErrorKind::NotFound, "read missing")?;
    io(fs::create_dir("d"), "mkdir d")?;
    ensure(fs::read("d").is_err(), "reading a directory must fail")?;
    expect_err(
        fs::read("out.txt/x"),
        ErrorKind::NotADirectory,
        "file as a dir",
    )?;
    ensure(
        fs::hard_link("out.txt", "hard").is_err(),
        "hard links unsupported",
    )?;
    io(fs::remove_dir("d"), "rmdir d")
}

fn seek() -> Check {
    io(fs::write("seek.txt", "0123456789"), "write")?;
    let mut f = io(File::open("seek.txt"), "open")?;
    io(f.seek(SeekFrom::Start(5)), "seek start")?;
    let mut s = String::new();
    io(f.read_to_string(&mut s), "read")?;
    ensure(s == "56789", format!("from 5: {s}"))?;
    io(f.seek(SeekFrom::End(-2)), "seek end")?;
    s.clear();
    io(f.read_to_string(&mut s), "read")?;
    ensure(s == "89", format!("from end-2: {s}"))?;
    let mut f = io(
        OpenOptions::new().read(true).write(true).open("seek.txt"),
        "open rw",
    )?;
    io(f.seek(SeekFrom::Start(2)), "seek")?;
    io(f.write_all(b"AB"), "write at 2")?;
    io(f.seek(SeekFrom::Current(1)), "seek current")?;
    io(f.write_all(b"C"), "write at 5")?;
    drop(f);
    ensure(read("seek.txt")? == "01AB4C6789", "overwritten in place")
}

fn set_len() -> Check {
    let f = io(OpenOptions::new().write(true).open("seek.txt"), "open")?;
    io(f.set_len(3), "shrink")?;
    io(f.set_len(5), "grow")?;
    drop(f);
    let bytes = io(fs::read("seek.txt"), "read")?;
    ensure(bytes == b"01A\0\0", format!("{bytes:?}"))
}

/// Past the ceiling the host gives this tree. `set_len` (`set-size`) gets
/// ENOSPC. `write` goes through a `wasi:io` output stream, and wasi-libc
/// turns every stream failure into EIO (it never asks
/// `filesystem-error-code`), so that one is reported, not required.
fn enospc() -> Check {
    let f = io(File::create("big.bin"), "create big.bin")?;
    expect_err(
        f.set_len(8 << 20),
        ErrorKind::StorageFull,
        "set_len past the ceiling",
    )?;
    ensure(
        io(f.metadata(), "metadata")?.len() == 0,
        "refused set_len left it empty",
    )?;
    drop(f);
    let big = vec![7u8; 8 << 20];
    match fs::write("big.bin", &big) {
        Ok(()) => return Err("an 8 MiB write fit".into()),
        Err(e) => match e.raw_os_error() {
            // ENOSPC, or EIO from wasi-libc's stream error mapping.
            Some(51) | Some(29) => println!(
                "info write_past_ceiling_errno={}",
                e.raw_os_error().unwrap_or(0)
            ),
            _ => return Err(format!("write: got {e} ({:?})", e.kind())),
        },
    }
    // What fit before the refusal stays; nothing past the ceiling.
    let len = io(fs::metadata("big.bin"), "metadata")?.len();
    ensure(
        len > 0 && len < 8 << 20,
        format!("partial big.bin is {len} bytes"),
    )?;
    io(fs::remove_file("big.bin"), "remove big.bin")?;
    // The space came back.
    io(fs::write("small.bin", [1u8; 1024]), "write after ENOSPC")?;
    io(fs::remove_file("small.bin"), "remove small.bin")
}

fn leave_results() -> Check {
    io(fs::create_dir_all("out"), "mkdir out")?;
    io(fs::write("out/report.txt", "report"), "write report")?;
    io(
        fs::write("result.json", r#"{"ok":true}"#),
        "write result.json",
    )
}

fn main() {
    let checks: &[(&str, fn() -> Check)] = &[
        ("cwd", cwd),
        ("read_staged", read_staged),
        ("dotdot", dotdot),
        ("write_relative_read_absolute", write_relative_read_absolute),
        ("append", append),
        ("create_new", create_new),
        ("truncate", truncate),
        ("dirs", dirs),
        ("metadata", metadata),
        ("rename", rename),
        ("remove", remove),
        ("errors", errors),
        ("seek", seek),
        ("set_len", set_len),
        ("enospc", enospc),
        ("leave_results", leave_results),
    ];
    let mut passed = 0;
    for (name, check) in checks {
        match check() {
            Ok(()) => {
                passed += 1;
                println!("ok {name}");
            }
            Err(why) => println!("FAIL {name}: {why}"),
        }
    }
    println!("done {passed}/{}", checks.len());
    if passed != checks.len() {
        std::process::exit(1);
    }
}
