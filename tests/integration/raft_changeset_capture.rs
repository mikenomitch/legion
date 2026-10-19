//! Validates the core mechanism behind Raft persistence: a store mutation is
//! executed inside a savepoint on the proposing node with a SQLite session
//! attached, the resulting changeset is captured, the savepoint is rolled
//! back (so nothing is visible before quorum commit), and the changeset is
//! later applied — byte-identically — on every node's state machine.
//!
//! These tests pin the exact properties the replication layer depends on:
//! capture-then-rollback leaves the source untouched, apply reproduces the
//! effect exactly (including rowids), nested savepoints inside the captured
//! region behave, and tables without a declared PRIMARY KEY would be
//! silently ignored (which is why a migration test enforces PKs).

use rusqlite::session::{ConflictAction, Session};
use rusqlite::Connection;

fn test_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         CREATE TABLE u (k TEXT PRIMARY KEY, n INTEGER);
         INSERT INTO t VALUES (1, 'seed');
         INSERT INTO u VALUES ('a', 1);",
    )
    .expect("schema");
}

fn capture<F: FnOnce(&Connection)>(conn: &Connection, mutate: F) -> Vec<u8> {
    let mut session = Session::new(conn).expect("session");
    session.attach(None).expect("attach all tables");
    conn.execute_batch("SAVEPOINT capture_outer;")
        .expect("savepoint");
    mutate(conn);
    let mut changeset: Vec<u8> = Vec::new();
    session.changeset_strm(&mut changeset).expect("changeset");
    conn.execute_batch("ROLLBACK TO capture_outer; RELEASE capture_outer;")
        .expect("rollback");
    changeset
}

fn apply(conn: &Connection, changeset: &[u8]) {
    let mut input = changeset;
    conn.apply_strm(
        &mut input,
        None::<fn(&str) -> bool>,
        |_conflict_type, _item| ConflictAction::SQLITE_CHANGESET_ABORT,
    )
    .expect("apply changeset");
}

fn content_hash(conn: &Connection) -> String {
    let mut rows: Vec<String> = Vec::new();
    for table in ["t", "u"] {
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
            .unwrap();
        let col_count = stmt.column_count();
        let mut q = stmt.query([]).unwrap();
        while let Some(row) = q.next().unwrap() {
            let mut line = format!("{table}:");
            for i in 0..col_count {
                line.push_str(&format!(
                    "{:?}|",
                    row.get::<_, rusqlite::types::Value>(i).unwrap()
                ));
            }
            rows.push(line);
        }
    }
    rows.join("\n")
}

#[test]
fn capture_rollback_leaves_source_untouched_and_apply_reproduces_it() {
    let src = Connection::open_in_memory().unwrap();
    let dst = Connection::open_in_memory().unwrap();
    test_schema(&src);
    test_schema(&dst);

    let changeset = capture(&src, |conn| {
        conn.execute_batch(
            "INSERT INTO t VALUES (2, 'x');
             UPDATE u SET n = 5 WHERE k = 'a';
             DELETE FROM t WHERE id = 1;",
        )
        .unwrap();
    });
    assert!(!changeset.is_empty(), "changeset captured");

    // Rollback means the source still has only the seed state.
    let n: i64 = src
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1, "source rolled back to pre-mutation state");

    // Apply to both connections: they converge to identical content.
    apply(&src, &changeset);
    apply(&dst, &changeset);
    assert_eq!(content_hash(&src), content_hash(&dst), "replicas converge");

    let v: String = dst
        .query_row("SELECT v FROM t WHERE id = 2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "x");
    let gone: i64 = dst
        .query_row("SELECT count(*) FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(gone, 0, "delete replicated");
    let n: i64 = dst
        .query_row("SELECT n FROM u WHERE k = 'a'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 5, "update replicated");
}

#[test]
fn nested_savepoints_inside_capture_work() {
    let src = Connection::open_in_memory().unwrap();
    let dst = Connection::open_in_memory().unwrap();
    test_schema(&src);
    test_schema(&dst);

    // Store methods open their own transactions; under capture those become
    // nested savepoints. An inner rollback must be excluded from the changeset.
    let changeset = capture(&src, |conn| {
        conn.execute_batch(
            "SAVEPOINT inner1;
             INSERT INTO t VALUES (10, 'kept');
             RELEASE inner1;
             SAVEPOINT inner2;
             INSERT INTO t VALUES (11, 'discarded');
             ROLLBACK TO inner2; RELEASE inner2;",
        )
        .unwrap();
    });

    apply(&dst, &changeset);
    let kept: i64 = dst
        .query_row("SELECT count(*) FROM t WHERE id = 10", [], |r| r.get(0))
        .unwrap();
    let discarded: i64 = dst
        .query_row("SELECT count(*) FROM t WHERE id = 11", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        (kept, discarded),
        (1, 0),
        "inner rollback excluded from changeset"
    );
}

#[test]
fn rowids_replicate_exactly() {
    // AUTOINCREMENT/rowid values assigned on the leader must appear verbatim
    // on followers — no divergent id assignment.
    let src = Connection::open_in_memory().unwrap();
    let dst = Connection::open_in_memory().unwrap();
    for c in [&src, &dst] {
        c.execute_batch("CREATE TABLE ev (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);")
            .unwrap();
    }
    // Advance the source's autoincrement counter out from under the follower.
    src.execute_batch("INSERT INTO ev (body) VALUES ('a'); DELETE FROM ev;")
        .unwrap();

    let mut session = Session::new(&src).unwrap();
    session.attach(None).unwrap();
    src.execute_batch("SAVEPOINT cap;").unwrap();
    src.execute_batch("INSERT INTO ev (body) VALUES ('b');")
        .unwrap();
    let leader_id: i64 = src
        .query_row("SELECT id FROM ev", [], |r| r.get(0))
        .unwrap();
    let mut changeset: Vec<u8> = Vec::new();
    session.changeset_strm(&mut changeset).unwrap();
    src.execute_batch("ROLLBACK TO cap; RELEASE cap;").unwrap();
    drop(session);

    apply(&dst, &changeset);
    let follower_id: i64 = dst
        .query_row("SELECT id FROM ev", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        leader_id, follower_id,
        "rowid assigned by leader replicates verbatim"
    );
    assert_eq!(
        leader_id, 2,
        "leader id reflects its own autoincrement history"
    );
}

#[test]
fn tables_without_declared_primary_key_are_silently_ignored() {
    // This is the sharp edge that motivates the schema-wide PK audit test:
    // sessions skip tables with no declared PRIMARY KEY.
    let src = Connection::open_in_memory().unwrap();
    src.execute_batch("CREATE TABLE nopk (v TEXT);").unwrap();

    let mut session = Session::new(&src).unwrap();
    session.attach(None).unwrap();
    src.execute_batch("SAVEPOINT cap;").unwrap();
    src.execute_batch("INSERT INTO nopk VALUES ('lost');")
        .unwrap();
    let mut changeset: Vec<u8> = Vec::new();
    session.changeset_strm(&mut changeset).unwrap();
    src.execute_batch("ROLLBACK TO cap; RELEASE cap;").unwrap();
    drop(session);

    assert!(
        changeset.is_empty(),
        "changes to PK-less tables are NOT captured — every replicated table must declare a PK"
    );
}
