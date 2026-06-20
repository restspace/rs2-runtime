//! Guards the committed V8 prelude snapshot against drift. If the bootstrap or
//! `js_prelude.js` change without regenerating the blob, the engine would still
//! be correct (it bakes the *old* prelude) but silently miss the edit — so fail
//! loudly and point at the fix.
#![cfg(feature = "js")]

#[test]
fn prelude_snapshot_is_current() {
    let committed = include_str!("../src/engines/js_prelude.snapshot.hash").trim();
    let blob = include_bytes!("../src/engines/js_prelude.snapshot.bin");

    // An empty blob means "no snapshot" — the engine falls back to running the
    // prelude from source, which is always correct. Allowed, but warn.
    if blob.is_empty() {
        eprintln!(
            "note: js_prelude.snapshot.bin is empty (running prelude from source). \
             Regenerate for the fast path: cargo run -p rs2-core --example gen-js-snapshot --features js"
        );
        return;
    }

    let current = format!("{:016x}", rs2_core::engines::js::prelude_snapshot_source_hash());
    assert_eq!(
        committed, current,
        "prelude snapshot is stale — bootstrap/js_prelude.js changed since it was generated.\n\
         Regenerate: cargo run -p rs2-core --example gen-js-snapshot --features js"
    );
}
