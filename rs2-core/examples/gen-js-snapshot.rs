//! Regenerate the V8 prelude startup snapshot.
//!
//!   cargo run -p rs2-core --example gen-js-snapshot --features js
//!
//! Writes `rs2-core/src/engines/js_prelude.snapshot.bin`. Re-run whenever the
//! bootstrap or `js_prelude.js` change (the `prelude_snapshot_stale` test will
//! fail until you do). The snapshot is a pure perf optimization — the engine
//! falls back to running the prelude from source if the blob is empty.
//!
//! This MUST run in its own process: deno_core initializes V8 in snapshot mode
//! here, and the serving binary initializes it in normal mode — V8 forbids both
//! in one process.
#[cfg(feature = "js")]
fn main() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/engines");
    let blob = rs2_core::engines::js::generate_prelude_snapshot();
    std::fs::write(format!("{dir}/js_prelude.snapshot.bin"), &blob).expect("write snapshot");
    // Sidecar hash so a test can detect a stale blob after a prelude edit.
    let hash = rs2_core::engines::js::prelude_snapshot_source_hash();
    std::fs::write(
        format!("{dir}/js_prelude.snapshot.hash"),
        format!("{hash:016x}\n"),
    )
    .expect("write hash");
    println!(
        "wrote js_prelude.snapshot.bin ({} bytes), hash {hash:016x}",
        blob.len()
    );
}

#[cfg(not(feature = "js"))]
fn main() {
    eprintln!("build with --features js to generate the snapshot");
    std::process::exit(1);
}
