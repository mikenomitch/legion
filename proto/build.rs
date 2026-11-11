fn main() {
    // Explicit rerun triggers replace Cargo's default of re-running on any
    // change in the package.
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Deserialize, serde::Serialize)]")
        // Every JSON key Legion emits is camelCase: the jobspec, the CLI's
        // hand-built envelopes, and (through this) every API message the CLI
        // `--json` and MCP tools print. Enums and oneof variants keep their
        // names; map keys are data and are never renamed.
        //
        // `default`: a field missing from the JSON takes its proto3 default,
        // as it does on the wire. Proto messages are persisted as JSON (the
        // worker's state files among them), so without it a field added to a
        // message made every file written before it unreadable (`missing
        // field envelopeResources` reading `netd_state.json` after a worker
        // upgrade). Messages only: serde rejects a container `default` on
        // the enums and oneofs `type_attribute(".")` also reaches.
        .message_attribute(".", "#[serde(rename_all = \"camelCase\", default)]")
        .type_attribute("Job", "#[derive(tabled::Tabled)]")
        .type_attribute("Submission", "#[derive(tabled::Tabled)]")
        // A checkpoint part key must never reach a log line or a JSON dump:
        // Debug is hand-written to redact it (src/redacted.rs) and serde
        // skips it.
        .skip_debug("legion.CheckpointPartKey")
        .field_attribute("legion.CheckpointPartKey.key", "#[serde(skip)]")
        // The same for a session sealing key, which rides on envelope
        // payloads: never logged, never in the state file or session log.
        .skip_debug("legion.SessionSealKey")
        .field_attribute("legion.SessionSealKey.key", "#[serde(skip)]")
        // And for a job's chunk key, which rides on publish requests.
        .skip_debug("legion.CheckpointChunkKey")
        .field_attribute("legion.CheckpointChunkKey.key", "#[serde(skip)]")
        // The table shows the `blocked` status; the block itself is for
        // `-j` output and `job info`.
        .field_attribute("legion.Job.blocked", "#[tabled(skip)]")
        .field_attribute("legion.Job.reschedule_backoffs", "#[tabled(skip)]")
        // The include path doubles as tonic-build's `rerun-if-changed`
        // watch, so it names this directory and nothing wider: with the
        // repository root, editing a doc or a UI file re-ran this script.
        //
        // `isolate_host.proto` is the node-local protocol between a worker
        // and its `legion-isolate` host processes; it is its own package
        // and is not part of the public API.
        .compile_protos(&["legion.proto", "isolate_host.proto"], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {e}"));
}
