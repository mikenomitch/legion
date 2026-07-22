#![crate_type = "cdylib"]

//! Sample out-of-tree artifact fetcher plugin (artifact system plan Phase 6).
//!
//! Serves the `demo://` scheme by materializing the URL's `content` query
//! parameter as a single staged file — no network involved, which makes it a
//! self-contained example of the dylib fetcher contract:
//!
//! ```jsonnet
//! artifacts: [{ source: 'demo://example/hello.txt?content=hi', name: 'hello' }]
//! ```
//!
//! The host resolves unknown schemes to `plugins/artifacts/<scheme>/` and
//! calls the exported `get()`; everything else (auth, checksums, unpack,
//! status reporting) is shared pipeline code in the worker core.

use std::io::Write;

use legion::artifacts::fetcher::{
    ArtifactFetcher, FetchRequest, FetchedArtifact, FetchedPayload,
};
use legion::artifacts::stage::ArtifactSink;
use legion::util::R;

#[derive(Default)]
struct DemoFetcher;

impl ArtifactFetcher for DemoFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["demo"]
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let content = req
            .url
            .query_pairs()
            .find(|(key, _)| key == "content")
            .map(|(_, value)| value.into_owned())
            .unwrap_or_else(|| "demo artifact".to_string());
        let file_name = req
            .url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|segment| !segment.is_empty())
            .unwrap_or("demo.txt");

        let mut writer = sink.file_writer(file_name)?;
        writer.write_all(content.as_bytes())?;
        let (path, digest, bytes) = writer.finish()?;
        Ok(FetchedArtifact {
            bytes,
            payload: FetchedPayload::File { path, digest },
        })
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn ArtifactFetcher> {
    Box::new(DemoFetcher)
}
