//! Built-in artifact fetchers, one per scheme (artifact system plan §3).

mod file;
mod git;
mod http;
mod oci;
mod s3;

pub use file::FileFetcher;
pub use git::GitFetcher;
pub use http::HttpFetcher;
pub use oci::OciFetcher;
pub use s3::S3Fetcher;
