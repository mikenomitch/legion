{
    workerType: "default",
    pool: "default",
    additionalNodeChecks: [],
    attributes: {
        foo: "bar"
    },
    // Artifact fetch settings for workers of this type (docs/artifacts.md).
    // The values below are the defaults; the block can be omitted entirely.
    artifacts: {
        fetch: {
            // Concurrent fetches per worker.
            parallelism: 4,
            // Per-artifact retries before the allocation fails.
            attempts: 3,
            // Overall fetch deadline per artifact.
            timeout: "10m",
            // Per-artifact size cap; also bounds decompression.
            maxArtifactSize: "50GiB",
        },
    },
}
