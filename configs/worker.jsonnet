{
  server: {
    port: 4949,
    address: "127.0.0.1",
  },
  workerConfig: {
    fingerprinters: [
        "os",
        "cpu",
        "memory",
        "disk",
        "network",
        "docker"
    ],
  },
}
