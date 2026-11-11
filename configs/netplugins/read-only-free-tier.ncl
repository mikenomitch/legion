# A network plugin object (docs/network-plugins.md):
#
#   legion net plugin apply -f configs/netplugins/read-only-free-tier.ncl
#
# The module is the documented example, built from
# tests/netplugins/read-only-free-tier/ (tests/netplugins/build.sh). Host the
# .wasm where nodes can fetch it and point `source` at it; the checksum pins
# the exact bytes.
{
    name = "read-only-free-tier",
    description = "Free-tier tasks may only GET",
    module = {
        source = "https://artifacts.example.com/netplugins/read-only-free-tier.wasm",
        checksum = "sha256:ae94b6c52e3b2517d4acfdd64e58b4268b3898e7df15b80db66a5ac8317dac53",
    },
    stages = ["request"],
    usableBy = "policies",
    limits = { memory = "16Mi", deadline = "10ms" },
}
