# Runs the read-only-free-tier plugin (configs/netplugins/) on every HTTP
# egress request from a task labeled tier = "free".
#
#   legion net plugin apply -f configs/netplugins/read-only-free-tier.ncl
#   legion net policy apply -f configs/policies/free-tier-read-only.ncl
{
    name = "free-tier-read-only",
    enforcement = "require",
    priority = 50,
    selector = { labels = { tier = "free" } },
    egress = {
        protocol = "http",
        policy = "allow",
        handlers = [
            { plugin = { name = "read-only-free-tier" } },
        ],
    },
}
