# Opt-in egress for agent sandboxes: the common package registries and
# GitHub's clone hosts (docs/network-handlers.md, "Default deny").
#
# A gated task that no task block and no policy object covers denies every
# destination (`gated-default`). This policy is the shortcut for the read
# paths an agent usually needs. It is never applied for you: apply it on
# purpose, and `legion net policy show <job>` names it as the source of
# every rule it contributes.
#
#   legion net policy apply -f configs/policies/agent-sandbox-egress.ncl
#
# Read every line before you apply it. Behind HTTPS `CONNECT`, the proxy sees
# a host and a port, not a method or a path, so a host listed here is open
# for everything that host serves, uploads included where the host takes
# them. Each line says what it opens. Model APIs are not listed: which one
# your platform uses is yours to add, in the task or in a policy of your own.
{
    name = "agent-sandbox-egress",
    description = "Opt-in read paths for agent sandboxes: package registries and GitHub clones.",

    # Below any policy of your own, so yours wins a tie.
    "priority" = 10,

    selector = {
        runtimes = ["sandbox"],
    },

    # `default`: applies only to a task that declares no egress chain of its
    # own. A task with `network.egress` keeps exactly what it says.
    enforcement = "default",

    egress = {
        policy = "deny",
        allow = [
            # PyPI's index. Package uploads go to upload.pypi.org, which is
            # not listed.
            "pypi.org",
            # PyPI's package files (downloads only; nothing uploads here).
            "files.pythonhosted.org",
            # npm's registry. Reads AND `npm publish`: the proxy cannot tell
            # them apart behind CONNECT, so a token in the sandbox can publish.
            "registry.npmjs.org",
            # crates.io's API (index queries, and `cargo publish` with a token).
            "crates.io",
            # crates.io's download host (crate files only).
            "static.crates.io",
            # crates.io's sparse index (read only).
            "index.crates.io",
            # Go's module proxy and checksum database (read only).
            "proxy.golang.org",
            "sum.golang.org",
            # GitHub over HTTPS. Clones AND pushes: a credential in the
            # sandbox can push to any repository it can write.
            "github.com",
            # GitHub's archive downloads (tarballs and zips; read only).
            "codeload.github.com",
            # GitHub release assets and raw files (read only).
            "objects.githubusercontent.com",
            "raw.githubusercontent.com",
        ],
        accessLog = true,
    },
}
