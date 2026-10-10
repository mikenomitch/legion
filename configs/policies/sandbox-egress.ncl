# A network policy object (docs/network-handlers.md). Apply it with
#
#   legion net policy apply -f configs/policies/sandbox-egress.ncl
#
# and every task the selector matches gets it on its next heartbeat — no
# restart, no rollout. `legion net policy show <job>` prints the merged result
# for any job, naming which policy contributed each rule.
{
    name = "sandbox-egress",
    description = "Deny-by-default egress for sandboxes on the untrusted pool.",

    # Higher applies first among `require` policies and wins among `default`
    # ones. Two `default` policies that tie on priority with overlapping
    # selectors are rejected at apply.
    "priority" = 100,

    # Which tasks this applies to. Every list is an OR; the fields are an AND;
    # an empty selector matches every task on the cluster.
    selector = {
        nodePools = ["untrusted"],   # node.pool (globs allowed)
        namespaces = ["default"],    # every job is in `default` (no user namespaces)
        runtimes = ["sandbox"],
        jobs = ["agent-*"],          # job id globs
        labels = { tier = "free" },  # task network.labels that must all match
    },

    # `require`: always applied, ahead of the task's own block; a job may add
    # rules and denials but never remove or widen.
    # `default`: applied only when the task declares no chain of its own.
    enforcement = "require",

    # Optionally impose a datapath mode: gated | host | auto.
    mode = "gated",

    egress = {
        protocol = "http",           # tcp (default) | http — never inferred
        policy = "deny",             # fallthrough when no rule matches
        allow = [
            "*.pypi.org",
            "files.pythonhosted.org",
            "api.github.com/repos/**",  # a path needs protocol = "http"
        ],
        rules = [
            # Explicit rules run after the `allow` sugar; first match wins.
            {
                when = { header = { authorization = "*" } },
                action = "deny",
                reason = "credentials must not leave the sandbox",
            },
        ],
        accessLog = true,
    },
}
