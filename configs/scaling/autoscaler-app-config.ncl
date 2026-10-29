# The autoscaler's app config: the scaling plugins this cluster installs
# (docs/autoscaling.md, "Plugins"; docs/autoscaling-plugins.md).
#
#   legion app config set autoscaler -f configs/scaling/autoscaler-app-config.ncl
#
# Only the operator sets this. A plugin's `source`, `command`, `url`, and an
# `exec` target's `config.command` are code fields: changing one needs the
# app's whole grant, not just `app:write`. Policies name a plugin and supply data
# (a query, a target value, a config record); they never carry a command or
# a URL. `namespaces` limits which namespaces' policies may use a plugin;
# leave it out to allow every namespace.
{
  interval = "15s",
  plugins = {
    # A Prometheus server, for the payments teams and the media team's render
    # farm (render-farm.ncl). A tenant's query reads whatever this server can
    # see, so give each tenant group its own source if their metrics must
    # not mix.
    team-prom = {
      builtin = "prometheus",
      config = { address = "http://prometheus.internal:9090" },
      namespaces = ["payments-*", "media"],
    },
    # A JSON document over HTTP; a check's query is a JSON pointer into it.
    ingest-stats = {
      builtin = "http",
      config = { url = "http://ingest.internal:8080/stats", timeout = "5s" },
      namespaces = ["ingest"],
    },
    # One-shot commands for a target outside Legion (see render-farm.ncl).
    render-exec = {
      builtin = "exec",
      config = { command = ["/opt/legion/render-farm.sh"] },
      namespaces = ["media"],
    },
    # An external strategy, fetched as an artifact into the app's task
    # directory (code/plugins/calendar/) and run from there.
    calendar = {
      source = "https://example.com/legion/calendar-strategy-0.1.0.tar.gz",
      checksum = "sha256:0000000000000000000000000000000000000000000000000000000000000000",
      command = ["python3", "calendar_strategy.py"],
      config = { windows = [{ start = "02:00", end = "03:30" }] },
    },
    # A plugin served by its own Legion service, reached over HTTP.
    forecast = { url = "http://forecast.internal:8080/rpc" },
    # Node groups (nodes-aws.ncl): the first-party AWS Auto Scaling target
    # (plugins/scaling/aws-asg). Its credentials come from the `aws`
    # identity below, exchanged with STS: no access keys anywhere.
    aws-asg = {
      source = "https://example.com/legion/aws-asg-0.1.0-linux-amd64.tar.gz",
      checksum = "sha256:0000000000000000000000000000000000000000000000000000000000000000",
      config = {
        region = "us-east-1",
        webIdentityTokenFile = "$LEGION_TASK_DIR/secrets/aws.token",
        roleArn = "arn:aws:iam::123456789012:role/legion-autoscaler",
      },
    },
    # A scripted node provider (nodes-exec.ncl).
    scripted = {
      builtin = "exec",
      config = { command = ["/opt/legion/nodes.sh"] },
    },
  },
  # A token the autoscaler's job holds for AWS STS (an IAM OIDC provider for
  # this cluster's issuer, and a role trusting it, are the AWS side:
  # docs/workload-identity.md, "AWS").
  identities = [
    {
      name = "aws",
      audience = ["sts.amazonaws.com"],
      ttl = "15m",
      file = { path = "secrets/aws.token" },
    },
  ],
}
