# The autoscaler's app config: the scaling plugins this cluster installs
# (docs/autoscaling.md, "Plugins"; docs/autoscaling-plugins.md).
#
#   legion app config set autoscaler -f configs/scaling/autoscaler-app-config.ncl
#
# Only the operator sets this: every plugin field but `namespaces` is a code
# field, so changing one needs the app's whole grant. Policies name a plugin and supply data
# (a query, a target value, a config record); they never carry a command or
# a URL. `namespaces` limits which namespaces' policies may use a plugin;
# leave it out to allow every namespace.
{
  interval = "15s",
  plugins = {
    # A Prometheus server, for teams in the payments namespaces. A tenant's
    # query reads whatever this server can see, so give each tenant group
    # its own source if their metrics must not mix.
    team-prom = {
      builtin = "prometheus",
      config = { address = "http://prometheus.internal:9090" },
      namespaces = ["payments-*"],
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
  },
}
