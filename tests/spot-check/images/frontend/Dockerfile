# nginx as an unprivileged user, with every runtime-written path under /tmp.
#
# Two Legion behaviors on Linux shape this (see
# internal_docs/launch-spot-check-findings.md):
# - Containers get a minimal capability set (no CAP_CHOWN or CAP_SETUID), so
#   the stock image's root master, which chowns its cache dirs and drops to
#   the nginx user, cannot start. Running as nginx needs neither.
# - Layer extraction does not keep file ownership, so a `chown` here would not
#   survive into the container. /tmp is world-writable, so it works anyway.
# - A task's stdout and stderr are root-owned log files, so a non-root process
#   cannot reopen /dev/stdout or /dev/stderr by path (the stock image's log
#   symlinks). `error_log stderr` writes to fd 2 without reopening it.
FROM public.ecr.aws/nginx/nginx:1.27-alpine
RUN sed -i -e '/^user /d' -e 's#^pid .*#pid /tmp/nginx.pid;#' \
        -e 's#^error_log .*#error_log stderr notice;#' \
        -e 's#access_log .*#access_log off;#' \
        -e 's#include /etc/nginx/conf.d/\*.conf;#include /tmp/nginx/*.conf;#' /etc/nginx/nginx.conf
COPY index.html /usr/share/nginx/html/index.html
COPY --chmod=0755 start.sh /start.sh
USER nginx
CMD ["/start.sh"]
