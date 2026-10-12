#!/bin/sh
# Renders the nginx config at start, because the listen port and the mesh
# address of the API are only known once Legion places the task. Everything
# nginx writes lives under /tmp (see the Dockerfile for why).
set -eu
: "${LEGION_PORT_HTTP:?set by Legion}"
: "${LEGION_UPSTREAM_API:?set by Legion from connect.upstreams}"

mkdir -p /tmp/nginx
cat >/tmp/nginx/default.conf <<CONF
client_body_temp_path /tmp/client_temp;
proxy_temp_path /tmp/proxy_temp;
fastcgi_temp_path /tmp/fastcgi_temp;
uwsgi_temp_path /tmp/uwsgi_temp;
scgi_temp_path /tmp/scgi_temp;

server {
  listen ${LEGION_PORT_HTTP};
  location /api/ { proxy_pass http://${LEGION_UPSTREAM_API}; }
  location /whoami { default_type text/plain; return 200 "frontend ${LEGION_ALLOC_ID:-?}\n"; }
  location / { root /usr/share/nginx/html; }
}
CONF
echo "frontend listening on ${LEGION_PORT_HTTP}, api via ${LEGION_UPSTREAM_API}"
exec nginx -g 'daemon off;'
