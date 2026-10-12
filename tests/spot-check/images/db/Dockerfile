# Postgres as the postgres user from the start. The stock entrypoint starts as
# root, chowns PGDATA, and drops privileges with gosu, which needs CAP_CHOWN
# and CAP_SETUID that Legion's Linux containers do not grant. Run as postgres,
# the entrypoint skips both. The volume must be writable by uid 70 (the job
# sets the local-dir volume's mode).
FROM public.ecr.aws/docker/library/postgres:16-alpine
USER postgres
