# An agent workspace: the tools a coding agent expects, plus a stand-in
# agent loop. Alpine keeps the microVM root disk small, and /bin/sh is
# required by the sandbox runtime's generated init.
FROM public.ecr.aws/docker/library/alpine:3.20
RUN apk add --no-cache bash curl git python3 jq vim ripgrep
RUN git config --system user.email agent@legion.local && git config --system user.name "Legion Agent" \
    && git config --system init.defaultBranch main
COPY --chmod=0755 agent.sh /usr/local/bin/agent
WORKDIR /workspace
CMD ["/usr/local/bin/agent"]
