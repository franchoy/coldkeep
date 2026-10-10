# syntax=docker/dockerfile:1
FROM golang:1.26.9-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c

ENV DEBIAN_FRONTEND=noninteractive \
    GOTOOLCHAIN=local

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      jq \
      postgresql-client \
      python3 \
      shellcheck && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/local/go/bin/go /usr/local/bin/go && \
    ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt && \
    useradd --create-home --shell /bin/bash --uid 1000 --user-group vscode

WORKDIR /workspaces/coldkeep
USER vscode

CMD ["sleep", "infinity"]
