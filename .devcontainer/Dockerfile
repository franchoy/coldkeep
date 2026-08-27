# syntax=docker/dockerfile:1
FROM golang:1.26.7-bookworm@sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514

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
