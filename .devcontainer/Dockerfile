# syntax=docker/dockerfile:1
FROM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0

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
