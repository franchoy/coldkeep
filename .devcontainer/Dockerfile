# syntax=docker/dockerfile:1
FROM golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af

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
