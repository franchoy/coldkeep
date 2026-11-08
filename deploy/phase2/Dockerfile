# syntax=docker/dockerfile:1
# Internal action 2.02 only. Run prepare_daemon.py at a clean source checkpoint
# before this build; generated assets are copied and validated by diagnostics.
FROM golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af AS builder
ENV GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY db/ ./db/
RUN test "$(go env GOVERSION)" = go1.26.10 && go build -trimpath -buildvcs=false -o /out/coldkeepd ./cmd/coldkeepd && /out/coldkeepd --diagnostics
FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
COPY --from=builder /out/coldkeepd /usr/local/bin/coldkeepd
USER 65532:65532
WORKDIR /app
ENTRYPOINT ["coldkeepd"]
