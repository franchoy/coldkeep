# syntax=docker/dockerfile:1
# Internal action 2.02 only. Run prepare_daemon.py at a clean source checkpoint
# before this build; generated assets are copied and validated by diagnostics.
FROM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS builder
ENV GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY db/ ./db/
RUN test "$(go env GOVERSION)" = go1.26.11 && go build -trimpath -buildvcs=false -o /out/coldkeepd ./cmd/coldkeepd && /out/coldkeepd --diagnostics
FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
COPY --from=builder /out/coldkeepd /usr/local/bin/coldkeepd
USER 65532:65532
WORKDIR /app
ENTRYPOINT ["coldkeepd"]
