# syntax=docker/dockerfile:1

# Product image only. Development tooling belongs in .devcontainer/.
FROM --platform=$BUILDPLATFORM golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af AS builder

ARG TARGETOS=linux
ARG TARGETARCH

ENV GOTOOLCHAIN=local

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY db/ ./db/

RUN test "$(go env GOVERSION)" = "go1.26.10" && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -buildvcs=false -o /out/coldkeep ./cmd/coldkeep

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

WORKDIR /app

COPY --from=builder /out/coldkeep /usr/local/bin/coldkeep

ENTRYPOINT ["coldkeep"]
CMD ["version"]
