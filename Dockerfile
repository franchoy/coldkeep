# syntax=docker/dockerfile:1

# Product image only. Development tooling belongs in .devcontainer/.
FROM --platform=$BUILDPLATFORM golang:1.26.9-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS builder

ARG TARGETOS=linux
ARG TARGETARCH

ENV GOTOOLCHAIN=local

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY db/ ./db/

RUN test "$(go env GOVERSION)" = "go1.26.9" && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -buildvcs=false -o /out/coldkeep ./cmd/coldkeep

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

WORKDIR /app

COPY --from=builder /out/coldkeep /usr/local/bin/coldkeep

ENTRYPOINT ["coldkeep"]
CMD ["version"]
