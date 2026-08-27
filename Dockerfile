# syntax=docker/dockerfile:1

# Product image only. Development tooling belongs in .devcontainer/.
FROM --platform=$BUILDPLATFORM golang:1.26.7-bookworm@sha256:e8c859f5632dcfde7b32d2012b4351728f6437930887c2f6a91ea242459e5514 AS builder

ARG TARGETOS=linux
ARG TARGETARCH

ENV GOTOOLCHAIN=local

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY db/ ./db/

RUN test "$(go env GOVERSION)" = "go1.26.7" && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath -buildvcs=false -o /out/coldkeep ./cmd/coldkeep

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

WORKDIR /app

COPY --from=builder /out/coldkeep /usr/local/bin/coldkeep

ENTRYPOINT ["coldkeep"]
CMD ["version"]
