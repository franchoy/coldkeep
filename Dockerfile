# ---------- Builder ----------
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git (required for some Go modules)
RUN apk add --no-cache git

# Copy module files first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o coldkeep ./cmd/coldkeep


# ---------- Runtime ----------
FROM alpine:3.19

RUN apk add --no-cache \
    ca-certificates \
    bash

WORKDIR /app

# Copy binary
COPY --from=builder /app/coldkeep /usr/local/bin/coldkeep

# Copy scripts
COPY scripts/ ./scripts/

ENTRYPOINT ["coldkeep"]
CMD ["stats"]