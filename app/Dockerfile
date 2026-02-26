# ---------- Builder ----------
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git (required for some Go modules)
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY app/ .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o coldkeep .

# ---------- Runtime ----------
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    bash

WORKDIR /app

# Copy binary
COPY --from=builder /app/coldkeep /usr/local/bin/coldkeep

# Copy scripts for smoke/testing
COPY scripts/ ./scripts/

# Default command
ENTRYPOINT ["coldkeep"]
CMD ["stats"]