# ---------- Builder ----------
FROM golang:1.23-alpine AS builder

WORKDIR /src

# Copy module files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY app/ ./app/

WORKDIR /src/app

RUN go build -o /capsule

# ---------- Runtime ----------
FROM alpine:3.19

WORKDIR /app

COPY --from=builder /capsule /usr/local/bin/capsule

ENTRYPOINT ["capsule"]