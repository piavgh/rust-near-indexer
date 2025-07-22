# Multi-stage build for NEAR Intents Indexer
# Stage 1: Build stage using Rust image
FROM rust:1.85 AS builder
WORKDIR /tmp/

# Optimize caching by copying dependencies first
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch

# Copy source code and migrations for build
COPY ./src ./src
COPY ./migrations ./migrations
RUN cargo build -p near-intents-indexer --release

# Stage 2: Runtime stage using minimal Ubuntu image
FROM ubuntu:22.04
RUN apt update && apt install -yy --no-install-recommends openssl ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

# Copy binary and migrations (PostgreSQL migrations embedded, ClickHouse needs runtime access)
COPY --from=builder /tmp/target/release/near-intents-indexer .
COPY --from=builder /tmp/migrations ./migrations
RUN chmod +x near-intents-indexer

ENTRYPOINT ["./near-intents-indexer"]
