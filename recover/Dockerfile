FROM rust:1.88-bookworm AS builder

WORKDIR /app

# Cache dependencies — copy manifests first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main(){}" > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY config ./config

# Rebuild only application code
RUN touch src/lib.rs src/main.rs \
    && cargo build --release --bin recover

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/recover ./recover
COPY --from=builder /app/config ./config

EXPOSE 8080

ENTRYPOINT ["./recover", "--config", "config/default.toml", "serve"]
