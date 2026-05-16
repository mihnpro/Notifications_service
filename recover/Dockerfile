FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY config ./config

RUN cargo build --release --bin recover

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/recover /usr/local/bin/recover
COPY --from=builder /app/config ./config

ENV RECOVER_CONFIG=/app/config/default.toml

EXPOSE 8080

CMD ["recover", "serve"]
