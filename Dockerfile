# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1.83-slim-bookworm AS builder
WORKDIR /app

# System deps for building (rustls uses ring; needs a C toolchain).
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy the whole workspace and build release binaries.
COPY . .
RUN cargo build --release --bin moaray --bin mock-upstream

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 moaray

COPY --from=builder /app/target/release/moaray /usr/local/bin/moaray
COPY --from=builder /app/target/release/mock-upstream /usr/local/bin/mock-upstream
COPY config.example.yaml /app/config.example.yaml

ENV MOARAY_CONFIG=/app/config.yaml
EXPOSE 8080
USER moaray

ENTRYPOINT ["moaray"]
