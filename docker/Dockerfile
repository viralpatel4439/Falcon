# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml ./
COPY crates ./crates
RUN cargo build --release -p falcon-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 falcon

COPY --from=builder /build/target/release/falcon /usr/local/bin/falcon

USER falcon
WORKDIR /data
VOLUME ["/data"]

# 8080 = HTTP/WebSocket, 6380 = binary wire protocol, 7070 = gRPC replication
EXPOSE 8080 6380 7070

ENV FALCON_DATA_DIR=/data
ENV FALCON_HTTP_BIND=0.0.0.0:8080
ENV FALCON_WIRE_BIND=0.0.0.0:6380

# Liveness uses /healthz (always 200 while the process is up). Orchestrators
# should use /readyz for readiness (503 until startup completes / a follower
# has caught up) and scrape /metrics for autoscaling signals.
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

# SIGTERM triggers graceful drain + final durable flush; give it time to finish.
STOPSIGNAL SIGTERM

ENTRYPOINT ["falcon"]
