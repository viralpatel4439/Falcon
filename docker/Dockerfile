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

# Falcon is configured via the CLI (never env vars). Bake a default profile into
# the image (KV product, /data storage) at a fixed path, then serve from it.
# Override the product/settings by mounting your own profile at ~/.falcon or by
# running `docker run ... falcon-image install <feature> ...` before `serve`.
ENV HOME=/home/falcon
RUN falcon install kv --data-dir /data --http-bind 0.0.0.0:8080 && \
    falcon config set wire-bind 0.0.0.0:6380 && \
    falcon config set wire-enabled true

# Liveness uses /healthz (always 200 while the process is up). Orchestrators
# should use /readyz for readiness (503 until startup completes / a follower
# has caught up) and scrape /metrics for autoscaling signals.
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

# SIGTERM triggers graceful drain + final durable flush; give it time to finish.
STOPSIGNAL SIGTERM

# `serve` runs the node from the baked-in profile. To reconfigure, run the CLI:
#   docker run ... falcon-image config set http-bind 0.0.0.0:9090
# then serve. Serve flags can also override for one run: falcon serve --region ...
ENTRYPOINT ["falcon"]
CMD ["serve"]
