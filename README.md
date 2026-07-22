# Falcon

A fast, safe, fully-configurable data platform in Rust. Falcon bundles four
components behind one binary:

- **FalconDB** — the key-value store
- **Falcon Queue** — durable work queues
- **Falcon Pub/Sub** — topics
- **Falcon Realtime DB** — live WebSocket subscriptions

…plus TTL, multi-region replication (single- and multi-leader), and pluggable
storage — over HTTP, a lean binary protocol, and WebSockets. Everything optional
is **off by default and costs nothing when unused**.

## What it does

- **FalconDB** (key-value) with per-keyspace storage tiers:
  - `hot` — in-memory only (fastest, not durable)
  - `warm` — in-memory index + group-commit WAL (durable, **default**)
  - `cold` — disk-backed via sled (for datasets larger than RAM)
  - `tiered` — automatic hot/cold: hot working set in RAM, cold tail auto-spilled
    to disk (CLOCK eviction), so you can hold far more than RAM and pay RAM only
    for the working set
  - `file-per-key` — each value is an independent file/object (portable,
    inspectable; the seam for third-party object stores)
  - `sharded` — keys are **hashed into a fixed set of buckets**, each bucket
    stored as one object in the backing store, so N buckets = N objects no
    matter how many keys. The cost-efficient way to sit on a request-billed
    object store (vs. `file-per-key`'s one-object-per-key billing); O(1) point
    reads via an in-memory index
- **Pub/Sub topics** — `ephemeral` (fast, at-most-once) or `durable` (persisted,
  replayable, survives restart), selectable per topic
- **Work queues** — durable, at-least-once, with ack + redelivery-on-timeout and
  competing consumers per group
- **Event streams** (Falcon Event Streaming) — partitioned, offset-addressed,
  replayable logs (Kafka-shaped): records route to partitions by key hash
  (per-key total ordering), consumer groups keep durable per-partition offsets
  and resume after restart, and any group can replay history or live-tail
- **TTL** — per-write (`?ttl=<secs>`) or per-keyspace default; lazy + background
  reaper; expiries replicate and notify subscribers consistently
- **Replication** — single-leader (strong ordering, default) or multi-leader
  (active-active, HLC last-write-wins convergence), async over gRPC
- **Three protocols** — REST/JSON (`:8080`), a binary pipelined protocol (`:6380`),
  and WebSocket subscriptions
- **Optional auth** — a shared-secret token across all protocols, off by default
- **Safety** — `#![forbid(unsafe_code)]` on every crate (zero unsafe), fuzz-tested
  parsers, `cargo audit` clean, and a configurable **max value size** (anti-OOM)
- **Production-ready operations** (all on by default) — Prometheus `/metrics`,
  a `/readyz` readiness probe distinct from `/healthz` liveness, background **WAL
  compaction** (bounds disk + restart time), and **graceful shutdown** (SIGTERM
  drains in-flight requests and force-flushes buffered writes — zero-loss
  autoscaling/rollouts)

## Quickstart

```bash
cargo build --release -p falcon-cli
./target/release/falcon          # zero config: single node, warm tier, ./data
```

```bash
# CRUD over HTTP
curl -X PUT localhost:8080/kv/foo -d 'bar'
curl localhost:8080/kv/foo
curl -X PUT 'localhost:8080/kv/session?ttl=60' -d 'expires in 60s'
curl -X DELETE localhost:8080/kv/foo
curl 'localhost:8080/kv?prefix=user:'
curl localhost:8080/healthz          # shows the active feature set
```

## Config

Everything has a sane default; `config/default.toml` documents every option.

```bash
falcon --config config/default.toml
```

CLI flags (`--http-bind`, `--wire-bind`, `--node-id`, `--region`, `--data-dir`,
`--auth-token`, `--log-level`) and matching `FALCON_*` env vars override the file,
in order: defaults < file < env < flags.

## Storage location

- No config → data lives in `./data` (relative to the working dir). Mount a Docker
  volume there and it "just works."
- Set `[storage] data_dir` to any path (local disk, network/mounted volume).
- Set a keyspace's `tier = "file-per-key"` to store each value as its own file —
  portable and the integration point for third-party object stores (implement the
  `ObjectStore` trait).

## Binary protocol (`:6380`)

A length-prefixed TCP protocol with pipelining, for high throughput. Ops: `GET`,
`SET`, `DEL`, `PING`, `PUBLISH`, `SUBSCRIBE`, `PUSH`, `POP`, `ACK`, `AUTH`. Send
many requests back-to-back and read replies in order (no per-op round-trip). See
`crates/falcon-wire/src/protocol.rs` for the frame layout.

## Pub/Sub and queues

Configure topics/queues (see `config/default.toml`):

```toml
[[topic]]
name = "events"
mode = "durable"        # or "ephemeral"

[[queue]]
name = "jobs"
ack_timeout_secs = 30
```

- **Topics**: `PUBLISH` to a topic; `SUBSCRIBE` turns a wire connection into a live
  push stream. Durable topics also persist and can be replayed from an offset.
- **Queues**: `PUSH` a job; `POP` (per consumer group) delivers it and starts an ack
  timer; `ACK` confirms it; unacked jobs are redelivered after the timeout.
  Multiple consumers in a group share the work; different groups each get the full
  stream.

## Real-time subscriptions (WebSocket)

Off by default. Enable per keyspace (`subscriptions = true`), then connect to
`ws://localhost:8080/subscribe` and send:

```json
{"type":"subscribe","id":"sub1","keyspace":"default","key":"foo"}
```

(or `"prefix"` instead of `"key"`). You get `update` pushes on change.

## Replication

**Single-leader** (default, strong ordering): `[replication] enabled = true`,
`role = "leader"` on one node, `role = "follower"` + `leader_addr` on others; mark
each replicated keyspace `replication = true`.

**Multi-leader** (active-active): set a keyspace's `write_mode = "multi-leader"` and
list peers under `[replication].peers`. Any region accepts writes; they converge via
Hybrid Logical Clock last-write-wins. **This is eventual consistency** — concurrent
writes to the same key resolve deterministically (one wins, no merge). Use
single-leader if you need strong ordering.

## API key (optional auth)

Off by default. Set `[auth] api_key = "..."` (or `--api-key` / `FALCON_API_KEY`) and
**every connection must present it** — all client protocols *and* container-to-container
replication:

- **HTTP / REST**: `Authorization: Bearer <key>` header, or `?api_key=<key>` query param
  (`/healthz` is exempt for liveness probes)
- **WebSocket** (`/subscribe`): `?api_key=<key>` — browsers can't set handshake headers
- **Binary wire protocol**: an `AUTH` frame with the key, first, before any other op
  (all KV, pub/sub, and queue ops are gated)
- **gRPC replication** (node-to-node / container-to-container): `authorization` metadata

The key is compared in constant time. When unset, auth is fully off with zero overhead.

> **Security note:** the `?api_key=` query form is only as safe as the transport — put
> the deployment behind TLS so the URL isn't sniffable, and note URLs can appear in
> proxy/access logs. Prefer the `Authorization` header wherever the client can set it;
> the query param exists for browser WebSocket clients that can't.

## Operations, metrics & autoscaling

Falcon is built to run as a single autoscalable container. Everything here is
**on by default** with production-safe values, and every knob lives under
`[ops]` / `[storage]` in config (all overridable by `FALCON_*` env / flags).

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | **Liveness** — 200 while the process is up (restart signal). Unauthenticated. |
| `GET /readyz` | **Readiness** — 200 only once startup finished (503 otherwise). Route traffic on this; a catching-up follower stays *live* but *not ready*. Unauthenticated. |
| `GET /metrics` | **Prometheus** text metrics: op counts, latency histograms, hit-rate, WAL bytes, replication lag, connections. The signal HPA/KEDA scale on. Unauthenticated. |

- **Graceful shutdown** — on `SIGTERM` (k8s/docker stop) or Ctrl-C, Falcon stops
  accepting, drains in-flight requests, then force-flushes every buffered write
  (the sharded store's coalesce window, interval-fsync WAL) before exiting.
  Bounded by `[ops] shutdown_grace_secs`. **Zero-loss rollouts/autoscaling.**
- **WAL compaction** — a background task rewrites each warm-tier WAL as a live-key
  snapshot once it passes `[ops] compaction_min_bytes`, so disk usage and
  restart-replay time stay bounded no matter how long the container lives.
- **Anti-OOM** — `[storage] max_value_bytes` (default 64 MiB) rejects an oversized
  PUT with `413` before it can exhaust memory.

Example autoscale signals from `/metrics`: `rate(falcon_kv_put_total[1m])`
(throughput), `falcon_kv_put_latency_seconds` (tail latency),
`falcon_replication_lag_sequences` (follower lag).

## Docker

```bash
docker build -f docker/Dockerfile -t falcon .
docker run -p 8080:8080 -p 6380:6380 -v falcondata:/data falcon
docker compose -f docker/docker-compose.yml up   # 2-region cluster
```

## Safety & durability

- **Zero unsafe code**, compiler-enforced (`#![forbid(unsafe_code)]` everywhere).
- **Durable writes** via group-commit WAL (batched fsync); crash recovery tested.
  Per-keyspace `interval_fsync_ms` trades a bounded crash-loss window for lower write
  latency when you want it.
- `cargo audit` clean (no known vulnerabilities).

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo audit

# Sustained load test (tail latency under load)
cargo build --release -p falcon-cli -p falcon-bench
./target/release/falcon-bench --load-test --load-secs 15 --load-conns 64 \
  --load-write-ratio 0.5 --key-count 1000
```
