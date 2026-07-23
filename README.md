# Falcon

A fast, safe, fully-configurable data platform in Rust. Falcon bundles five
components behind one binary:

- **FalconDB** — the key-value store (six pluggable storage tiers)
- **Falcon Queue** — durable, at-least-once work queues
- **Falcon Pub/Sub** — topics (ephemeral or durable)
- **Falcon Event Streaming** — partitioned, replayable event logs with durable
  consumer groups (Kafka-shaped)
- **Falcon Realtime DB** — live WebSocket subscriptions (write→notify)

…plus TTL, multi-region replication (single- and multi-leader), pluggable
storage, Prometheus metrics, WAL compaction, and graceful shutdown — over HTTP,
a lean binary protocol, and WebSockets. Everything optional is **off by default
and costs nothing when unused**.

**Every subsystem is benchmarked for *fast · reliable · safe · durable*** with
`falcon-bench --bench-all` (see [Benchmarks](#benchmarks)) — each run asserts
correctness (no loss, ordering, survives restart) so a regression can't hide
behind a good number.

## What it does

- **FalconDB** (key-value) with per-keyspace storage tiers:
  - `hot` — in-memory only (fastest, not durable)
  - `warm` — in-memory index + group-commit WAL (durable, **default**)
  - `cold` — disk-backed via sled (for datasets larger than RAM)
  - `tiered` — automatic hot/cold: hot working set in RAM, cold tail auto-spilled
    to disk (CLOCK eviction), so you can hold far more than RAM and pay RAM only
    for the working set
  - `file-per-key` — each value is an independent file/object (portable,
    inspectable; the seam for third-party object stores). **Portability tier,
    not a hot tier** — one I/O (or one billed request on a remote bucket) *per
    key*, no in-memory index. For hot/cost-sensitive object storage use
    `sharded` instead.
  - `sharded` — **the optimized object-store engine.** Keys are **hashed into a
    fixed set of buckets**, each bucket stored as one object in the backing
    store, so *N keys cost a fixed object count* no matter how large N gets.
    O(1) point reads from an in-memory index; writes coalesce per bucket. This
    is the cost-efficient, fast way to sit on a request-billed object store —
    use it instead of `file-per-key` for anything under load.
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
  `ObjectStore` trait). This is a portability seam, **not** for hot data.
- Set `tier = "sharded"` (recommended for object storage under load): the same
  `ObjectStore` seam, but keys hash into a fixed number of buckets so N keys cost
  a fixed object count, with O(1) in-memory reads. Tune `shard_buckets` /
  `shard_flush_ms`.

## Binary protocol (`:6380`)

A length-prefixed TCP protocol with pipelining, for high throughput. Ops: `GET`,
`SET`, `DEL`, `PING`, `PUBLISH`, `SUBSCRIBE`, `PUSH`, `POP`, `ACK`,
`STREAM_APPEND`, `AUTH`. Send many requests back-to-back and read replies in
order (no per-op round-trip). See `crates/falcon-wire/src/protocol.rs` for the
frame layout.

## Concurrency

Every subsystem serves requests with **true concurrency**, verified by the
`--bench-all` suite (all numbers there are multi-connection):

- **Connections run in parallel.** Each binary-protocol connection is its own
  Tokio task; the HTTP/WebSocket server (axum) handles every request
  concurrently. Thousands of clients make progress at once — nothing funnels
  through a global lock.
- **Storage is sharded, not globally locked.** Reads hit a concurrent `DashMap`
  (or sled) with no lock; writes take only a *per-key* lock (a 1024-way sharded
  lock table), so writes to different keys proceed fully in parallel.
- **Durability batches instead of serializing.** The KV WAL and the messaging
  log both use **group commit** — a background writer coalesces many concurrent
  writes into one fsync — so durable throughput *scales with* concurrency
  instead of pinning at one-fsync-at-a-time. Messaging fsyncs run on the
  blocking pool so a slow disk never stalls the async workers.
- **Ordering is preserved under concurrency.** Sequence assignment and the WAL
  append are atomic, so the replication log a follower streams is strictly
  ordered even when unrelated keys are written in parallel (regression-tested).
- **Streams parallelize by partition.** Different partitions are independent
  ordering domains written in parallel; a single key stays totally ordered.

The one deliberate exception: ops **pipelined on a single connection** are
dispatched in arrival order (Redis-like per-connection ordering). Concurrency
comes from using multiple connections — which real clients and the benchmark do.

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

## Event streaming (Falcon Event Streaming)

A **stream** is the Kafka-shaped sibling of a topic: **partitioned**, durable,
replayable, with **per-consumer-group committed offsets**. Records route to a
partition by key hash (same key ⇒ same partition ⇒ **ordered**); each partition
is its own durable log; every consumer group keeps a durable offset *per
partition* and resumes there after a restart (**at-least-once**). Different
groups each see the full stream independently.

Configure streams (see `config/default.toml`):

```toml
[[stream]]
name = "user-events"
partitions = 8          # ordering domains / parallelism; a key is ordered within its partition
capacity = 1024         # live-tail broadcast buffer per partition
```

Drive it over **REST** — append (producer), poll + commit (consumer group):

```bash
# Append a record; ?key= picks the partition. Returns {partition, offset}.
curl -X POST 'localhost:8080/streams/user-events/records?key=user:42' -d 'signed_up'

# Poll a partition for a consumer group (records AFTER its committed offset).
curl 'localhost:8080/streams/user-events/poll?group=analytics&partition=3'

# After processing, durably commit progress (this is the at-least-once boundary).
curl -X POST 'localhost:8080/streams/user-events/commit?group=analytics&partition=3&offset=57'

# Stream metadata (partition count).
curl localhost:8080/streams/user-events
```

The high-throughput **producer path is also on the binary wire protocol**
(`STREAM_APPEND`, op `0x20`): keyspace = stream name, key = partition key,
value = payload; the reply carries `partition(4B) + offset(8B)`. Consumer
poll/commit are request/response and use REST.

**Topic vs. queue vs. stream:** topic = simple fan-out; queue = work
distribution with acks; **stream = ordered-by-key, replayable, partitioned
history with independent consumer groups.**

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

## Benchmarks

Measured with the bundled `falcon-bench` load tester (`--release`, LTO) on a
development Mac (Apple Silicon, APFS). **These are real numbers from this repo —
reproduce them with the commands below.** A Linux server with
power-loss-protected NVMe does materially better on the write path (fsync is the
bottleneck there).

**Read path — Redis-class.** Reads are served from the in-memory index; the
binary wire protocol pipelines many ops per round-trip.

| Path | Throughput | p50 | p99 | p99.9 |
|------|-----------:|----:|----:|------:|
| Wire GET, pipeline d=128 | **5.6 M ops/sec** | 152 µs | 341 µs | — |
| Wire GET, pipeline d=16 | 1.85 M ops/sec | 63 µs | 140 µs | — |
| Wire GET, d=1 (no pipeline) | 176 K ops/sec | 42 µs | 104 µs | — |
| Sustained read load (64 conns) | **3.0 M ops/sec** | 328 µs | 615 µs | 723 µs |
| HTTP GET (JSON, 1 req/op) | 61 K ops/sec | 79 µs | 197 µs | — |

**Write path — a durability dial you control.** Every write goes through the
group-commit WAL. With `fsync`-every-write (the default, *zero acked-write
loss*) throughput is bound by disk fsync latency; `interval_fsync_ms` trades a
small bounded loss window for a ~400× throughput jump.

| Write mode | Throughput | p50 | p99 |
|------------|-----------:|----:|----:|
| `fsync` every write (max durability) | ~1 K ops/sec | 7 ms | 11 ms |
| `interval_fsync_ms = 10` (≤10 ms loss window) | **397 K ops/sec** | 1 ms | 5 ms |

**Stability:** every sustained load test reported `STABLE (no latency cliff /
queue buildup)` — throughput held flat under 64-connection saturation with no
tail runaway.

### Every subsystem (`--bench-all`)

`falcon-bench --bench-all` spawns real servers and benchmarks **every**
component, and each run **asserts correctness on four axes — a failed check
aborts the run**, so these numbers can't hide a regression:

- **FAST** — throughput / latency
- **RELIABLE** — stable under load; a slow consumer never loses or reorders
- **SAFE** — no errors; every accepted op accounted for
- **DURABLE** — data survives a hard process restart (verified by killing the
  server and re-reading)

Durable-write throughput, concurrent + pipelined (same dev Mac):

All numbers are **concurrent** (many connections/writers at once), not
single-threaded, so they reflect real capacity:

| Subsystem | Concurrent peak | Correctness verified |
|-----------|----------------:|----------------------|
| **FalconDB** (KV, durable) | ~2,200 ops/sec | 4000/4000 survived a hard restart |
| **Falcon Pub/Sub** (durable topic) | **~4,600 ops/sec** | ordered, persisted across restart |
| **Falcon Queue** (at-least-once) | **~4,250 ops/sec** | all delivered, acked jobs not redelivered |
| **Falcon Event Streaming** (1 partition) | **~4,300 ops/sec** | per-key ordered, durable commit/resume |
| **Falcon Realtime DB** (32 concurrent subs) | **~2,270 ops/sec** | every write notified its subscriber |
| **Multi-region** (16 concurrent writers) | ~940 ops/sec¹ | 400/400 converged, none lost |

¹ *Multi-region also reports **cross-region convergence time** — how long the
whole concurrent batch takes to appear on the follower (~1.0–1.3 s for 400
writes over async gRPC replication). Replication is async by design; the number
is convergence, not per-write latency.*

> **A real bug this caught.** Writing the concurrent multi-region benchmark
> surfaced a genuine data-loss bug: under a burst of concurrent writes to
> *different* keys, sequence allocation and the WAL append weren't atomic, so
> the on-disk log order didn't match sequence order — which stranded a
> follower's log catch-up and silently dropped writes. Fixed by making
> sequence-assign + WAL-enqueue atomic (file order == sequence order) and adding
> a safety re-read poll to the leader's replication stream. Regression-tested in
> `crates/falcon-storage/tests/replog_ordering.rs`.

Two engine-level fixes got messaging here (durable pub/sub, queue, and stream
appends were previously fsync-bound at **~270 ops/sec — a ~16× improvement**):

1. **Group commit for the message log** — a background writer drains all queued
   appends and does one fsync per batch (the same design as the KV WAL).
2. **Non-blocking dispatch** — messaging fsyncs run on the blocking pool so a
   slow disk never stalls the async workers driving other connections.

**Stream partitions are an ordering-parallelism-vs-throughput dial:** on a
single disk each partition fsyncs independently, so fewer partitions = higher
single-node throughput (1 partition ≈ 4,400/sec; 8 partitions ≈ 2,300/sec) —
the same property Kafka has. Default is 1; raise it for parallel ordering
domains, and use a stream's `interval_fsync_ms` to trade a bounded loss window
for throughput at higher partition counts.

Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
# Every subsystem, with fast/reliable/safe/durable checks:
./target/release/falcon-bench --bench-all
# KV throughput + latency percentiles (HTTP baseline + pipelined wire):
./target/release/falcon-bench --pipeline-depths 1,16,128
# Sustained read load:
./target/release/falcon-bench --load-test --load-secs 15 --load-conns 64 --load-write-ratio 0.0
# Write path with interval fsync:
./target/release/falcon-bench --load-test --load-secs 12 --load-write-ratio 1.0 --load-interval-fsync-ms 10
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
