# Falcon

Falcon is a fast, safe data platform written in Rust. It bundles five data
products behind one binary, and you install only the one(s) you want:

- **Falcon Cache** — a low-latency RAM cache that spills to disk, with TTL.
- **Falcon KV Store** — a durable key-value store with real-time updates.
- **Falcon Pub/Sub** — publish/subscribe topics with live fan-out.
- **Falcon Queue** — durable work queues with competing consumers.
- **Falcon Event Stream** — partitioned, replayable event logs.

Every product runs over three protocols (a pipelined binary TCP protocol, REST,
and WebSockets), supports **multi-region replication**, can attach **third-party
object storage**, and can serve every hop over **TLS** — all configured through
the CLI or the web UI, never environment variables.

Every crate is `#![forbid(unsafe_code)]` (zero unsafe). The whole platform is
benchmarked end-to-end with `falcon-bench`, and every benchmark **asserts
correctness** (no loss, ordering, survives restart) so a number can never hide a
regression.

---

## Table of contents

- [Install what you want](#install-what-you-want)
- [Quickstart](#quickstart)
- [Using each product](#using-each-product)
- [Configuration (CLI / UI only)](#configuration-cli--ui-only)
- [Storage: local disk or third-party](#storage-local-disk-or-third-party)
- [Multi-region replication](#multi-region-replication)
- [Protocols & TLS](#protocols--tls)
- [Web console](#web-console)
- [Operations & metrics](#operations--metrics)
- [Benchmarks](#benchmarks)
- [Building & testing](#building--testing)
- [Architecture](#architecture)

---

## Install what you want

Falcon is **install-first**. You pick a product; you get its CLI verbs, its own
web UI, and its routes — and nothing else.

| Product | Install | What you get |
|---------|---------|--------------|
| **Falcon Cache** | `falcon install cache` | RAM cache that spills to disk, with TTL |
| **Falcon KV Store** | `falcon install kv` | durable key-value store with real-time updates |
| **Falcon Pub/Sub** | `falcon install pubsub` | publish/subscribe topics |
| **Falcon Queue** | `falcon install queue` | durable work queues, competing consumers |
| **Falcon Event Stream** | `falcon install stream` | partitioned, replayable event logs |

```console
$ falcon install cache --region us-east-1
Installed Falcon Cache at ~/.falcon/profile.toml
  low-latency RAM cache that spills to disk, with TTL

Next:
  falcon serve                       # run this node
  open http://127.0.0.1:8080/        # the Falcon Cache UI
```

A node runs **only the product(s) in its profile**. On a cache node the Cache UI
is served at `/`, the cache verbs work, and every other product's HTTP routes
return `404` — the surface is genuinely scoped.

**Two ways to scope a build:**

- **One binary, gated by profile** (the default `full` build): install a single
  product and the node scopes itself at runtime.
- **Per-product binary** (Cargo features): compile a slim binary that doesn't
  even *contain* the other products' code:
  ```console
  cargo build --release --no-default-features --features feat-cache
  ```
  A `feat-cache` binary refuses to install or run any other product.

### Dependency separation

A slim build pulls only the heavy dependencies its product needs — the
separation is real at the crate level, not just at runtime:

| Build | `sled` (on-disk tier) | remote-store client (reqwest + SigV4) |
|-------|:---:|:---:|
| `feat-pubsub` / `feat-queue` / `feat-stream` | — | — |
| `feat-cache` / `feat-kv` | ✓ (warm/tiered tiers) | — |
| `+ feat-remote` (or `full`) | ✓ | ✓ |

So a pub/sub-only binary compiles neither the on-disk storage engine nor the
remote-storage client.

---

## Quickstart

```bash
cargo build --release -p falcon-cli

# 1. Install a product (writes ~/.falcon/profile.toml)
falcon install kv

# 2. Run the node (dashboard at http://localhost:8080/)
falcon serve
```

```bash
# In another shell, use the same binary as a client:
falcon put foo bar
falcon get foo                   # → bar
falcon scan --prefix user:
falcon status                    # what's installed + current config
falcon health
```

```bash
# …or over plain HTTP:
curl -X PUT localhost:8080/kv/foo -d 'bar'
curl localhost:8080/kv/foo
curl -X PUT 'localhost:8080/kv/session?ttl=60' -d 'expires in 60s'
curl 'localhost:8080/kv?prefix=user:'
curl localhost:8080/health           # active products + feature set
```

> Swap `kv` for `cache`, `pubsub`, `queue`, or `stream` — each serves its own UI
> and only its own routes. See [Using each product](#using-each-product).

---

## Using each product

Every product is `install → serve → use it` (CLI, HTTP, or the UI at
`http://localhost:8080/`).

### 1. Falcon Cache — low-latency cache with TTL

RAM-first (tiered engine) with the cold tail spilled to disk, so it holds far
more than RAM while serving the hot set at RAM speed. Keys can expire via TTL.

```bash
falcon install cache && falcon serve
```
```bash
# CLI (cache keyspace is "cache")
falcon put session:42 '{"user":7}' --keyspace cache --ttl 300
falcon get session:42 --keyspace cache

# HTTP
curl -X PUT 'localhost:8080/keyspaces/cache/kv/session:42?ttl=300' -d 'value'
```
The UI shows hit-rate, hot keys/bytes, evictions, and TTL-tracked keys.

### 2. Falcon KV Store — durable KV with real-time updates

A durable store (warm tier: in-memory index + group-commit WAL) with live
WebSocket updates — subscribe to a key prefix and every write is pushed to you.

```bash
falcon install kv && falcon serve
```
```bash
falcon put user:7 'Alice'
falcon get user:7
falcon scan --prefix user:

# Real-time updates over WebSocket:
#   ws://localhost:8080/subscribe?keyspace=default&prefix=user:
```
The UI has read/write/scan plus a live subscription viewer.

### 3. Falcon Pub/Sub — topics (fan-out)

Publish to a topic; every live subscriber gets it (at-most-once for ephemeral
topics, persisted and replayable for durable ones).

```bash
falcon install pubsub && falcon serve
```
```bash
falcon topic publish events 'hello everyone'
curl -X POST localhost:8080/topics/events/publish -d 'hello'
# Subscribe:  ws://localhost:8080/subscribe?topic=events
```

### 4. Falcon Queue — durable work queues

Push jobs; competing consumers in a group each pop different jobs (at-least-once
with ack + redelivery-on-timeout). Work is distributed, not broadcast.

```bash
falcon install queue && falcon serve
```
```bash
falcon queue push jobs 'resize-image:42'
falcon queue pop jobs --group workers        # one consumer gets one job

curl -X POST localhost:8080/queues/jobs/push -d 'resize-image:42'
curl -X POST 'localhost:8080/queues/jobs/pop?group=workers'
```

### 5. Falcon Event Stream — partitioned, replayable logs

Kafka-shaped: records route to partitions by key (same key → same partition →
totally ordered). Consumer groups keep a durable per-partition offset and resume
where they left off; any group can replay history independently.

```bash
falcon install stream && falcon serve
```
```bash
falcon stream append clicks 'click:home' --key user:7    # key sets the partition
falcon stream poll clicks --partition 0 --group analytics

curl -X POST 'localhost:8080/streams/clicks/records?key=user:7' -d 'click:home'
curl 'localhost:8080/streams/clicks/poll?group=analytics&partition=0'
```

---

## Configuration (CLI / UI only)

Falcon **never reads environment variables**. All settings live in a single
profile file (`~/.falcon/profile.toml`), written only through:

- the CLI — `falcon config set <key> <value>` / `get` / `list`, and `falcon install`;
- the web UI — the config panel (`POST /config`, auth-gated) writes the same file.

```bash
falcon config set http-bind 0.0.0.0:9090
falcon config set api-key s3cret
falcon config list                       # every key + current value
falcon status                            # installed products + build + settings
```

`falcon serve` loads the profile; its flags (`--http-bind`, `--wire-bind`,
`--node-id`, `--region`, `--data-dir`, `--worker-threads`, `--log-level`)
override the profile **for one run**. Order: **profile < serve flags**.

### Config reference

| Key | Example | Controls |
|-----|---------|----------|
| `node.id` | `us-1` | Node identity (used in replication + logs). |
| `region` | `us-east-1` | Region label (display + HLC tiebreak). |
| `http-bind` | `0.0.0.0:8080` | REST / WebSocket / UI address. |
| `wire-bind` | `0.0.0.0:6380` | Binary protocol address (if `wire-enabled`). |
| `wire-enabled` | `true` | Turn the binary protocol on/off (off by default). |
| `api-key` | `s3cret` | Shared secret required on every connection. Empty = auth off. |
| `data-dir` | `/data` | Where durable data lives. |
| `log-level` | `info` | `error`/`warn`/`info`/`debug`/`trace`. |
| `write-mode` | `single-leader` / `multi-leader` / `primary-queue` | Multi-region write model. |
| `replicate` | `true` | Turn multi-region replication on. |
| `replication.role` | `leader` / `follower` | Replication role. |
| `grpc-bind` | `0.0.0.0:7070` | Replication gRPC address. |
| `leader-addr` | `http://10.0.0.1:7070` | Leader to follow (`role=follower`). |
| `peers` | `a:7070,b:7070` | Peer nodes (or `falcon peers add/remove`). |
| `tls-enabled` | `true` | In-process TLS on **all** hops. Off by default. |
| `tls-cert` / `tls-key` | `/path/*.pem` | PEM cert chain + private key. |
| `storage.backend` | `local` / `remote` | Local disk or a third-party object store. |
| `remote-url` / `remote-bucket` / `remote-access-key` / `remote-secret-key` / `remote-region` / `remote-prefix` | (yours) | Third-party store connection — **no defaults**. |

Product-internal tuning (storage tiers, partition counts, topic durability, ops
knobs) is documented in [`config/default.toml`](config/default.toml).

---

## Storage: local disk or third-party

There are exactly **two** storage kinds:

- **`local`** (default) — data lives on local disk at `data-dir`.
- **`remote`** — a third-party object store you fully specify. Falcon ships
  **no provider defaults** and hardcodes nothing; you supply the endpoint and
  everything needed to sign a request. Because the object HTTP API (as
  popularized by S3) is what these stores speak, one `remote` backend reaches
  any of them — managed or self-hosted — by URL + credentials.

```bash
falcon install cache \
  --storage remote \
  --remote-url https://your-endpoint \
  --remote-bucket my-bucket \
  --remote-access-key <key-id> \
  --remote-secret-key <secret> \
  --remote-region <region-or-omit>
falcon serve
```

If `storage.backend = remote` but a required field is missing, `falcon serve`
refuses to start with a clear error — remote storage is never half-configured.
Internally this uses the `sharded` object-store tier: keys hash into a fixed set
of bucket objects (N keys → a fixed object count), with an in-memory index for
O(1) point reads.

---

## Multi-region replication

Every product can replicate across regions over a dedicated **gRPC channel**:
each node binds `grpc-bind` (default `:7070`), the others dial it. There are
three write models — choose per keyspace with `write-mode`:

| Mode | Concurrent same-key writes | Local write latency | Use when |
|------|----------------------------|---------------------|----------|
| `single-leader` (default) | only the leader writes | leader-local | one writer region, strong order |
| `multi-leader` | converge, **one dropped** (HLC last-write-wins) | local everywhere (fast) | low latency, eventual consistency OK |
| `primary-queue` | **all kept**, total order | primary-local; others: one cross-region hop | you must not lose any write |

### Single-leader (default)

```bash
# Leader:
falcon install kv --node-id us --region us-east-1 --replicate --role leader
falcon serve
# Follower:
falcon install kv --node-id eu --region eu-west-1 --replicate --role follower \
      --leader-addr http://<leader-host>:7070
falcon serve
```

### Multi-leader (active-active)

Every node is a leader and lists the others as peers; writes converge via a
Hybrid Logical Clock. **Note:** multi-leader guarantees all regions converge to
the same value, but **not that the real-latest write survives** — of two
concurrent writes to a key, the higher-HLC one wins and the other is *dropped*.
Use `primary-queue` if that is unacceptable.

```bash
falcon install kv --node-id us --region us-east-1 --replicate --role leader --write-mode multi-leader
falcon peers add http://eu-host:7070
falcon serve
```

### Primary-queue (no lost writes)

Any region accepts writes, but they are **forwarded to one primary**, which
commits them in a **single ordered queue** and streams the committed log to
every region. One serialization point means concurrent writes are ordered, not
raced — **no write is dropped**.

```
   client ─put─▶ follower(eu) ──forward──▶ PRIMARY(us) ─commit (ordered)─┐
   follower(eu) ◀── committed change ◀── streamed to all regions ◀───────┘
```

```bash
# Primary (ordering authority):
falcon install kv --node-id us --region us-east-1 --replicate --role leader --write-mode primary-queue
falcon serve
# Other regions:
falcon install kv --node-id eu --region eu-west-1 --replicate --role follower \
      --leader-addr http://<primary-host>:7070 --write-mode primary-queue
falcon serve
```

Manage peers any time: `falcon peers add|remove|list`. **Open `7070/tcp`
between regions** — `8080`/`6380` are for clients, not node-to-node traffic.

---

## Protocols & TLS

Falcon uses the right protocol for each hop rather than one everywhere — each is
chosen to be **fast, safe, reliable, and durable**:

| Hop | Protocol | Why |
|-----|----------|-----|
| client ↔ service (KV hot path) | binary TCP, pipelined | lowest latency for small ops (µs-scale); one persistent stream |
| client ↔ service (REST / UI) | HTTP/1.1 + HTTP/2 | ubiquitous, browser + curl friendly |
| service → client (realtime) | WebSocket | server-push change feeds |
| service ↔ service (replication) | gRPC / HTTP/2 | streaming, flow-control, multiplexing for log shipping |

All hops keep **persistent connections**, so this optimizes the per-op path.

**TLS everywhere (optional, off by default).** Turn it on once and every hop —
HTTP, wire, and gRPC — listens encrypted:

```bash
falcon config set tls-enabled true
falcon config set tls-cert /path/cert.pem
falcon config set tls-key  /path/key.pem
falcon serve            # HTTPS, WSS, binary-over-TLS, gRPC-over-TLS
```

TLS is terminated **in process** with rustls (pure-Rust, AES-NI accelerated) —
not via an extra proxy hop. On persistent connections the handshake is a
one-time per-connection cost and per-record encryption adds only single-digit
microseconds, so the low-latency hot path is preserved. gRPC clients dial
`https://` peers using the platform's trusted roots; use `http://` for a
plaintext peer.

### API key (optional auth)

Set `falcon config set api-key "..."` and **every** connection must present it —
all client protocols *and* node-to-node replication:

- **HTTP/REST**: `Authorization: Bearer <key>` (or `?api_key=<key>`; `/healthz` exempt)
- **WebSocket**: `?api_key=<key>`
- **Binary wire**: an `AUTH` frame first, before any other op
- **gRPC replication**: `authorization` metadata

The key is compared in constant time; when unset, auth is fully off.

---

## Web console

Open **`http://localhost:8080/`**. Each product serves its **own** self-contained
UI (embedded in the binary, no build step, works offline): live stats, the
product's operations panel, and — where relevant — a live subscription viewer
and a config panel that writes the profile. If auth is on, the console prompts
for the API key and stores it locally.

---

## Operations & metrics

Falcon is built to run as a single autoscalable container. Everything here is on
by default with production-safe values, tuned via `falcon config set`.

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Liveness — 200 while the process is up. Unauthenticated. |
| `GET /readyz` | Readiness — 200 only once startup finished (503 otherwise). Route traffic on this; a catching-up follower stays *live* but *not ready*. |
| `GET /metrics` | Prometheus text metrics. Unauthenticated. |
| `GET /health` | JSON: active products, node/region, replication, keyspaces. |

- **Graceful shutdown** — on SIGTERM/Ctrl-C, Falcon stops accepting, drains
  in-flight requests, then force-flushes every buffered write before exiting
  (bounded by `ops.shutdown_grace_secs`) — zero-loss rollouts.
- **WAL compaction** — a background task rewrites each warm-tier WAL as a
  live-key snapshot once it passes `ops.compaction_min_bytes`, bounding disk and
  restart-replay time.
- **Anti-OOM** — `storage.max_value_bytes` (default 64 MiB) rejects an oversized
  PUT with `413`.

Metrics exposed include `falcon_kv_{get,put,delete}_total`, GET hit/miss,
per-op latency histograms, `falcon_wal_bytes`, `falcon_wal_fsync_total`,
`falcon_replication_lag_sequences`, `falcon_wire_connections`,
`falcon_ws_subscriptions`, and `falcon_ready`.

---

## Benchmarks

Run with the bundled load tester; every run **asserts correctness** (no loss,
ordering, survives a hard restart) so a number can't hide a regression.

```bash
cargo build --release -p falcon-cli -p falcon-bench

falcon-bench --skip-writes --pipeline-depths 1,16,128   # read path
falcon-bench --load-test --load-secs 8 --load-conns 64  # sustained load
falcon-bench --bench-all                                # every product
```

**Measured on an Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` +
LTO.** These are real numbers from this repo — reproduce them with the commands
above. Fsync-bound write paths go faster on a Linux server with
power-loss-protected NVMe. Throughput figures are **concurrent** (many
connections/writers at once), so they reflect real capacity, not single-thread.

### Read path — served from the in-memory index, over the pipelined wire protocol

| Path | Throughput | p50 | p99 |
|------|-----------:|----:|----:|
| Wire GET, pipeline depth=128 | **9.29 M ops/sec** | 100 µs¹ | 191 µs¹ |
| Wire GET, pipeline depth=16 | **2.30 M ops/sec** | 53 µs¹ | 99 µs¹ |
| Wire GET, depth=1 (no pipeline) | 184 K ops/sec | 42 µs | 80 µs |
| Sustained read load (64 conns, depth=16) | **3.01 M ops/sec** | 329 µs¹ | 622 µs¹ |
| HTTP GET (JSON, 1 req/op) | 80 K ops/sec | 86 µs | 238 µs |

¹ *In the pipelined rows, latency percentiles are **per batch** (batch = `depth`
ops); throughput is aggregate.* The sustained read test reported **STABLE (no
latency cliff / queue buildup)**.

### Write path — a durability dial you control

Every write goes through the group-commit WAL. `fsync`-every-write (default) is
bound by disk fsync latency; `interval_fsync_ms` trades a small bounded loss
window for far higher throughput.

| Write mode | Throughput | p50 | p99 |
|------------|-----------:|----:|----:|
| `fsync` every write (max durability) | ~1 K ops/sec | 7 ms | 12 ms |
| `interval_fsync_ms = 10` (≤10 ms loss window, 64 conns) | **406 K ops/sec** | 1 ms | 5 ms |

The interval-fsync write test reported **STABLE**.

### Every product (`falcon-bench --bench-all`, 5000 records each)

Each row also passed its correctness checks.

| Product | Concurrent peak | Per-op latency (sequential, durable) | Correctness verified |
|---------|----------------:|--------------------------------------|----------------------|
| **Falcon KV Store** | ~2,450 ops/sec | p50 3.4 ms · p99 4.1 ms | 5000/5000 survived a hard restart |
| **Falcon Pub/Sub** | **~4,900 ops/sec** | — | ordered; persisted across restart |
| **Falcon Queue** | ~4,270 ops/sec | p50 357 µs · p99 575 µs | 5000/5000 delivered; acked jobs not redelivered |
| **Falcon Event Stream** | ~4,260 ops/sec | — | per-key ordered; resumes at committed offset |
| **Falcon KV Store (real-time)** | ~2,260 ops/sec | p50 4.0 ms · p99 4.2 ms | 640/640 writes notified (32 subs), no drops/dupes |
| **Multi-region** (leader→follower, 16 writers) | ~880 ops/sec | — | 496/496 converged, none lost; batch converged in ~1.6 s |

*Multi-region throughput reflects cross-region convergence over async gRPC, not
per-write latency; both nodes persist independently.*

---

## Building & testing

```bash
cargo build --release                 # full build (every product + backend)
cargo test                            # 121 tests across the workspace
cargo clippy --workspace --all-targets

# Slim per-product build (omits other products' code + unused heavy deps):
cargo build --release --no-default-features --features feat-cache
# Add third-party storage to any build:  --features feat-remote
```

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the crate map, the storage engines
(hot / warm / cold / tiered / sharded), the group-commit WAL, the replication
model, and the request lifecycle.

## License

MIT — see [LICENSE](LICENSE).
