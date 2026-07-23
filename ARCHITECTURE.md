# Falcon Architecture

Falcon is a single Rust binary that provides five installable data products —
Cache, KV Store, Pub/Sub, Queue, and Event Stream — over three protocols
(binary TCP, REST, WebSocket), with multi-region replication, pluggable
third-party storage, and optional TLS on every hop. Everything optional is off
by default and adds no overhead when unused, and every crate is
`#![forbid(unsafe_code)]`.

This document explains how the pieces fit, the data structures and algorithms on
the hot paths, and the design of the replication and storage layers.

---

## 1. System overview

Every client protocol is a thin front-end over one shared `Node`. They all call
the same `Keyspace` / `Messaging` methods, so durability, ordering, replication,
and metrics are identical no matter how a request arrives.

```
                 ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   clients  ───▶ │ REST / HTTP  │  │ Binary wire  │  │  WebSocket   │
                 │   :8080      │  │   :6380      │  │  /subscribe  │
                 └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                        │   auth · TLS · body-limit · metrics│
                        └───────────────┬───────────────────┘
                                        ▼
                              ┌────────────────────┐
                              │        Node        │  composition root
                              │  (Arc, shared)     │  + Metrics registry
                              └─────────┬──────────┘
             ┌───────────────┬──────────┼───────────┬─────────────────┐
             ▼               ▼          ▼           ▼                 ▼
        ┌─────────┐   ┌────────────┐ ┌──────┐ ┌───────────┐   ┌──────────────┐
        │Keyspace │   │ Messaging  │ │ TTL  │ │ Ops tasks │   │ Replication  │
        │ (KV)    │   │ topics/    │ │reaper│ │ compactor │   │ gRPC :7070   │
        │         │   │ queues/    │ │      │ │ shutdown  │   │ leader/multi │
        │ engine  │   │ streams    │ └──────┘ └───────────┘   │ /primary-q   │
        └────┬────┘   └─────┬──────┘                          └──────┬───────┘
             ▼              ▼                              ships ordered log
     ┌───────────────┐  durable append logs                    to followers
     │ StorageEngine │  (topic/queue/stream files)
     │ hot·warm·cold │  ◀── every KV write becomes one ChangeEvent, observed
     │ tiered·sharded│      identically by subscribers AND replication
     └───────────────┘
```

The five **products** are a view over these subsystems. A profile's installed
feature set decides which keyspaces/topics/queues/streams a node builds, which
HTTP routes mount, and which embedded UI is served.

---

## 2. Crate map

```
falcon-cli          the `falcon` binary: subcommands (install/config/peers,
│                   serve, client verbs), profile → Config, server wiring,
│                   multi-core runtime, replication startup
├── falcon-api      REST (axum) + WebSocket servers, per-product embedded UIs,
│                   route gating, /config write path, optional HTTP TLS
├── falcon-wire     binary pipelined TCP protocol (:6380), optional TLS
├── falcon-core     Node/Keyspace, Config, Profile, Feature model, HLC write
│   │               paths, WriteForwarder, shared TLS loader
│   ├── falcon-storage    engines: hot/warm/cold/tiered/sharded + remote store
│   ├── falcon-messaging  topics, queues, streams (durable append logs)
│   └── falcon-events     ChangeEvent, EventBus (broadcast), HLC clock
├── falcon-replication    gRPC leader/follower, multi-leader, primary-queue
├── falcon-metrics  zero-dep counters/gauges/histograms + Prometheus encoder
├── falcon-proto    generated gRPC types (tonic/prost)
└── falcon-bench    end-to-end load tester with correctness assertions
```

Dependency direction is downward: servers depend on `falcon-core`, which depends
on `falcon-storage` / `falcon-messaging` / `falcon-events`. `falcon-replication`
depends on `falcon-core` for the `WriteForwarder` trait. No cycles.

Heavy dependencies are behind Cargo features so slim builds stay lean:
`cold` gates `sled` (cold + tiered tiers); `remote` gates the third-party
object-store client (reqwest + a SigV4 signer). The `full` build enables both.

---

## 3. The product / feature model

`Feature` (in `falcon-core/src/feature.rs`) is the single source of truth for
the five products. It has two lives:

1. **Compile time** — each product is a Cargo feature (`feat-cache`, …) on the
   `falcon` binary; a slim build compiles exactly one and its dependencies.
2. **Runtime** — the installed **profile** (`~/.falcon/profile.toml`) records
   which product(s) a node runs. `Profile::to_config()` turns the profile into
   the engine `Config` — each feature contributes its slice (a cache feature →
   a tiered `cache` keyspace; pubsub → a topic; etc.).

Configuration is **CLI/UI only** — Falcon reads no environment variables. The
CLI (`install` / `config` / `peers`) and the UI's `POST /config` both write the
same profile file; `serve` flags override it for a single run.

---

## 4. Request lifecycle (KV write)

```
client ──REST/wire──▶ Server ──▶ Keyspace.put(k, v)
                                     │
                    ┌────────────────┼─────────────────────┐
                    ▼                ▼                     ▼
             StorageEngine     TTL expiry map        EventBus.publish
             (returns seq)     (DashMap)             (ChangeEvent)
                                                      │
                                        ┌─────────────┴─────────────┐
                                        ▼                           ▼
                                WebSocket subscribers      Replication log shipper
```

Every write flows through `Keyspace` (`falcon-core/src/keyspace.rs`) — the one
place a write becomes a `ChangeEvent`. That guarantees WebSocket subscribers,
TTL expiries, and replication observe *exactly the same ordered stream*. The
`EventBus` is a `tokio::broadcast` channel constructed only when subscriptions
or replication are enabled for the keyspace, so the default path allocates
nothing extra.

---

## 5. Storage engines

All engines implement one trait, `StorageEngine` (`falcon-storage/src/engine.rs`):
`get / put / delete / scan_prefix / apply_replicated`, with each `put`/`delete`
returning a monotonically increasing **sequence** the keyspace stamps onto the
event. This uniform contract lets a keyspace swap engines by config.

| Tier | Backing | Durability | Best for | Key data structure |
|------|---------|-----------|----------|--------------------|
| `hot` | `DashMap` in RAM | none | ephemeral cache, sessions | sharded hash map |
| `warm` (default) | RAM index + group-commit WAL | fsync'd | general-purpose durable | append log + sparse index |
| `cold` | sled | fsync'd | datasets larger than RAM | B-tree (sled) |
| `tiered` | RAM working set + `cold` tail | fsync'd | far-larger-than-RAM with a hot set | CLOCK eviction |
| `sharded` | N bucket objects (local dir or remote store) | sync or coalesced | object storage, local or third-party | hash bucketing + in-mem index |

### 5.1 Shared concurrency primitives

- **`KeyLockTable`** (`lock_table.rs`): a fixed array of 1024 mutexes. A key
  hashes to a shard, so writes to *different* keys almost never contend, while
  repeated writes to the *same* key serialize in arrival order. Reads take no
  lock. This is per-key ordering without a per-key allocation.
- **Group-commit WAL** (`wal.rs` / `wal_writer.rs`): a background task owns the
  WAL file exclusively and batches concurrently-submitted writes into a single
  fsync. Under light load a batch is one request (same latency as fsync-per-
  write); under concurrent load, writes arriving during an in-flight fsync pile
  up and flush together, so throughput scales with concurrency instead of
  staying flat at `1 / fsync_latency`. `FsyncPolicy::IntervalMs` trades a
  bounded crash-loss window for still-higher throughput.

### 5.2 Warm tier — ordering correctness

The warm engine allocates a sequence and enqueues the WAL append under a small
`seq_order` mutex so the **durable log order always matches sequence order**.
Without this, two writes to different keys could allocate seq N and N+1 but
enqueue out of order, leaving the replication log unordered — which would strand
a follower's sparse-index catch-up. The mutex is held only across a non-blocking
channel send (never across the fsync await), so group commit still batches fully.

### 5.3 Sharded tier — bucket-per-hash

Keys are hashed (FNV-1a, stable across processes/platforms) into a fixed number
of **buckets**; each bucket is one object in the backing `ObjectStore`. So a
keyspace of millions of keys maps to `N` objects, not millions — keeping the
number of object-store requests bounded and behaving identically on local disk
and a remote bucket. An in-memory index (one `HashMap` per bucket behind an
`RwLock`) serves reads in O(1) once a bucket is resident; writes re-serialize the
touched bucket (per-write with `FlushPolicy::Sync`, or coalesced on an interval
with `FlushPolicy::Coalesce`).

### 5.4 Pluggable third-party storage

The `sharded` tier is backend-agnostic — it addresses bucket objects through the
`ObjectStore` trait (`object_store.rs`). Two implementations ship: `LocalDirStore`
(one file per bucket under `data_dir`) and `RemoteObjectStore` (`remote_store.rs`),
a minimal AWS-Signature-V4 signer over `reqwest` behind the `remote` feature. The
operator supplies the endpoint + credentials (no defaults); the same client
reaches any object store speaking the S3-style HTTP API.

---

## 6. Messaging (Pub/Sub, Queue, Event Stream)

All three reuse the same durable append-log pattern (`messaging/src/log.rs`) and
are owned by `Messaging`, built once at startup.

- **Topics** (`topic.rs`) — `ephemeral` (fast, in-memory, at-most-once) or
  `durable` (persisted, replayable). A subscriber becomes a live push stream.
- **Queues** (`queue.rs`) — a durable log plus, per consumer group, a cursor and
  an in-flight set. `pop` hands out the next undelivered message with a
  redelivery deadline; `ack` removes it; expired in-flight messages redeliver
  (at-least-once). Competing consumers in one group share the stream; different
  groups each see it in full.
- **Streams** (`stream.rs`) — partitioned, offset-addressed, replayable logs.
  Records route to partitions by key hash (per-key total order within a
  partition); consumer groups keep durable per-partition committed offsets and
  resume after restart. On a single disk each partition fsyncs independently, so
  more partitions trade single-node write throughput for parallel ordering (as
  in Kafka); `interval_fsync_ms` coalesces fsyncs to reclaim throughput.

---

## 7. Multi-region replication

Replication ships each keyspace's ordered change log to other regions over gRPC
(`:7070`). A follower resumes from `engine.last_applied_sequence()` (durable on
the follower), so it never loses data across reconnects. There are three write
models (`WriteMode`):

### 7.1 Single-leader (default)

One region (`role=leader`) accepts writes and streams its log; followers replay
it. Strong ordering. A follower stays *live* but *not ready* until caught up.

### 7.2 Multi-leader (active-active)

Every node is a leader and lists the others as `peers`. A local write is stamped
with a **Hybrid Logical Clock** (`events/src/hlc.rs`) and applied via the warm
engine's last-write-wins path; replicated events converge via LWW, re-broadcast
only if they win. All regions converge deterministically — but of two concurrent
same-key writes, the higher-HLC one wins and the other is **dropped** (HLC is not
a perfect global clock, so "wins" ≠ "was truly last"). Use primary-queue if no
write may be lost.

### 7.3 Primary-queue (no lost writes)

One region is the **primary**. A non-primary node installs a `WriteForwarder`
(`falcon-core`) whose `put`/`delete` **forward the write to the primary** over a
`ForwardWrite` gRPC instead of writing locally. The primary commits every write
(its own + forwarded) through its single ordered write path — a total order, no
LWW, no dropped writes — then streams the committed log to every region, which is
what actually mutates each region's local storage. The forwarded write's ack
returns the sequence the primary durably committed.

### 7.4 Catch-up

A new or far-behind follower can pull a full snapshot (`GetSnapshot`) instead of
replaying the entire log entry by entry; live tailing uses the event bus as a
low-latency wake hint plus a short safety poll, so correctness never depends on a
wake being delivered.

---

## 8. Protocols

- **Binary wire** (`falcon-wire`) — a length-delimited TCP protocol built for
  pipelining: every field is length-prefixed, so a client can send many requests
  back-to-back over one persistent connection and the server replies in order
  (no request IDs, Redis-RESP-style). Ops: `GET/SET/DEL/PING/AUTH`,
  `PUBLISH/SUBSCRIBE`, `PUSH/POP/ACK`, `STREAM_APPEND`. This is the low-latency
  hot path. `TCP_NODELAY` is set so pipelined replies flush immediately.
- **REST/JSON** (`falcon-api`, axum) — the ubiquitous path for the KV, messaging,
  stream, health, metrics, and config endpoints; also serves the embedded UI.
- **WebSocket** (`/subscribe`) — server-push change feeds for realtime KV and
  Pub/Sub.
- **gRPC/HTTP2** (`falcon-replication`, tonic) — node-to-node replication:
  streaming, flow-controlled, multiplexed.

**TLS** is optional and shared: one rustls loader (`falcon-core/src/tls.rs`)
feeds all three server hops (HTTP via a tokio-rustls accept loop into hyper,
wire via a tokio-rustls acceptor around the generic connection handler, gRPC via
tonic's `ServerTlsConfig`). rustls uses AES-NI; on Falcon's persistent
connections the handshake is a one-time per-connection cost, so per-op latency
stays microsecond-scale.

---

## 9. Observability & operations

`falcon-metrics` is a zero-dependency core: lock-free atomic counters/gauges and
a fixed-bucket latency histogram, rendered to Prometheus text. Incrementing a
counter is one relaxed atomic add — instrumenting the request path is effectively
free, and the only cost when nobody scrapes is those adds.

Operational tasks run in the background: a **TTL reaper** sweeps expired keys; a
**WAL compactor** rewrites a warm-tier WAL as a live-key snapshot past a size
threshold (bounding disk + restart-replay); **graceful shutdown** on SIGTERM
stops accepting, drains in-flight requests, and force-flushes buffered writes
before exit. Readiness (`/readyz`) is distinct from liveness (`/healthz`) so an
orchestrator routes traffic only to caught-up nodes while keeping catching-up
followers alive.

---

## 10. Safety & testing

- `#![forbid(unsafe_code)]` on every crate — zero `unsafe`.
- 121 workspace tests: storage engines (WAL recovery, group commit, compaction,
  sparse index, LWW, sharded object count), messaging, wire pipelining/auth,
  API auth + feature gating, config/profile round-trips, HLC ordering, and the
  pluggable-backend seam.
- `falcon-bench --bench-all` spawns real servers and benchmarks every product,
  **asserting correctness on four axes** (fast / reliable / safe / durable) — a
  failed check aborts the run, so a throughput number can never mask a
  regression. See [Benchmarks](README.md#benchmarks) for measured results.
