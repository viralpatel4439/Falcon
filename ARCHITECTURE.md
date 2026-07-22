# Falcon Architecture

Falcon is a single Rust binary that bundles a key-value store, pub/sub topics,
durable work queues, **partitioned event streams**, real-time WebSocket
subscriptions, TTL, and multi-region replication — over REST, a binary wire
protocol, and WebSockets. Everything optional is **off by default and costs
nothing when unused**, and every crate is `#![forbid(unsafe_code)]`.

This document explains how the pieces fit, the data structures and algorithms
that make the hot paths fast, and the design of the two subsystems added most
recently: the **sharded storage engine** and **Falcon Event Streaming**.

## System overview

Every client protocol is a thin front-end over one shared `Node`. They call the
same `Keyspace` / `Messaging` methods, so durability, ordering, replication, and
metrics are identical no matter how a request arrives.

```
                 ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   clients  ───▶ │ REST / HTTP  │  │ Binary wire  │  │  WebSocket   │
                 │   :8080      │  │   :6380      │  │  /subscribe  │
                 └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                        │   auth · body-limit · metrics     │
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
        │ engine  │   │ STREAMS    │ └──────┘ └───────────┘   └──────┬───────┘
        └────┬────┘   └─────┬──────┘                                 │
             ▼              ▼                              ships ordered log
     ┌───────────────┐  durable append logs                    to followers
     │ StorageEngine │  (topic/queue/stream/offset files)
     │ hot·warm·cold │
     │ tiered·sharded│  ◀── every KV write becomes one ChangeEvent, observed
     │ file-per-key  │      identically by subscribers AND replication
     └───────────────┘
```

The rest of this document goes tier by tier and subsystem by subsystem.

---

## 1. Crate map

```
falcon-cli          binary entrypoint: config load, wiring, server startup
├── falcon-api      REST (axum) + WebSocket subscription servers
├── falcon-wire     binary pipelined TCP protocol (:6380)
├── falcon-core     Node/Keyspace: ties engines to events, TTL, write modes
│   ├── falcon-storage    storage engines (hot/warm/cold/tiered/file-per-key/SHARDED)
│   ├── falcon-messaging  topics, queues, and STREAMS (event streaming)
│   └── falcon-events     ChangeEvent, EventBus (broadcast), HLC clock
├── falcon-replication    gRPC leader/follower + multi-leader log shipping
├── falcon-metrics  zero-dep counters/gauges/histograms + Prometheus encoder
└── falcon-proto    generated gRPC types (tonic/prost)
```

Dependency direction is strictly downward: servers depend on `falcon-core`,
which depends on `falcon-storage` / `falcon-messaging` / `falcon-events`. No
cycles; the storage and messaging layers never call back up.

---

## 2. Request lifecycle (KV write)

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

Every write flows through `Keyspace` (`falcon-core/src/keyspace.rs`) — the
single place a write becomes a `ChangeEvent`. That guarantees WebSocket
subscribers, TTL expiries, and replication all observe *exactly the same
ordered stream*. The `EventBus` is a `tokio::broadcast` channel and is only
constructed when subscriptions or replication are enabled for the keyspace, so
the default path allocates nothing extra.

---

## 3. Storage engines

All engines implement one trait, `StorageEngine`
(`falcon-storage/src/engine.rs`): `get / put / delete / scan_prefix /
apply_replicated`, each `put`/`delete` returning a monotonically increasing
**sequence** the keyspace stamps onto the event. This uniform contract lets a
keyspace swap engines by config with no change upstream.

| Tier | Backing | Durability | Best for | Key DSA |
|------|---------|-----------|----------|---------|
| `hot` | `DashMap` in RAM | none | ephemeral cache, sessions | sharded hash map |
| `warm` (default) | RAM index + group-commit WAL | fsync'd | general purpose, low-latency durable | append log + sparse index |
| `cold` | sled | fsync'd | datasets larger than RAM | B-tree (sled) |
| `tiered` | RAM working set + disk tail | fsync'd | far-larger-than-RAM with hot set | CLOCK eviction |
| `file-per-key` | one object per key | atomic write-rename | portable/inspectable, object-store seam | — |
| **`sharded`** | **N bucket objects** | sync or coalesced | **cheap 3rd-party object storage** | **hash bucketing + in-mem index** |

### 3.1 Concurrency primitives shared across engines

- **`KeyLockTable`** (`lock_table.rs`): a fixed array of 1024 mutexes. A key
  hashes to a shard, so writes to *different* keys almost never contend, while
  repeated writes to the *same* key serialize in arrival order. Reads take no
  lock (DashMap/sled are already concurrent). This is per-key ordering without
  a per-key allocation.
- **Group-commit WAL** (`wal.rs` / `wal_writer.rs`): concurrent writers append
  to an in-memory batch; one flusher fsyncs the batch and wakes all waiters, so
  fsync cost amortizes across a burst. `FsyncPolicy::IntervalMs` trades a
  bounded crash-loss window for throughput.

---

## 4. Sharded storage engine (bucket-per-hash)

**Problem it solves.** `file-per-key` stores one object per key. On a
third-party, request-billed object store (S3-compatible or otherwise) that
means **one billed PUT/GET per key** — pathologically expensive at scale, and
slow (a network round-trip per key). Users asked for object-store portability
*without* per-key cost.

**Design.** Keys are hashed into a small, fixed number of **buckets**; each
bucket is persisted as **one object** in the backing `ObjectStore`. So a
keyspace of millions of keys costs `N` objects, not millions.

```
        key ──FNV-1a──▶ hash ──& (N-1)──▶ bucket index (0..N)
                                             │
   in-memory index:  Vec<RwLock<Option<HashMap<key,val>>>>   (one per bucket)
                                             │
   backing store:    bucket_0, bucket_1, … bucket_{N-1}      (one object each)
```

### 4.1 Hash / bucket / shard strategy

- **Hash — FNV-1a 64-bit.** Chosen over the standard-library `DefaultHasher`
  because its output must be **stable across processes**: a key must map to the
  same bucket after a restart. FNV-1a is fast, allocation-free, and
  deterministic.
- **Bucket — power-of-two masking.** `N` is rounded up to a power of two so
  routing is a single bitmask `hash & (N-1)` (no modulo) and the distribution
  is uniform. Bucket sizing is a tuning knob: pick `N` so a bucket object stays
  a comfortable size (default 4096; e.g. millions of small keys → tens of KB
  per bucket).
- **Shard — independent bucket locks.** Each bucket has its own `RwLock` (for
  the resident map) and `Mutex` (for load/flush I/O). Writes to different
  buckets proceed fully in parallel; same-bucket writes serialize and
  **coalesce** into a single object write.

### 4.2 Read / write path

- **Read (`get`)** — O(1). On first touch a bucket's object is fetched once and
  decoded into the in-memory `HashMap`; every subsequent read is pure memory,
  **zero object-store round-trips**.
- **Write (`put`/`delete`)** — mutate the in-memory map, mark the bucket dirty,
  then persist per the flush policy:
  - `FlushPolicy::Sync` (default) — re-serialize and PUT the bucket object
    before the write is acked. Fully durable; one object PUT per write.
  - `FlushPolicy::Coalesce { interval_ms }` — a background task flushes all
    dirty buckets every interval, so a burst of writes to hot buckets collapses
    into **far fewer** object PUTs (dramatically cheaper on request-billed
    stores) at the cost of a bounded crash-loss window.
- **Prefix scan** — loads and sweeps every bucket, because hashing destroys key
  locality. This is the deliberate tradeoff: sharding optimizes point access
  (GET/PUT/DEL), not range scans. Use `warm`/`cold` where range scans dominate.

### 4.3 Bucket object encoding

Each bucket object is a compact self-framing blob, independent of any external
serde format: `[count:u32]` then per entry `[klen:u32][key][vlen:u32][value]`,
all big-endian. Decoding validates every length against the buffer, returning
`CorruptWal` rather than panicking on a truncated object.

### 4.4 Guarantees & limits

- **Object-count bound** (tested): N buckets ⇒ ≤ N objects regardless of key
  count.
- **Durability across restart** (tested): reopening rebuilds each bucket's map
  lazily from its object; deletes survive.
- **Not a replication leader.** Like `file-per-key`, sharded has no ordered
  durable log to ship, so it can be a replication *target* (`apply_replicated`
  works) but config rejects it as a *leader*.

---

## 5. Messaging layer

Three primitives share one durable append-log pattern (`log.rs`):
`[len:u32][offset:u64][ts:u128][payload]`, which truncates cleanly to the last
whole record after a crash.

- **Topics** (`topic.rs`) — pub/sub. `ephemeral` = in-memory broadcast
  (at-most-once, lowest latency); `durable` = persisted + replayable from an
  offset.
- **Queues** (`queue.rs`) — durable, at-least-once work distribution with ack +
  redelivery-on-timeout and competing consumers per group.
- **Streams** (`stream.rs`) — **Falcon Event Streaming**, below.

---

## 6. Falcon Event Streaming

A **stream** is the Kafka-shaped sibling of a topic. Where a topic is a single
log with live fan-out, a stream adds the three things an event pipeline needs:
**partitioning**, **consumer groups with durable offsets**, and
**replay + live tail**.

```
producer.append_keyed(key, payload)
        │
        │  partition = FNV-1a(key) % P            (same key ⇒ same partition ⇒ ordered)
        ▼
  ┌───────────── Stream "user-events" (P partitions) ─────────────┐
  │  partition_0.log   partition_1.log   …   partition_{P-1}.log   │  durable, offset-addressed
  │        │                 │                      │              │
  │   broadcast tx      broadcast tx           broadcast tx        │  live tail
  └───────────────────────────────────────────────────────────────┘
        │
   consumer groups (durable committed offset per partition):
     group "analytics":  [c0=12, c1=8,  … ]   ← resumes after commit on restart
     group "audit":      [c0=0,  c1=0,  … ]   ← independent cursor, full stream
```

### 6.1 Partitioning

- `partition = FNV-1a(partition_key) % partitions`. Records sharing a key land
  on the **same partition** and are therefore **totally ordered** relative to
  each other; unrelated keys spread across partitions for parallelism. The same
  stable hash as the sharded store — a key's partition never shifts on restart.
- Each partition is an independent durable `MessageLog`. A record's durable
  coordinate is `(partition, offset)`, offsets 1-based and monotonic per
  partition.
- Producers can also `append_to(partition, …)` directly (e.g. round-robin
  producers that partition themselves).

### 6.2 Consumer groups & offset commit

- A group holds one **committed offset per partition**, persisted in a tiny
  per-group file (`[count:u32]` then `offset:u64` × partitions), written via
  temp-file-and-rename so a crash never leaves a torn offset file.
- `poll(group, partition)` returns records *after* the group's committed offset
  — it does **not** commit. The consumer commits after processing
  (`commit(group, partition, offset)`), which is what makes delivery
  **at-least-once**: a crash between poll and commit replays uncommitted
  records rather than losing them.
- Commits are **monotonic** (a backwards commit is ignored). Different groups
  have independent cursors, so each group sees the full stream.
- On reopen, committed offsets are recovered from the offset files, so a
  consumer resumes exactly where it left off (tested).

### 6.3 Live tail

Each partition also has a `tokio::broadcast` sender: `subscribe(partition)`
delivers records appended from now on, with the durable log always available to
replay anything a slow live subscriber lagged past. Durability precedes the
broadcast (append+fsync, *then* send), so a live consumer never sees a record
that isn't persisted.

### 6.4 Streams vs. topics vs. queues

| | Topic (durable) | Queue | Stream |
|--|------------------|-------|--------|
| Ordering | single log | per-group FIFO-ish | **per-partition (per-key)** |
| Parallelism | none | competing consumers | **partitions × groups** |
| Cursor | subscriber offset | server-side ack | **durable per-group, per-partition** |
| Replay | yes | no (consumed) | **yes, from any offset** |
| Delivery | at-least-once from cursor | at-least-once + redelivery | at-least-once from last commit |

Use a **topic** for simple fan-out, a **queue** for work distribution with
acks, and a **stream** when you need ordered-by-key, replayable, partitioned
event history with independent consumer groups.

### 6.5 Network API

Streams are usable over the wire, not just as a library primitive:

- **REST** (`falcon-api/src/rest/streams.rs`) — the full consumer lifecycle:
  - `POST /streams/{s}/records?key=K` — append (body = payload) → `{partition, offset}`
  - `GET  /streams/{s}/poll?group=G&partition=P` — records after the group's commit
  - `POST /streams/{s}/commit?group=G&partition=P&offset=O` — durably advance the cursor
  - `GET  /streams/{s}` — metadata (partition count)
- **Binary wire protocol** — `OP_STREAM_APPEND` (`0x20`), the high-throughput
  producer path: `keyspace` = stream, `key` = partition key, `value` = payload;
  reply is a `Stored{partition, offset}` frame. Auth-gated like every other op
  (the connection must AUTH first when a key is configured). Consumer
  poll/commit are request/response and live on REST.

The append path is durable-before-ack (append + fsync, then the live
broadcast), so no consumer — polling or live-tailing — ever observes a record
that isn't persisted.

---

## 7. Operations layer (single-image, autoscale-ready)

Falcon is designed to run as **one autoscalable container**. Four subsystems
make that safe; all are on by default with tunable, production-safe values.

### 7.1 Metrics (`falcon-metrics`)

A zero-dependency registry of lock-free atomic **counters** and **gauges** plus
fixed-bucket latency **histograms**, rendered to the Prometheus text format at
`GET /metrics`. Incrementing a counter is one relaxed atomic add — instrumenting
the request path is effectively free, and when nobody scrapes, the only cost is
those adds. The registry lives on the `Node` behind an `Arc` and is shared with
the HTTP and wire servers, so every path records into the same series. Exposed
signals include op counts, GET hit/miss, per-op latency histograms, WAL bytes,
replication lag, and connection/subscription gauges — everything an HPA/KEDA
autoscaler needs to scale on real throughput and tail latency.

### 7.2 Liveness vs. readiness

- `GET /healthz` — **liveness**: 200 whenever the process is up. Drives the
  orchestrator's restart decision.
- `GET /readyz` — **readiness**: 200 only once startup has completed (and, in
  principle, a follower has caught up), 503 otherwise. Orchestrators route
  traffic on readiness but restart on liveness, so a catching-up node stays
  alive without receiving reads. Backed by the `falcon_ready` gauge.

Both, plus `/metrics`, bypass auth so probes/scrapers work without a key.

### 7.3 Graceful shutdown

`shutdown_signal()` resolves on `SIGTERM` (k8s/docker stop) or Ctrl-C. It fans
out over a broadcast channel to every server, which stop accepting and drain
in-flight work via axum's `with_graceful_shutdown` / a `select!` on the wire
accept loop. Once drained, the process marks itself not-ready and calls
`Node::flush_all()`, which invokes `StorageEngine::flush` on every keyspace —
the authoritative final persist that closes the sharded store's coalesce window
and any interval-fsync WAL gap. Bounded by `[ops] shutdown_grace_secs`. This is
what makes autoscaling and rolling deploys **zero-loss**.

### 7.4 WAL compaction

The warm-tier WAL is append-only, so without compaction a long-lived container's
disk and restart-replay time grow without bound. A background task
(`Node::spawn_compactor`) periodically rewrites each eligible WAL as a snapshot
of only the **live** keys — dropping every superseded value and tombstone — via
`WarmEngine::compact_inner`:

1. Take the WAL's exclusive `RwLock` write guard (normal writes hold a shared
   read guard, so they briefly pause; reads never touch this lock).
2. Snapshot live keys from the in-memory map (deterministic key order), each
   carrying its current HLC.
3. Write them to a temp WAL, fsync, and **atomically rename** over the live WAL
   (a crash before the rename leaves the old WAL intact; after, the new one is
   fully durable).
4. Spawn a fresh `WalWriter` on the compacted file and swap it in; replace the
   shared sparse index's contents in place.

Compaction **renumbers sequences**, which would break a replication leader's
watermark contract, so `Node::compact_all` skips replicated keyspaces. It also
only runs once a WAL exceeds `[ops] compaction_min_bytes`, so small/idle stores
are never rewritten needlessly.

### 7.5 Anti-OOM

`[storage] max_value_bytes` (default 64 MiB) caps request body size via an axum
`DefaultBodyLimit` layer, so a single huge PUT is rejected with `413` before it
can allocate. `0` disables the cap.

---

## 8. Events & replication

- **`ChangeEvent`** carries keyspace, key, value (Put/Delete), sequence,
  timestamp, origin region, and an **HLC** stamp.
- **Single-leader** (default): the leader ships its ordered engine log to
  followers over gRPC; `sequence` orders writes; `apply_replicated` is
  idempotent (no-op if `sequence <= last_applied`).
- **Multi-leader** (active-active): every write is stamped with a **Hybrid
  Logical Clock** and applied via last-write-wins. Concurrent same-key writes
  converge deterministically (one wins, no merge) — eventual consistency. Only
  the warm tier supports it (HLC is persisted there).
- Engines without an ordered log (`file-per-key`, `sharded`) can be replication
  *targets* but not *leaders* — enforced at config validation.

---

## 9. Performance & safety principles

- **Pay-for-what-you-use.** Optional subsystems allocate nothing until enabled
  per keyspace/topic/stream. The `EventBus` isn't even constructed without
  subscribers or replication.
- **Sharding everywhere contention would concentrate.** Lock table (1024
  shards), sharded store buckets, and stream partitions all use the same idea:
  hash into independent slots so unrelated work never contends.
- **Stable hashing** (FNV-1a) wherever a hash must survive a restart (bucket
  routing, partition routing).
- **Amortized fsync** via group commit (WAL) and coalesced flush (sharded
  store) to turn per-op durability cost into per-batch cost.
- **Zero `unsafe`**, compiler-enforced on every crate; fuzz-tested parsers;
  crash-recovery tests for every durable format (WAL, message log, bucket
  objects, offset files).

### Measured performance

From `falcon-bench` (`--release`, LTO) on a development Mac (Apple Silicon,
APFS). Reproducible via the commands in the README's Benchmarks section.

| Path | Throughput | p50 | p99 |
|------|-----------:|----:|----:|
| Wire GET, pipeline d=128 | 5.6 M ops/sec | 152 µs | 341 µs |
| Sustained read load, 64 conns | 3.0 M ops/sec | 328 µs | 615 µs |
| HTTP GET (JSON) | 61 K ops/sec | 79 µs | 197 µs |
| Write, `fsync` every write (max durability) | ~1 K ops/sec | 7 ms | 11 ms |
| Write, `interval_fsync_ms = 10` | 397 K ops/sec | 1 ms | 5 ms |

The read path is Redis-class (millions of ops/sec, sub-millisecond tail). The
write path is a **durability dial**: fsync-every-write guarantees zero
acked-write loss and is bound by disk fsync latency; `interval_fsync_ms` trades
a bounded loss window for a ~400× throughput gain. Every sustained test stayed
`STABLE` (flat throughput, no tail runaway) under saturation. As of this
writing the workspace has **104 tests across 42 binaries, all green**.

---

## 10. Where to look in the code

| Concern | File |
|--------|------|
| Storage trait & tiers | `crates/falcon-storage/src/engine.rs` |
| Sharded engine | `crates/falcon-storage/src/sharded_store.rs` |
| Object-store seam | `crates/falcon-storage/src/object_store.rs` |
| Per-key lock table | `crates/falcon-storage/src/lock_table.rs` |
| Group-commit WAL | `crates/falcon-storage/src/wal.rs`, `wal_writer.rs` |
| Event streaming | `crates/falcon-messaging/src/stream.rs` |
| Metrics registry + Prometheus | `crates/falcon-metrics/src/` |
| WAL compaction | `crates/falcon-storage/src/warm.rs` (`compact_inner`) |
| /metrics, /readyz, body limit | `crates/falcon-api/src/rest/handlers.rs`, `server.rs` |
| Graceful shutdown + compactor | `crates/falcon-core/src/node.rs`, `crates/falcon-cli/src/main.rs` |
| Topics / queues / log | `crates/falcon-messaging/src/{topic,queue,log}.rs` |
| Keyspace (write→event) | `crates/falcon-core/src/keyspace.rs` |
| Node wiring | `crates/falcon-core/src/node.rs` |
| Config & validation | `crates/falcon-core/src/config.rs` |
| Replication | `crates/falcon-replication/src/` |
```
