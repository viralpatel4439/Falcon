# Falcon KV Store — architecture & rationale

A **durable key-value store with real-time updates**. Every write is persisted
through a group-commit write-ahead log and, at the same instant, pushed to any
WebSocket subscribers watching that key's prefix.

- **Install:** `falcon install kv` · **Keyspace:** `default` · **Tier:** `warm`
- **Core code:** [`warm.rs`](../crates/falcon-storage/src/warm.rs),
  [`wal_writer.rs`](../crates/falcon-storage/src/wal_writer.rs),
  [`keyspace.rs`](../crates/falcon-core/src/keyspace.rs)

---

## 1. What it is — API surface

One product = one URL (`/kv`). Key and value travel in a JSON body; the
operation is the HTTP method. No keyspaces, no key-in-URL.

**Example — a durable user profile.** The `key:id` convention (`user:42`) groups
related records under a scannable prefix; values are whatever your app stores.

### CLI
```bash
falcon put "user:42" '{"name":"Ada Lovelace","plan":"pro"}'
falcon get "user:42"              # → {"name":"Ada Lovelace","plan":"pro"}
falcon delete "user:42"
falcon scan --prefix user:        # every user record
```
### HTTP / REST
```bash
curl -X POST localhost:8080/kv -H 'content-type: application/json' \
     -d '{"key":"user:42","value":"{\"name\":\"Ada Lovelace\",\"plan\":\"pro\"}"}'  # → {"ok":true}
curl 'localhost:8080/kv?key=user:42'                 # → {"value":"{\"name\":\"Ada Lovelace\"...}"}
curl -X DELETE 'localhost:8080/kv?key=user:42'
curl 'localhost:8080/kv/scan?prefix=user:'           # → {"items":[...]}
# optional TTL: add "ttl": 60 (seconds) to the POST body
```
`value` is a string — the client JSON-stringifies numbers/objects into it and
parses them back on read, so the store stays schema-free.
### Binary wire (low-latency hot path)
`GET`/`SET`/`DEL` over the pipelined TCP protocol on `:6380`
(`falcon config set wire-enabled true`).
### Real-time (WebSocket)
```
ws://localhost:8080/subscribe?prefix=user:
```

---

## 2. How it's built — the `warm` tier

```
   REST / wire / WS ──put(k,v)──▶ Keyspace ──▶ WarmEngine
                                     │              │
                                     │      ┌───────┼─────────────────┐
                                     │      ▼       ▼                 ▼
                                     │  KeyLockTable  DashMap map   WalWriter task
                                     │  (1024 mutexes)(RAM index)   (group-commit fsync)
                                     │                  ▲                 │ owns
                                     │   get(k): lock-free read           ▼
                                     │                              default.wal  ── seek via ──▶ SparseIndex
                                     ▼                              (append log)                 (seq→offset)
                                EventBus ──▶ WebSocket subscribers + replication log shipper
                                (built only if subscriptions/replication on)
```

The KV Store is the **`warm` engine**: an in-RAM index made durable by an
append-only, group-committed WAL. Two data structures:

| Structure | Type | Role |
|-----------|------|------|
| `map` | `DashMap<Vec<u8>, Vec<u8>>` | the authoritative in-memory key→value index; serves all reads |
| WAL | append-only file + `WalWriter` task | durability; on restart it is replayed to rebuild `map` |
| `sparse_index` | `SparseIndex` (seq → file offset) | lets replication seek near a sequence instead of scanning from byte 0 |

### The write path (single-leader / local)

```
put(k,v)
  └─ lock the key's shard (KeyLockTable: 1024 mutexes, key hashes to one)
      └─ seq_and_submit:
           ├─ [under seq_order mutex]  seq = next_sequence();  enqueue framed record to WAL
           └─ [outside the lock]       await the group-commit fsync
      └─ map.insert(k, v)      # index updated only after the record is durable
```

Two locks, each with a precise job (see `warm.rs`):

- **`KeyLockTable`** — a fixed array of 1024 mutexes; a key hashes to one shard.
  Writes to *different* keys almost never contend; repeated writes to the *same*
  key serialize in arrival order. Reads take **no** lock.
- **`seq_order`** — held only across sequence allocation + the (non-blocking)
  WAL enqueue, never across the `fsync` await.

### The read path

`get` is a single `DashMap` lookup — no lock, no disk. This is why reads
benchmark in the millions of ops/sec over the pipelined wire protocol.

### Restart recovery

`open_with_policy` replays the WAL, applying each `Put`/`Delete` to rebuild
`map`, and restores `max_seq` so sequence allocation resumes correctly. A
background **compactor** rewrites the WAL as a live-key snapshot once it exceeds
`ops.compaction_min_bytes`, bounding both disk size and replay time.

---

## 3. Why it's built this way — the reasoning

**Why an in-memory index rather than a B-tree on disk (like the `cold`/sled
tier)?** The KV Store's job is *durable general-purpose storage with the lowest
possible read latency for the working set*. Keeping the authoritative index in
RAM makes every read a hash lookup; durability is provided *beside* the index by
the WAL, not *by* the read structure. The `cold` and `tiered` tiers exist for
datasets that exceed RAM — KV picks `warm` because "fits in RAM, must be durable,
reads must be instant" is the common case.

**Why does the WAL double as the replication log?** A naive design keeps a
storage log and a separate replication log and copies between them — two things
to keep ordered and consistent. Falcon instead makes the durable WAL *be* the
ordered change log: `read_replog_from(seq)` serves a follower directly from the
same file, seeking via the sparse index. One log, one ordering, no divergence.

**Why is `seq_order` held across enqueue but not `fsync`?** This is the subtle
correctness point (documented at `warm.rs:39`). Sequence order *must* equal WAL
file order, or a follower's sparse-index catch-up strands. If two writes to
different keys allocated seq N and N+1 but enqueued out of order, the log would
be misordered. Holding `seq_order` across the *enqueue* (a non-blocking channel
send) guarantees file order = sequence order; releasing it before the `fsync`
await means group commit still batches fully. You get both ordering *and*
throughput.

**Why write the index only after the record is durable?** So a read can never
observe a value that a crash could lose. The `map.insert` happens after the
`fsync` returns — acknowledged reads reflect only durable state.

---

## 4. Storage on disk

```
<data-dir>/kv/default.wal
```
Own `kv/` subdirectory (isolated from co-located products). With a remote object
store attached, the keyspace instead uses the `sharded` tier under prefix
`falcon/default`.

## 5. Configuration

| Key | Effect | Why it exists |
|-----|--------|---------------|
| `write-mode` | `single-leader` / `multi-leader` / `primary-queue` | pick the multi-region consistency/latency trade-off |
| `interval_fsync_ms` | 0 = fsync-per-write; >0 = interval fsync | trade a bounded loss window for throughput |
| `storage.max_value_bytes` | rejects oversized PUT with `413` | anti-OOM guard (default 64 MiB) |

## 6. Multi-region replication

The keyspace is replication-aware; with `multi-leader` it uses the HLC
last-write-wins path (`put_lww`/`apply_lww` in `warm.rs`), with `primary-queue`
it forwards writes to the primary. See
[README](../README.md#multi-region-replication).

## 7. Benchmarks

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` + LTO**;
every run also asserts correctness. Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --bench-all                               # KV row below (durable path)
falcon-bench --skip-writes --pipeline-depths 1,16,128  # read path
```

**Read path** — served from the in-memory `map`, over the pipelined wire protocol:

| Path | Throughput | p50 | p99 |
|------|-----------:|----:|----:|
| Wire GET, pipeline depth=128 | **8.59 M ops/sec** | 106 µs¹ | 200 µs¹ |
| Wire GET, pipeline depth=16 | 2.30 M ops/sec | 53 µs¹ | 99 µs¹ |
| Wire GET, depth=1 (no pipeline) | 181 K ops/sec | 42 µs | 88 µs |
| HTTP GET (JSON, 1 req/op) | 80 K ops/sec | 86 µs | 205 µs |

**Write path** — every write goes through the group-commit WAL; durability is a dial:

| Write mode | Throughput | p50 | p99 |
|------------|-----------:|----:|----:|
| `fsync` every write (max durability, HTTP 1 req/op) | ~980 ops/sec | 7 ms | 12 ms |
| `interval_fsync_ms = 10` (≤10 ms loss window, 64 conns) | **393 K ops/sec** | 1 ms | 5 ms |

**Durable end-to-end** (`--bench-all`, 2000 records):

| Scenario | Concurrent peak | Per-op (sequential, durable) | Correctness |
|----------|----------------:|------------------------------|-------------|
| KV Store | ~2,120 ops/sec | p50 3.9 ms · p99 4.1 ms | 2000/2000 survived a hard restart |
| KV real-time (32 subs) | ~2,310 ops/sec | p50 4.0 ms · p99 4.3 ms | 640/640 writes notified, no drops/dupes |

¹ *Pipelined rows: latency percentiles are **per batch** (batch = depth ops);
throughput is aggregate. The sustained read test reported STABLE (no latency
cliff).*

## 8. Guarantees

- **Durable:** an acked write (fsync policy) survives a hard restart.
- **Ordered:** durable log order == sequence order == the stream subscribers and
  replicas observe.
- Verified by `falcon-bench`: 2000/2000 records survived a hard restart; 640/640
  writes notified to 32 subscribers with no drops or dupes.
