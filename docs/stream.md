# Falcon Event Stream — architecture & rationale

**Partitioned, replayable event logs** (Kafka-shaped). Records route to
partitions by key (same key → same partition → totally ordered). Consumer groups
keep a durable per-partition offset and resume where they left off; any group can
replay history independently.

- **Install:** `falcon install stream` · **Default stream:** `events` (1 partition)
- **Core code:** [`stream.rs`](../crates/falcon-messaging/src/stream.rs),
  [`log.rs`](../crates/falcon-messaging/src/log.rs)

---

## 1. What it is — API surface

One product = one URL (`/stream`). Append a `{key,value}`; read the next batch
with a plain `GET` (which also advances your position). Partitions, groups, and
offsets are internal — same-key records stay ordered automatically.

**Example — a per-user activity stream.** Use the user id as the `key` so all of
one user's events keep their order; a consumer reads the next batch in a loop and
can replay history from the start. Unlike a [Queue](queue.md), the log is
retained and any number of independent consumers can read it at their own pace.

### CLI / HTTP
```bash
# key = user id → all of user 42's events stay in order on one partition
falcon stream append '{"event":"page_view","path":"/home"}' --key user:42
falcon stream next                               # read the next batch

curl -X POST localhost:8080/stream -H 'content-type: application/json' \
     -d '{"key":"user:42","value":"{\"event\":\"page_view\",\"path\":\"/home\"}"}'  # → {"ok":true}
curl localhost:8080/stream                       # → {"items":[{"value":"..."}]}
```
### Binary wire
`STREAM_APPEND` over the pipelined TCP protocol on `:6380`.

---

## 2. How it's built

```
   append(key, payload)
        │  partition = fnv1a(key) % N   (same key → same partition → totally ordered)
        ▼
   ┌──────────── stream_<name>/ ────────────┐        ┌──── consumer groups ────┐
   │ partition_0.log  ─▶ broadcast (live)    │        │ analytics: off[p0..pN]  │
   │ partition_1.log  ─▶ broadcast (live)    │◀─poll──│ billing:   off[p0..pN]  │
   │ …                                       │─commit▶│  (durable in offsets/   │
   │ partition_N.log                         │        │   group_<g>.off)        │
   │  ▲ all partitions share ONE SharedWriter│        └─────────────────────────┘
   │    → one fsync per touched file / batch │   groups read the same history
   └─────────────────────────────────────────┘   independently; either can replay
```

A `Stream` is **N partitions**, each an independent durable `MessageLog`, plus
durable per-group offsets:

| Structure | Role |
|-----------|------|
| `partitions: Vec<Partition>` | each is one append log + a live broadcast channel |
| `groups: HashMap<String, GroupCursor>` | per group, a committed offset **per partition** |
| `offsets_dir` | each group's committed offsets mirrored to one small `.off` file (durable) |
| one shared `SharedWriter` | all partitions coalesce onto one fsync-batching thread |

### Append path — routing by key
```
append(key, payload)
  ├─ partition = fnv1a(key) % N          # same key → same partition
  └─ partitions[partition].append(payload)  → offset; also broadcast live
```
### Consume path — poll + commit
```
poll(group, partition)  → records at/after the group's committed offset for that partition
commit(group, partition, offset)  → advance and persist the committed offset (durable)
```
After a restart, `recover_groups` reloads each group's committed offsets from
`offsets/`, so consumers resume exactly where they left off.

---

## 3. Why it's built this way — the reasoning

**Why partition by key hash?** Two requirements pull in opposite directions:
*ordering* (events for one entity must be processed in order) and *parallelism*
(the whole stream must scale past one consumer / one disk). Hashing the key to a
partition satisfies both: everything for `user:7` lands on one partition and is
therefore **totally ordered**, while unrelated keys spread across partitions and
are consumed in parallel. This is the same bargain Kafka strikes, for the same
reason.

**Why FNV-1a specifically, and why the same hash as the sharded store?** The
partition of a key must be **stable across processes, restarts, and platforms** —
otherwise a restart could re-route a key to a different partition and shatter its
order. Rust's `DefaultHasher` is explicitly *not* guaranteed stable; FNV-1a is a
fixed, well-defined function. Reusing the exact hash the sharded storage tier uses
keeps one stable-hashing rule in the codebase rather than two that could drift.
(The stream maps the hash to a partition with `% N`; the sharded *storage* tier
uses a power-of-two bucket count and a bitmask instead — same hash, different
downstream mapping suited to each.)

**Why per-partition durable offsets held by the *consumer group*, not the
server?** This is what makes streams *replayable* and multi-reader. The server
does not decide "this message was consumed and can be dropped"; each group tracks
its own progress. So an `analytics` group and a `billing` group read the same
history independently and at their own pace, and either can rewind and replay by
committing an earlier offset. Server-side "delete on consume" (a queue's model)
would make that impossible — which is exactly why Streams and the
[Queue](queue.md) are *different products* rather than one.

**Why one shared group-commit writer across all partitions?** Each partition
fsyncs independently for ordering, but a burst of appends spread across partitions
would otherwise cause many separate fsyncs. Funnelling every partition's writes
through one `SharedWriter` lets a burst fsync each touched partition file once per
batch. `interval_fsync_ms` goes further — coalescing fsyncs across partitions on a
timer to reclaim throughput at a bounded loss window. This is the "more partitions
trade single-node write throughput for parallel ordering" dial, made explicit.

---

## 4. Storage on disk
```
<data-dir>/stream/stream_<name>/
├── partition_0.log            # one append log per partition
├── partition_1.log
└── offsets/
    └── group_<group>.off      # durable per-group committed offsets
```
Own `stream/` subdirectory — isolated from a like-named Pub/Sub topic (see
[pubsub.md](pubsub.md)).

## 5. Configuration
| Key (stream tuning) | Effect | Why |
|---------------------|--------|-----|
| `partitions` | number of partitions | more = more parallel ordering, less single-node throughput |
| `interval_fsync_ms` | 0 = fsync every append; >0 = coalesce | trade a bounded loss window for throughput |
| `capacity` | live-tail buffer size | bounds memory for the live broadcast |

## 6. Multi-region replication
Each partition's ordered log ships to other regions like the other products (see
the [README](../README.md#multi-region-replication)).

## 7. Benchmarks

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` + LTO**;
the run also asserts per-key ordering and offset resume. Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --bench-all           # Event Stream row below
```

| Product | Concurrent peak | Correctness verified |
|---------|----------------:|----------------------|
| **Falcon Event Stream** | ~4,340 ops/sec | per-key ordered; resumes at committed offset |

Throughput scales with partition count: each partition orders independently, and
the shared group-commit writer coalesces fsyncs across them (raise
`interval_fsync_ms` to trade a bounded loss window for still higher throughput).

## 8. Guarantees
- **Per-key total order** within a partition.
- **Durable offsets:** a group resumes at its committed offset after restart;
  history is replayable independently by any group.
- Verified by `falcon-bench` (per-key ordered; resumes at committed offset).
