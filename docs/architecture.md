# Falcon architecture & product guide

The index for Falcon's documentation, plus the **shared architecture** every
product is built on. Falcon ships **five installable data products** behind one
Rust binary; this directory has one document per product, each written to answer
three questions, in order:

1. **What is it** — the product and its API surface (CLI, HTTP, wire).
2. **How is it built** — the architecture: the data structures, the write/read
   paths, the durability and ordering mechanics, and where it stores data.
3. **Why is it built that way** — the reasoning behind each design choice, and
   the trade-off it deliberately makes.

| Product | Install | Doc | One-line architecture |
|---------|---------|-----|-----------------------|
| **Falcon Cache** | `falcon install cache` | [cache.md](cache.md) | RAM working set (CLOCK eviction) write-through to a durable on-disk tail |
| **Falcon KV Store** | `falcon install kv` | [kv.md](kv.md) | In-memory index + group-commit WAL that doubles as the replication log |
| **Falcon Pub/Sub** | `falcon install pubsub` | [pubsub.md](pubsub.md) | Broadcast fan-out over an optional durable append log |
| **Falcon Queue** | `falcon install queue` | [queue.md](queue.md) | One durable log + per-group cursor and in-flight set (at-least-once) |
| **Falcon Event Stream** | `falcon install stream` | [stream.md](stream.md) | Key-hashed partitions, each a durable log, with durable group offsets |

**Not sure which product you need?** → **[compare.md](compare.md)** — the
differences, the *why*, and *when* to use each (Cache vs. KV, Pub/Sub vs. Queue
vs. Stream, a capability matrix, and worked scenarios).

For the platform-wide picture — install/serve model, protocols, TLS, auth,
multi-region replication, and benchmarks — see the top-level
[README](../README.md). The **shared foundation** section just below covers the
architecture every product is built on.

---

## The shared foundation (read this first)

The five products are **not** five separate databases. They are five views over
a small set of shared primitives. Understanding these once makes every
per-product doc short, because each doc only has to explain *which* primitives it
composes and *why*.

### The one shared write path → `ChangeEvent`

Every KV/cache write flows through **one** method — `Keyspace::put` /
`Keyspace::delete` ([`falcon-core/src/keyspace.rs`](../crates/falcon-core/src/keyspace.rs))
— which turns the write into a single ordered `ChangeEvent`. That one event is
then observed **identically** by three consumers:

```
                 Keyspace.put(k, v)  ── allocates one ordered sequence ──▶ ChangeEvent
                        │
      ┌─────────────────┼──────────────────────┐
      ▼                 ▼                        ▼
 StorageEngine      TTL expiry map       EventBus.publish
 (durable + seq)    (DashMap)            │
                                ┌────────┴─────────┐
                                ▼                  ▼
                       WebSocket subscribers   Replication log shipper
```

**Why one path:** if subscriptions, TTL, and replication each derived their own
notion of "what changed", they could disagree — a subscriber could see a write a
replica never got. Funnelling everything through one event makes "what a
subscriber sees", "what expires", and "what replicates" *the same ordered
stream, by construction*. The `EventBus` is only built when a keyspace actually
needs subscriptions or replication, so the default path allocates nothing extra.

### The group-commit WAL (the durability engine)

The durable products (KV, and Cache's cold tail, and every messaging log) are
built on the same idea: an **append-only log** fronted by a **background writer
task that batches concurrent writes into one `fsync`** (group commit,
[`wal_writer.rs`](../crates/falcon-storage/src/wal_writer.rs)).

**Why group commit:** an `fsync` is a fixed, expensive cost (~milliseconds on a
real disk). One `fsync` per write caps you at `1 / fsync_latency` writes/sec no
matter how many cores or clients you have. By letting writes that arrive during
an in-flight `fsync` pile up and flush *together*, throughput scales with
concurrency instead of staying flat — the benchmarks show ~1 K ops/sec at
fsync-per-write climbing to ~400 K ops/sec once writes batch. Under light load a
batch is a single write, so latency is unchanged. It is a pure win with a dial
(`interval_fsync_ms`) for those who will trade a bounded loss window for even
more throughput.

### The `StorageEngine` trait (swappable durability)

Every KV/cache tier — `hot`, `warm`, `cold`, `tiered`, `sharded` — implements
one trait ([`engine.rs`](../crates/falcon-storage/src/engine.rs)):
`get / put / delete / scan_prefix / apply_replicated`, where `put`/`delete`
return a monotonic **sequence**.

**Why a trait:** the keyspace, the API layer, the WebSocket feed, and
replication all program against this interface, so a product picks its
durability/cost profile by *config* (a cache is `tiered`, KV is `warm`, an
object-store-backed keyspace is `sharded`) without any of the layers above
changing. It also makes third-party object storage a drop-in: `sharded` talks to
an `ObjectStore` trait, and swapping a local directory for a remote bucket is a
config change, not a code change.

### One directory per product on disk

When several products run on one node/container, **each keeps its files in its
own subdirectory** under `data-dir`:

```
<data-dir>/
├── kv/        default.wal                 # Falcon KV Store
├── cache/     cache_tiered/               # Falcon Cache
├── pubsub/    topic_<name>.log            # Falcon Pub/Sub (durable topics)
├── queue/     queue_<name>.log            # Falcon Queue
└── stream/    stream_<name>/…             # Falcon Event Stream
```

**Why isolate:** co-located products must never be able to address one another's
files — e.g. a Pub/Sub topic named `events` and an Event Stream named `events`.
Prefix-per-type made collisions unlikely; a directory-per-product makes them
*impossible*, and makes per-product backup, wipe, and quota trivial. Upgrading
from the older flat layout migrates existing files into their new directory
automatically on first start (best-effort rename, no data loss).

### Multi-region replication (a cross-cutting layer, not a product)

Every product can replicate across regions over a dedicated gRPC channel. It is
built on the same `ChangeEvent` stream above, so it is described once here rather
than repeated per product. The three write models — `single-leader`,
`multi-leader` (HLC last-write-wins), `primary-queue` (forward-to-primary, no
lost writes) — are covered in the
[README](../README.md#multi-region-replication).
