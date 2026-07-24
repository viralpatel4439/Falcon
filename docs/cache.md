# Falcon Cache — architecture & rationale

A **low-latency RAM cache that spills to disk, with TTL**. The hot working set is
served from memory at RAM speed while the full dataset lives durably on disk, so
the cache holds far more than RAM.

- **Install:** `falcon install cache` · **Keyspace:** `cache` · **Tier:** `tiered`
- **Core code:** [`tiered.rs`](../crates/falcon-storage/src/tiered.rs),
  [`cold.rs`](../crates/falcon-storage/src/cold.rs)

---

## 1. What it is — API surface

One product = one URL (`/cache`). Key, value, and optional TTL travel in a JSON
body; the operation is the HTTP method. There is no keyspace to name.

**Example — a login session that expires on its own.** Store the session under
its token with a 30-minute TTL; every request reads it back at RAM speed, and it
disappears automatically when it goes stale — no cleanup job, no stale logins.

### CLI
```bash
# session token -> the logged-in user, auto-expiring after 1800s (30 min)
falcon put "session:7f3a9c" '{"user":42,"role":"admin"}' --cache --ttl 1800
falcon get "session:7f3a9c" --cache          # → {"user":42,"role":"admin"}
falcon delete "session:7f3a9c" --cache       # e.g. on logout
```
### HTTP / REST
```bash
curl -X POST localhost:8080/cache -H 'content-type: application/json' \
     -d '{"key":"session:7f3a9c","value":"{\"user\":42,\"role\":\"admin\"}","ttl":1800}'
# → {"ok":true}
curl 'localhost:8080/cache?key=session:7f3a9c'   # → {"value":"{\"user\":42,\"role\":\"admin\"}"}
curl -X DELETE 'localhost:8080/cache?key=session:7f3a9c'
```
The cache is **exact-key lookup only — there is deliberately no scan/list.**
Entries expire and evict, so enumerating a cache would return a racy, partial
snapshot and walk the whole keyspace the tiering exists to keep cold. If you need
to list keys, that belongs in your store — use [KV](kv.md)'s `/kv/scan`.

Other natural fits: a rate-limit counter (`ratelimit:ip:1.2.3.4`, TTL 60),
a rendered page fragment, or a short-lived API token — anything hot, derived,
and safe to lose. `value` is a string — the client JSON-stringifies numbers or
objects into it and parses them back on read. The UI at `/` shows hit-rate,
hot keys/bytes, evictions, and TTL-tracked keys.

---

## 2. How it's built — the `tiered` engine

```
   get(k)                                   put(k,v)  [write-through]
     │                                          │
     ▼                                          ▼
  ┌─────────────────────────────────┐    ①cold.put ──▶ ┌───────────────────┐
  │ HOT: DashMap<key, HotEntry>      │◀── ②hot_insert   │ COLD: ColdEngine  │
  │  bounded by capacity_bytes       │                  │  (sled B-tree)    │
  │  each entry: value, ref_bit,size │                  │  full dataset,    │
  └───────────────┬─────────────────┘                  │  durable on disk  │
     hit → ref_bit=1, return          miss → cold.get,  └───────────────────┘
                   │                   promote into hot         ▲
                   ▼                                            │
            maybe_evict() ── CLOCK sweep: clear ref_bit or drop │
            victim already durable in cold ──────────────────────┘
```

Two layers behind one `StorageEngine`:

| Layer | Structure | Role |
|-------|-----------|------|
| **hot** | `DashMap<Vec<u8>, HotEntry>` bounded by `capacity_bytes` | RAM cache of the working set; each entry has a CLOCK `ref_bit` and its byte `size` |
| **cold** | `ColdEngine` (sled B-tree) | the durable, full dataset on disk |

### Write path — write-through
```
put(k,v)
  ├─ cold.put(k,v)        # durable first (sled fsync + sequence + replog)
  ├─ hot_insert(k,v)      # then cache in RAM for fast reads
  └─ maybe_evict()        # drop cold-enough hot entries if over the byte budget
```

### Read path — promote on miss
```
get(k)
  ├─ hot hit?  → set ref_bit, return from RAM      (the fast path)
  └─ hot miss? → cold.get(k); if found, promote into hot, maybe_evict
```

### Eviction — CLOCK / second-chance
`maybe_evict` runs only when `approx_bytes > capacity_bytes`. It samples up to
`evict_sample` entries: an entry whose `ref_bit` is set gets a second chance (bit
cleared, kept); the first entry with a clear bit is evicted. Work per call is
bounded so a writer never stalls unboundedly.

---

## 3. Why it's built this way — the reasoning

**Why write-through and not write-back?** With write-through, every write is
durable in `cold` *before* it is cached in `hot`. That makes **eviction a pure
RAM drop**: the victim is already on disk, so dropping it can never lose data,
and it simply promotes back on the next read. A write-back cache would have to
flush dirty victims on eviction — turning eviction into I/O on the hot path and
adding a data-loss window on crash. Write-through trades a little write latency
(one sled write per put) for a dramatically simpler, safer, faster eviction path.

**Why CLOCK eviction instead of true LRU?** True LRU needs a mutation of shared
ordering state (a linked list / heap) on *every read* — contention that would
throttle the very hot path the cache exists to make fast. CLOCK approximates LRU
with a single relaxed atomic store of one bit per read (`ref_bit.store(true)`),
which is effectively free and lock-free. The "recently used" signal is slightly
coarser than exact LRU, but the read path stays a `DashMap` lookup + one atomic —
the right trade for a cache.

**Why a byte budget (`capacity_bytes`) rather than a key count?** Cache value
sizes vary wildly; a key-count cap can't bound memory. Tracking
`key.len() + value.len()` per entry and evicting on bytes gives a real RAM bound,
which is what an operator sizing a container actually needs.

**Why is the dataset allowed to exceed RAM at all?** Because `cold` (sled) holds
everything durably; `hot` is only the working set. This is the whole pitch — and
the `TierStats` (hit-rate, promotions, evictions) are surfaced in `/healthz` so
the cost story is *visible*: a high hit-rate on a hot set far smaller than the
dataset is exactly what you want to see.

---

## 4. Storage on disk
```
<data-dir>/cache/cache_tiered/       # sled-backed cold store (the full dataset)
```
Own `cache/` subdirectory. With a remote object store attached, the keyspace uses
the `sharded` tier under prefix `falcon/cache` instead.

## 5. TTL
A background **TTL reaper** sweeps expired keys. Set a keyspace default with
`default_ttl_secs`, or a per-write TTL via `--ttl` / `?ttl=` (which overrides the
default). TTL flows through the same single `ChangeEvent` path as writes, so
expiry is observed consistently.

## 6. Configuration

| Key (keyspace tuning) | Effect | Why |
|-----------------------|--------|-----|
| `hot_capacity_mb` | RAM budget for the hot set (default 256 MB) | the real memory bound; dataset may exceed it |
| `evict_sample` | CLOCK sample size | bigger = better victim choice, more scan per evict |
| `default_ttl_secs` | default key expiry | per-write `?ttl=` overrides it |

## 7. Benchmarks

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` + LTO**.
The cache shares Falcon's read/write engine characteristics, so its numbers track
the general storage path. Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --skip-writes --pipeline-depths 1,16,128   # hot-hit read path
```

| Path | Throughput | p50 | p99 | Notes |
|------|-----------:|----:|----:|-------|
| Hot-hit GET, wire depth=128 | **8.59 M ops/sec** | 106 µs¹ | 200 µs¹ | served from the RAM `DashMap` (a hot hit is a lock-free read + one atomic) |
| Hot-hit GET, wire depth=1 | 181 K ops/sec | 42 µs | 88 µs | no pipelining |
| Write (write-through, durable) | ~980 ops/sec | 7 ms | 12 ms | bound by the `cold` (sled) fsync; a miss that promotes adds one cold read |

The cache's own signal is its **hit-rate** (`TierStats`, surfaced in `/healthz`):
a high hit-rate on a hot set far smaller than the dataset means most reads take
the RAM path above while the full dataset stays durable on disk.

¹ *Pipelined rows: latency is per batch (batch = depth ops); throughput aggregate.*

## 8. Guarantees
- Hot set served from RAM; cold tail on disk — capacity not bounded by RAM.
- Write-through ⇒ eviction never loses data; victims promote back on read.
- Expired keys reaped in the background; per-write TTL always wins.
