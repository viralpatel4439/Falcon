# Choosing a Falcon product — comparison, why & when

Falcon has five products. They share one binary and (for the key-value pair) an
almost identical API, so the real question is never "how do I call it" — it's
**"which one fits what I'm doing."** This doc answers that: the differences, the
reason each exists, and when to reach for which.

- [30-second decision guide](#30-second-decision-guide)
- [The two shapes: key-value vs. messaging](#the-two-shapes-key-value-vs-messaging)
- [Cache vs. KV Store](#cache-vs-kv-store) — the most common confusion
- [Pub/Sub vs. Queue vs. Event Stream](#pubsub-vs-queue-vs-event-stream)
- [Full capability matrix](#full-capability-matrix)
- [Worked scenarios](#worked-scenarios)
- [Using more than one together](#using-more-than-one-together)

---

## 30-second decision guide

```
Do you store a VALUE you look up later by key, or do you move a MESSAGE between parties?

STORE a value ─────────────┐
  │                        │
  │  Must it survive a     │  It's hot/derived and
  │  crash & be the        │  safe to lose? Bigger
  │  source of truth?      │  than RAM? Should expire?
  ▼                        ▼
 KV Store                Cache

MOVE a message ────────────┬───────────────────────┐
  │                        │                       │
  │ Everyone interested    │ Exactly ONE worker    │ Ordered history that
  │ should get a copy      │ should handle each    │ many readers replay
  │ (fan-out)              │ job (work-sharing)    │ at their own pace
  ▼                        ▼                       ▼
 Pub/Sub                 Queue                  Event Stream
```

One-liners:

| Product | Use it when… |
|---------|--------------|
| **Cache** | you want a hot value back fast, it's derived/disposable, and it should expire on its own. |
| **KV Store** | the value is your source of truth and must survive a crash; you may also want to list keys or watch changes live. |
| **Pub/Sub** | you broadcast an event and every interested subscriber should get its own copy. |
| **Queue** | you have work that exactly one worker should do, and losing a job is unacceptable. |
| **Event Stream** | you need an ordered, replayable log that several independent consumers read at their own pace. |

---

## The two shapes: key-value vs. messaging

Falcon's five products fall into two families:

- **Key-value stores** — **Cache** and **KV Store**. You `put(key, value)` and
  `get(key)`. The value stays until you overwrite, delete, or (for cache) it
  expires/evicts.
- **Messaging** — **Pub/Sub**, **Queue**, **Event Stream**. You append a message
  and *someone else consumes it*. The difference between the three is **who gets
  the message and whether it's kept afterward**.

Pick the family first (am I storing a value, or moving a message?), then pick
within it below.

---

## Cache vs. KV Store

Both are `{key, value, ttl?}` over one URL — so on the surface they look the
same. They are **not**: they are built on different storage engines and make
opposite promises. This is the distinction to get right.

| | **Falcon Cache** | **Falcon KV Store** |
|---|---|---|
| **Storage engine** | `tiered` — bounded RAM working set (CLOCK eviction) over a durable sled tail | `warm` — full in-RAM index + group-commit WAL |
| **What lives in RAM** | only the **hot working set** (capped by `hot_capacity_mb`) | the **entire keyspace's index** (every key) |
| **Capacity** | can far **exceed RAM** — cold keys sit on disk, promote on read | **bounded by RAM** — the index must fit |
| **On a cache miss** | one disk read from the cold tier, then promote to RAM | never happens — every key is already in RAM |
| **Eviction** | **yes** — cold keys dropped under memory pressure (lossless: already on disk) | **never** — nothing is evicted |
| **Durability stance** | durable underneath, but entries are **meant to be disposable** | **authoritative** — an acked write is the source of truth and survives a hard restart |
| **Scan / list keys** | **no** (exact-key lookup by design) | **yes** — `GET /kv/scan?prefix=` |
| **Live subscriptions (WebSocket)** | not enabled | **enabled** — watch a key prefix change in real time |
| **TTL** | yes — the whole point | yes, but optional |
| **On disk** | `<data-dir>/cache/…` (sled) | `<data-dir>/kv/default.wal` |

### Why they're separate products

They optimize **opposite ends of one trade-off**:

- A **cache** accepts *"your data might not be here"* (it can expire or be
  evicted) in exchange for holding datasets **bigger than RAM** while serving the
  hot set at RAM speed. Losing a cold entry is fine — you recompute or refetch it.
- A **store** refuses to *ever silently drop your data* — every acked write is
  durable and authoritative — at the cost of the whole index having to **fit in
  RAM**.

That's also why the APIs differ where it counts: the cache has **no scan**
(enumerating a set that expires and evicts is racy and defeats the tiering), and
KV **adds scan + live subscriptions** (a store is meant to be listed and
watched). See [cache.md](cache.md) and [kv.md](kv.md) for the internals.

### When to use which

- **Use Cache** for: session tokens, rate-limit counters, rendered fragments,
  computed results, anything hot + derived + safe to lose, or a working set
  larger than RAM where only the hot part must be fast.
- **Use KV Store** for: user profiles, account records, config, any data that is
  the **truth** and must survive a restart — especially if you also want to
  `scan` a prefix or subscribe to live changes.

> **Rule of thumb:** if losing an entry means "recompute it," use Cache. If
> losing an entry means "data loss," use KV.

---

## Pub/Sub vs. Queue vs. Event Stream

All three move messages, but they answer three different questions about **who
receives a message and what happens to it afterward.**

| | **Pub/Sub** | **Queue** | **Event Stream** |
|---|---|---|---|
| **Delivery** | **fan-out** — every live subscriber gets a copy | **work-sharing** — each message goes to exactly one consumer in the group | **replayable log** — each consumer group reads the whole log independently |
| **After delivery** | gone (ephemeral) / replayable (durable topic) | removed once acked | **retained** — history stays for replay |
| **Ordering** | per-topic | FIFO-ish with redelivery | **per-key total order** (same key → same partition) |
| **Delivery guarantee** | at-most-once (ephemeral) / persisted (durable) | **at-least-once** (ack + redelivery on timeout) | at-least-once from a durable committed offset |
| **Consumer model** | N subscribers, all get everything | competing workers split the work | N groups, each replays at its own pace |
| **Missed while offline?** | ephemeral: lost · durable: replay | waits in the queue for you | still there — resume from your offset |
| **Canonical use** | notifications, cache-invalidation, live feeds | background jobs, task processing | event sourcing, analytics pipelines, audit logs |

The distinction in one line each:

- **Pub/Sub** — "tell **everyone** who's listening." A copy per subscriber.
- **Queue** — "give this to **exactly one** worker, and don't lose it." One
  consumer per message.
- **Event Stream** — "keep an **ordered history** everyone can replay." The log
  is retained; consumers track their own position.

### Queue vs. Stream (the subtle one)

Both are durable and ordered, so they're easy to confuse. The difference is
**retention and readership**:

- A **Queue** *consumes* a message — once a worker acks it, it's done and gone.
  One logical consumer. Use it for **work** ("resize this image").
- A **Stream** *retains* the log — a consumer only advances its own offset; the
  data stays for other consumers and for replay. Many independent consumers. Use
  it for **facts** ("a page was viewed") that several systems each process
  differently (analytics *and* billing *and* audit), possibly replaying history.

See [pubsub.md](pubsub.md), [queue.md](queue.md), [stream.md](stream.md).

---

## Full capability matrix

| Capability | Cache | KV | Pub/Sub | Queue | Stream |
|------------|:-----:|:--:|:-------:|:-----:|:------:|
| Key-value get/put | ✓ | ✓ | — | — | — |
| Durable / survives restart | ✓¹ | ✓ | ✓² | ✓ | ✓ |
| Authoritative (source of truth) | — | ✓ | — | — | ✓ |
| Bigger-than-RAM dataset | ✓ | — | — | — | ✓ |
| Eviction under pressure | ✓ | — | — | — | — |
| TTL / expiry | ✓ | ✓ (opt) | — | — | — |
| Scan / list keys | — | ✓ | — | — | — |
| Live subscribe (WebSocket) | — | ✓ | ✓ | — | — |
| Fan-out (all consumers) | — | — | ✓ | — | ✓³ |
| Work-sharing (one consumer) | — | — | — | ✓ | — |
| Replay history | — | — | ✓² | — | ✓ |
| Ordering guarantee | — | per-key seq | per-topic | FIFO-ish | per-key total |
| Multi-region replication | ✓ | ✓ | ✓ | ✓ | ✓ |

¹ Cache is durable underneath but entries are meant to be disposable (expire/evict).
² Durable topics only; ephemeral topics are at-most-once and hold nothing.
³ Stream gives fan-out *across consumer groups* — each group reads the whole log.

---

## Worked scenarios

| You want to… | Use | Why |
|--------------|-----|-----|
| Keep users logged in (sessions that time out) | **Cache** | hot lookups, TTL expiry, losing one just forces re-login |
| Store user profiles / accounts | **KV** | source of truth, must survive restart, scannable by prefix |
| Rate-limit by IP (N requests/min) | **Cache** | a counter with a short TTL; disposable |
| Serve a dataset larger than RAM with a hot set | **Cache** | tiered engine keeps the hot set in RAM, rest on disk |
| Notify every service that an order was placed | **Pub/Sub** | fan-out — email, analytics, inventory each get a copy |
| Resize uploaded images in the background | **Queue** | one worker per job, at-least-once, never lose work |
| Send one welcome email per signup | **Queue** | exactly-one-worker semantics prevents duplicate emails |
| Feed a clickstream to analytics + billing + audit | **Stream** | one ordered log, several groups replay independently |
| Rebuild a read model by replaying all events | **Stream** | retained, replayable, per-key ordered |
| Push live updates to a dashboard | **KV** (subscribe) or **Pub/Sub** | KV if it's key changes; Pub/Sub for arbitrary events |

---

## Using more than one together

The products compose — a real app usually runs several on one node (`full`
build), each isolated in its own storage directory. A common shape:

```
signup request
   ├─ KV Store   put user:42  (durable profile, the source of truth)
   ├─ Cache      put session:… ttl=1800  (fast, expiring login)
   ├─ Pub/Sub    publish user.created  (email + analytics react)
   └─ Queue      push "send-welcome-email"  (one worker sends it, at-least-once)

…meanwhile every page view →
   └─ Stream     append {event:page_view} key=user:42  (ordered, replayable feed)
```

Each keeps its own files under `<data-dir>/{kv,cache,pubsub,queue,stream}/`, so
co-locating them never mixes their storage. See
[architecture.md](architecture.md#one-directory-per-product-on-disk).
