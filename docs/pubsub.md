# Falcon Pub/Sub — architecture & rationale

**Publish/subscribe topics with live fan-out.** Publish to a topic; every live
subscriber receives the message. Topics are `ephemeral` (fast, in-memory,
at-most-once) or `durable` (persisted and replayable).

- **Install:** `falcon install pubsub` · **Default topic:** `events` (ephemeral)
- **Core code:** [`topic.rs`](../crates/falcon-messaging/src/topic.rs),
  [`log.rs`](../crates/falcon-messaging/src/log.rs)

---

## 1. What it is — API surface

One product = one URL (`/pubsub`). Publish a value in a JSON body; subscribe over
WebSocket. No topic name to manage.

**Example — broadcast an event other services react to.** Publish an
`order.placed` event once; every live subscriber (email service, analytics,
inventory) gets it. Fan-out, not point-to-point — for a job exactly one worker
should handle, use [Queue](queue.md) instead.

### CLI / HTTP
```bash
falcon topic publish '{"event":"order.placed","order":1001,"total":49.90}'
curl -X POST localhost:8080/pubsub -H 'content-type: application/json' \
     -d '{"value":"{\"event\":\"order.placed\",\"order\":1001}"}'   # → {"ok":true}
```
### Subscribe (WebSocket)
```
ws://localhost:8080/subscribe?topic=events
```
### Binary wire
`PUBLISH` / `SUBSCRIBE` over the pipelined TCP protocol on `:6380`.

---

## 2. How it's built

```
                              ┌──────────────────────── Topic ────────────────────────┐
   publish(msg) ──▶ HTTP/wire │  (durable) log.append(msg) ──▶ topic_<name>.log        │
                              │        │                       (MessageLog append log) │
                              │        ▼                                               │
                              │  tokio::broadcast::Sender ─┬─▶ Receiver ─▶ subscriber A │
                              │                            ├─▶ Receiver ─▶ subscriber B │
                              │                            └─▶ Receiver ─▶ subscriber C │
                              └────────────────────────────────────────────────────────┘
   Ephemeral: broadcast only (no disk).  Durable: append first, then fan out; a new
   subscriber can replay the log, then tail live.
```

A `Topic` is a `tokio::sync::broadcast` channel, optionally paired with a durable
append log:

| Mode | Structure | Delivery |
|------|-----------|----------|
| **Ephemeral** | broadcast channel only | at-most-once to whoever is connected now |
| **Durable** | broadcast channel **+** `MessageLog` (append log) | persisted; a new subscriber can replay history, then tail live |

### Publish path
```
publish(msg)
  ├─ (durable only) log.append(msg)     # persist first
  └─ tx.send(msg)                        # fan out to every live subscriber
```
A subscriber is a `broadcast::Receiver`; the server bridges it to a WebSocket or
a wire `SUBSCRIBE` stream. `broadcast` delivers each message to *all* current
receivers — that is the fan-out semantic.

---

## 3. Why it's built this way — the reasoning

**Why offer ephemeral *and* durable instead of one mode?** They serve opposite
needs. Ephemeral fan-out (metrics ticks, presence, cache-invalidation pings) wants
the lowest possible cost and does not care about a subscriber that was offline —
so it holds *nothing* on disk and never fsyncs. Durable topics (an event other
services must not miss) must survive a restart and let a late subscriber catch
up — so they append to a log. Forcing durability on the ephemeral case would tax
the common high-rate path for a guarantee it doesn't want; forcing everyone to be
ephemeral would make the log-backed use case impossible. One knob, two honest
semantics.

**Why `tokio::broadcast` for fan-out?** Fan-out is "one producer, N independent
consumers, each gets every message." That is exactly `broadcast`'s contract, with
back-pressure/lag handling built in — a slow subscriber that falls behind the
channel capacity is signalled a lag rather than blocking the publisher or the
other subscribers. Building this by hand (a `Vec` of per-subscriber queues) would
re-implement `broadcast` with more bugs.

**Why does a durable topic reuse the *same* `MessageLog` as the Queue and Event
Stream?** All three are "an ordered, durable sequence of records." Sharing one
append-log primitive ([`log.rs`](../crates/falcon-messaging/src/log.rs)) — with
the same group-commit writer — means the durability, recovery, and fsync-batching
logic is written, tested, and optimized **once**. The products differ in what
they layer *on top* (broadcast vs. per-group cursor vs. partitions), not in how
bytes reach the disk.

---

## 4. Storage on disk
```
<data-dir>/pubsub/topic_<name>.log      # durable topics only
```
Own `pubsub/` subdirectory. **Ephemeral topics hold nothing on disk**, so
`pubsub/` may be empty. This isolation is what lets a Pub/Sub topic named
`events` and an Event Stream named `events` coexist on one node — they live in
`pubsub/` and `stream/` and can never read each other's files.

## 5. Configuration
Topic mode (`ephemeral`/`durable`) and channel capacity are set in
[`config/default.toml`](../config/default.toml). `data-dir` sets the base;
durable logs live under `<data-dir>/pubsub/`.

## 6. Multi-region replication
Durable topics ship their ordered log to other regions like the other products
(see the [README](../README.md#multi-region-replication)).

## 7. Benchmarks

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` + LTO**;
the run also asserts ordering and cross-restart persistence. Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --bench-all           # Pub/Sub row below
```

| Product | Concurrent peak | Correctness verified |
|---------|----------------:|----------------------|
| **Falcon Pub/Sub** | **~4,550 ops/sec** | ordered; persisted across restart |

Pub/Sub is the highest-throughput product in `--bench-all`: fan-out over the
in-memory `broadcast` channel has no per-subscriber disk cost, and durable topics
append through the same group-commit writer the other products use.

## 8. Guarantees
- **Ephemeral:** at-most-once fan-out to whoever is connected at publish time.
- **Durable:** ordered, persisted, replayable across restart.
- Verified by `falcon-bench` (ordered; persisted across restart).
