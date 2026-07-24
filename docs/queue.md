# Falcon Queue — architecture & rationale

**Durable work queues with competing consumers.** Push jobs; consumers in a group
each pop *different* jobs (work is distributed, not broadcast). Delivery is
**at-least-once**: a popped job must be `ack`'d, or it is redelivered after a
timeout.

- **Install:** `falcon install queue` · **Default queue:** `jobs` (30s ack timeout)
- **Core code:** [`queue.rs`](../crates/falcon-messaging/src/queue.rs),
  [`log.rs`](../crates/falcon-messaging/src/log.rs)

---

## 1. What it is — API surface

One product = one URL (`/queue`). Push a value; dequeue with a plain `GET`;
confirm with the returned `id`. No queue name or consumer group to manage.

**Example — a background-job queue.** A web request enqueues work (resize an
image, send an email); a pool of workers each pops a *different* job, does it,
and acks. If a worker crashes mid-job the ack never comes and the job is
redelivered — so the work is never silently lost.

### CLI / HTTP
```bash
falcon queue push '{"job":"resize-image","file":"photo42.jpg","w":800}'
falcon queue pop                             # one worker gets one job

curl -X POST localhost:8080/queue -H 'content-type: application/json' \
     -d '{"value":"{\"job\":\"resize-image\",\"file\":\"photo42.jpg\"}"}'  # → {"ok":true}
curl localhost:8080/queue                    # → {"id":1,"value":"..."}  (204 if empty)
curl -X POST localhost:8080/queue/ack -H 'content-type: application/json' \
     -d '{"id":1}'                           # confirm after the job succeeds
```
### Binary wire
`PUSH` / `POP` / `ACK` over the pipelined TCP protocol on `:6380`.

---

## 2. How it's built

```
   push(job) ──▶ queue_<name>.log  (durable append log, jobs in push order)
                       │  offsets: 1  2  3  4  5 …
                       ▼
   pop(group) ──▶ ┌──────────── per-group state (in RAM) ────────────┐
                  │  cursor ─▶ next offset never delivered to group   │
                  │  in_flight: BTreeMap<offset, deadline>            │
                  └──────────────────────────────────────────────────┘
        step 1: any in_flight deadline ≤ now?  → redeliver it (retry timed-out work)
        step 2: else take log[cursor], advance cursor, mark in_flight   ── all under
   ack(group, off) ──▶ remove off from in_flight                          one group lock
```

One durable append log (the jobs, in push order) plus, **per consumer group**, a
small piece of in-memory state:

| Per-group state | Type | Role |
|-----------------|------|------|
| `cursor` | `Offset` | next offset never yet delivered to this group |
| `in_flight` | `BTreeMap<Offset, InFlight>` | delivered-but-unacked offsets and their redelivery deadlines |

### `pop(group)` — two-step selection (see `queue.rs`)
```
lock(groups)                                  # one mutex guards all group state
  1. redelivery: is there an in-flight offset whose deadline <= now?
        → re-arm its deadline, return it        (timed-out work retried first)
  2. otherwise: read the log at `cursor`, take the next record,
        advance cursor, mark it in-flight       (fresh work)
```
The whole of step 2 — cursor read, log read, cursor advance, in-flight insert —
runs **while holding the group lock**. `ack(group, offset)` simply removes the
offset from `in_flight`.

### Recovery
The log is durable; cursors and in-flight sets are in memory and rebuilt from the
log on restart (a restarted node re-delivers anything that wasn't durably acked —
consistent with at-least-once).

---

## 3. Why it's built this way — the reasoning

**Why one log + per-group cursor, instead of physically removing popped jobs?**
Removing from the middle of a log is expensive and destroys the very ordering and
replayability the append log gives you. A *cursor* per group means the log is
written once and read by each group independently — competing consumers in one
group share a cursor (work splits between them), while a second group with its own
cursor sees the whole stream. Same bytes, two delivery semantics, zero rewrites.

**Why is the entire fresh-delivery step under one lock?** This is the core
correctness property of a work queue: *no job may be handed to two consumers at
once.* If the lock were dropped around the log read (as an earlier version did),
two concurrent `pop`s for the same group could both observe the same `cursor`,
both select the same offset, and deliver the same job twice — silently breaking
work distribution. Holding the group lock across cursor read → select → advance →
mark-in-flight makes the reservation **atomic**. The log read is a pure read of an
append-only file and never touches group state, so holding the lock across it
cannot deadlock. This is covered by a concurrency regression test
(`competing_consumers_never_get_the_same_job`).

**Why at-least-once (ack + redelivery) rather than at-most-once?** A work queue's
failure mode must be "a job runs twice," never "a job is silently lost." Marking a
popped job in-flight with a deadline, and redelivering it if no `ack` arrives,
means a consumer that crashes mid-job doesn't drop the work — another consumer
picks it up after the timeout. The cost is that consumers must be idempotent or
dedupe by `offset`; that is the accepted, standard trade for not losing work.

**Why redeliver *before* fresh work in `pop`?** So a timed-out job is retried
promptly rather than starving behind a backlog of new jobs — retry latency stays
bounded by the ack timeout, not by queue depth.

---

## 4. Storage on disk
```
<data-dir>/queue/queue_<name>.log
```
Own `queue/` subdirectory, isolated from every other product.

## 5. Configuration
| Key (queue tuning) | Effect | Why |
|--------------------|--------|-----|
| `ack_timeout_secs` | how long a popped job may stay unacked before redelivery (default 30s) | shorter = faster retry on crash, but more spurious redelivery of slow-but-alive consumers |

## 6. Multi-region replication
The durable log ships to other regions like the other products (see the
[README](../README.md#multi-region-replication)).

## 7. Benchmarks

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS), `--release` + LTO**;
the run also asserts delivery and no-redelivery-after-ack. Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
falcon-bench --bench-all           # Queue row below
```

| Product | Concurrent peak | Per-op latency (sequential, durable) | Correctness verified |
|---------|----------------:|--------------------------------------|----------------------|
| **Falcon Queue** | ~4,280 ops/sec | p50 149 µs · p99 405 µs | 2000/2000 delivered; acked jobs not redelivered |

Pop/ack latency is sub-millisecond (the only product here that is) because
delivery is pure in-memory cursor bookkeeping — only `push` touches the disk
(one group-commit WAL append), whereas Pub/Sub and Stream fsync a durable log on
every op.

## 8. Guarantees
- **At-least-once** with ack + redelivery-on-timeout.
- **Work distribution:** competing consumers in one group never receive the same
  job simultaneously (concurrency regression test).
- **Independent groups:** each group sees the full stream.
- Verified by `falcon-bench` (2000/2000 delivered; acked jobs not redelivered).
