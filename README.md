# Falcon

A fast, safe, fully-configurable data platform in Rust. Falcon bundles five
components behind one binary:

- **Falcon Cache** — low-latency RAM cache that spills to disk, with TTL
- **Falcon KV Store** — the durable key-value store (six pluggable storage
  tiers) with real-time WebSocket updates (write→notify)
- **Falcon Pub/Sub** — topics (ephemeral or durable)
- **Falcon Queue** — durable, at-least-once work queues
- **Falcon Event Stream** — partitioned, replayable event logs with durable
  consumer groups (Kafka-shaped)

…plus TTL, multi-region replication (single- and multi-leader), pluggable
storage, Prometheus metrics, WAL compaction, and graceful shutdown — over HTTP,
a lean binary protocol, and WebSockets. Everything optional is **off by default
and adds no overhead when unused**.

**Every subsystem is benchmarked for *fast · reliable · safe · durable*** with
`falcon-bench --bench-all` (see [Benchmarks](#benchmarks)) — each run asserts
correctness (no loss, ordering, survives restart) so a regression can't hide
behind a good number.

## Install only what you want

Falcon ships as five **installable products**. You pick the one you need; you
get its CLI verbs, its own web UI, and multi-region low-latency replication —
and nothing else.

| Product | Install | What you get |
|---------|---------|--------------|
| **Falcon Cache** | `falcon install cache` | low-latency RAM cache that spills to disk, with TTL |
| **Falcon KV Store** | `falcon install kv` | durable key-value store with real-time updates |
| **Falcon Pub/Sub** | `falcon install pubsub` | publish/subscribe topics |
| **Falcon Queue** | `falcon install queue` | durable work queues with competing consumers |
| **Falcon Event Stream** | `falcon install stream` | partitioned, replayable event logs |

```console
$ falcon install cache --region us-east-1 --replicate --peer 10.0.0.2:7070
Installed Falcon Cache at ~/.falcon/profile.toml
  low-latency RAM cache that spills to disk, with TTL
  replication: leader (1 peer(s))

Next:
  falcon serve                       # run this node
  open http://127.0.0.1:8080/        # the Falcon Cache UI
```

A node runs **only the product(s) in its profile**. On a cache node, the Cache
UI is served at `/`, the cache verbs (`get`/`put`/`del`) work, and every other
product's HTTP routes return `404` — the surface is genuinely scoped.

**Two ways to get a scoped build:**

- **One binary, gated by profile** (default `full` build): install a single
  product and the node scopes itself to it at runtime.
- **Per-product binary** (Cargo features): compile a slim binary that doesn't
  even contain the other products' code —
  ```console
  cargo build --release --no-default-features --features feat-cache
  ```
  A `feat-cache` binary refuses to install or run any other product.

**Dependency separation.** A slim build pulls only the heavy dependencies its
product actually needs — the separation is real at the crate level, not just at
runtime:

| Build | Pulls `sled` (disk tier) | Pulls S3 client (reqwest+SigV4) |
|-------|:---:|:---:|
| `feat-pubsub` / `feat-queue` / `feat-stream` | — | — |
| `feat-cache` / `feat-kv` | ✓ (warm/tiered tiers) | — |
| add `feat-s3` (or `full`) | ✓ | ✓ |

So a pub/sub-only binary compiles neither the on-disk storage engine nor the S3
client; add S3 to any build with `--features feat-s3`.

### Configuration: CLI and UI only — no environment variables

Falcon **never reads environment variables** for configuration. Every setting
lives in a single profile file (`~/.falcon/profile.toml`) written exclusively
through:

- the CLI — `falcon config set <key> <value>` / `falcon config get` / `falcon config list`
- the web UI — the config panel (`POST /config`, auth-gated), which writes the
  same profile file

```console
$ falcon config set http-bind 0.0.0.0:9090
$ falcon config set api-key s3cret
$ falcon config set peers 10.0.0.2:7070,10.0.0.3:7070   # multi-region peers
$ falcon status
```

`falcon serve` loads the profile and runs. `serve` flags (e.g. `--http-bind`)
may override a field for one run, but the profile is the durable source of
truth. Config changes made in the UI persist to the profile and take effect on
the next `serve`.

## What it does

- **Falcon KV Store** (key-value) with per-keyspace storage tiers:
  - `hot` — in-memory only (fastest, not durable)
  - `warm` — in-memory index + group-commit WAL (durable, **default**)
  - `cold` — disk-backed via sled (for datasets larger than RAM)
  - `tiered` — automatic hot/cold: hot working set in RAM, cold tail auto-spilled
    to disk (CLOCK eviction), so you can hold far more than RAM while keeping only
    the working set resident
  - `sharded` — **the object-store tier** (local disk today, a third-party
    bucket via the same `ObjectStore` trait tomorrow). Keys are **hashed into a
    fixed set of buckets**, each bucket stored as one object, so *N keys map to a
    fixed object count* no matter how large N gets (one object per *bucket*, not
    per key). O(1) point reads from an in-memory index; writes coalesce per
    bucket. Behaves identically on local disk and remote buckets.
- **Pub/Sub topics** — `ephemeral` (fast, at-most-once) or `durable` (persisted,
  replayable, survives restart), selectable per topic
- **Work queues** — durable, at-least-once, with ack + redelivery-on-timeout and
  competing consumers per group
- **Event streams** (Falcon Event Stream) — partitioned, offset-addressed,
  replayable logs (Kafka-shaped): records route to partitions by key hash
  (per-key total ordering), consumer groups keep durable per-partition offsets
  and resume after restart, and any group can replay history or live-tail
- **TTL** — per-write (`?ttl=<secs>`) or per-keyspace default; lazy + background
  reaper; expiries replicate and notify subscribers consistently
- **Replication** — single-leader (strong ordering, default) or multi-leader
  (active-active, HLC last-write-wins convergence), async over gRPC
- **Three protocols** — REST/JSON (`:8080`), a binary pipelined protocol (`:6380`),
  and WebSocket subscriptions
- **Optional auth** — a shared-secret token across all protocols, off by default
- **Safety** — `#![forbid(unsafe_code)]` on every crate (zero unsafe), fuzz-tested
  parsers, `cargo audit` clean, and a configurable **max value size** (anti-OOM)
- **Production-ready operations** (all on by default) — Prometheus `/metrics`,
  a `/readyz` readiness probe distinct from `/healthz` liveness, background **WAL
  compaction** (bounds disk + restart time), and **graceful shutdown** (SIGTERM
  drains in-flight requests and force-flushes buffered writes — zero-loss
  autoscaling/rollouts)
- **Inbuilt web console** — a self-contained dashboard served at `/` (no build
  step, embedded in the binary): live metrics, KV browser (get/put/delete/scan),
  topic/queue/stream status, and node health
- **Full CLI** — one `falcon` binary is both the server *and* the client:
  `falcon serve` runs a node; `falcon get/put/scan`, `falcon topic publish`,
  `falcon queue push/pop`, `falcon stream append/poll`, `falcon health/metrics`
  talk to a running node. Everything is configured via the CLI or web UI (never env vars).
- **Multi-core** — an explicit multi-threaded runtime (one worker per logical
  CPU by default, `--worker-threads N` to pin); every subsystem runs
  concurrently across all cores.

## Quickstart

Falcon is **install-first**: you choose a product, then run it. `falcon serve`
with no installed product will tell you to install one.

```bash
cargo build --release -p falcon-cli

# 1. Install a product (writes ~/.falcon/profile.toml). Try Falcon KV Store:
falcon install kv

# 2. Run the node (reads the profile). Dashboard at http://localhost:8080/
falcon serve
```

```bash
# …then, in another shell, use the same binary as a client:
falcon put foo bar
falcon get foo                   # → bar
falcon scan --prefix user:
falcon status                    # what's installed + current config
falcon health
```

```bash
# …or the same over plain HTTP
curl -X PUT localhost:8080/kv/foo -d 'bar'
curl localhost:8080/kv/foo
curl -X PUT 'localhost:8080/kv/session?ttl=60' -d 'expires in 60s'
curl -X DELETE localhost:8080/kv/foo
curl 'localhost:8080/kv?prefix=user:'
curl localhost:8080/health           # shows the active products + feature set
```

> Want a different product? Swap `kv` for `cache`, `pubsub`, `queue`, or
> `stream`. Each serves its **own UI** at `/` and exposes only its own routes.
> See [Using each product](#using-each-product) below for the exact commands.

## Using each product

Every product follows the **same three steps**: `install` → `serve` → use it
(CLI, HTTP, or its UI at `http://localhost:8080/`). Below is the exact recipe
for each. All configuration is via `falcon install ... <flags>` or
`falcon config set <key> <value>` — never environment variables.

### 1. Falcon Cache — low-latency cache with TTL

A RAM-first cache (tiered engine) that spills the cold tail to disk, so it holds
far more than RAM while serving the hot set fast. Keys can expire with a TTL.

```bash
falcon install cache
falcon serve
```
```bash
# CLI (cache keyspace is named "cache")
falcon put session:42 '{"user":7}' --keyspace cache --ttl 300   # expires in 5 min
falcon get session:42 --keyspace cache
falcon del session:42 --keyspace cache

# HTTP
curl -X PUT 'localhost:8080/keyspaces/cache/kv/session:42?ttl=300' -d 'value'
curl localhost:8080/keyspaces/cache/kv/session:42
```
**UI** shows hit-rate, hot keys/bytes, evictions, and TTL-tracked keys, with a
set/get/delete panel. Use it for sessions, rate-limit counters, hot lookups.

### 2. Falcon KV Store — durable key-value with real-time updates

A durable store (warm tier: in-memory index + group-commit WAL) with live
WebSocket updates: subscribe to a key prefix and get every write pushed to you.

```bash
falcon install kv
falcon serve
```
```bash
# CLI (default keyspace is "default")
falcon put user:7 'Alice'
falcon get user:7
falcon scan --prefix user:

# HTTP
curl -X PUT localhost:8080/kv/user:7 -d 'Alice'
curl 'localhost:8080/kv?prefix=user:'

# Real-time updates over WebSocket (browser or wscat)
#   ws://localhost:8080/subscribe?keyspace=default&prefix=user:
```
**UI** has a read/write/scan panel plus a **live subscription** box that streams
writes as they happen. Use it as your primary database with change-feeds.

### 3. Falcon Pub/Sub — topics (fan-out)

Publish a message to a topic; every live subscriber gets it (at-most-once for
ephemeral topics, or persisted/replayable for durable ones).

```bash
falcon install pubsub
falcon serve
```
```bash
# CLI
falcon topic publish events 'hello everyone'

# HTTP
curl -X POST localhost:8080/topics/events/publish -d 'hello'

# Subscribe (WebSocket):  ws://localhost:8080/subscribe?topic=events
```
**UI** lists topics, has a publish box, and a **live subscription** viewer. Use
it for broadcast notifications, cache-invalidation fan-out, chat.

### 4. Falcon Queue — durable work queues

Push jobs; competing consumers in a group each pop different jobs (at-least-once
with ack + redelivery-on-timeout). Work is distributed, not broadcast.

```bash
falcon install queue
falcon serve
```
```bash
# CLI
falcon queue push jobs 'resize-image:42'
falcon queue pop jobs --group workers        # one consumer gets one job

# HTTP
curl -X POST localhost:8080/queues/jobs/push -d 'resize-image:42'
curl -X POST 'localhost:8080/queues/jobs/pop?group=workers'
```
**UI** lists queues, with push and pop (auto-ack) panels. Use it for background
jobs, task distribution across workers.

### 5. Falcon Event Stream — partitioned, replayable logs

Kafka-shaped: records route to partitions by key (same key → same partition →
totally ordered). Consumer groups keep a durable offset per partition and resume
where they left off; any group can replay history independently.

```bash
falcon install stream
falcon serve
```
```bash
# CLI
falcon stream append clicks 'click:home' --key user:7    # key sets the partition
falcon stream poll clicks --partition 0 --group analytics

# HTTP
curl -X POST 'localhost:8080/streams/clicks/records?key=user:7' -d 'click:home'
curl 'localhost:8080/streams/clicks/poll?group=analytics&partition=0'
```
**UI** lists streams, with append and poll panels. Use it for event sourcing,
analytics pipelines, audit logs — anything needing ordered, replayable history.

### Adding multi-region replication to any product

Replication is a cross-cutting layer — the same flags work for every product:

```bash
# Leader in us-east, telling it about its peer
falcon install kv --region us-east-1 --replicate --role leader --peer 10.0.0.2:7070
falcon serve

# Follower in eu-west, pointed at the leader
falcon install kv --region eu-west-1 --replicate --role follower \
      --leader-addr http://10.0.0.1:7070
falcon serve
```

Or set it after install: `falcon config set replicate true`,
`falcon config set peers 10.0.0.2:7070,10.0.0.3:7070`. Writes on the leader ship
to followers over gRPC (`:7070`); with `--role leader` on every node and peers
listed, it runs active-active (multi-leader, HLC last-write-wins).

## Web console

Open **`http://localhost:8080/`** in a browser. The dashboard is a single
self-contained page **embedded in the binary** (no build step, no external
assets, works offline) and auto-refreshes every 2s:

- Live metric tiles (ops, GET hit-rate, WAL size, connections, replication lag)
- Which products are active (Falcon KV Store, Queue, Pub/Sub, Event Stream,
  real-time updates, Replication)
- **KV browser** — scan by prefix, put, and delete keys per keyspace
- Topic / queue / stream listings, a quick publish box, and per-keyspace status

If the node has auth enabled, the console prompts for the API key (kept in the
browser's local storage) and sends it on data calls.

## CLI

The one `falcon` binary is both the **server** and a **client**.

```bash
# 1. Install the product you want (writes ~/.falcon/profile.toml)
falcon install kv --region us-east-1

# 2. Run the node (reads the profile; --http-bind etc. override for one run)
falcon serve --worker-threads 8          # multi-core; omit = one per CPU

# Client (talks to a running node; --addr selects it, default 127.0.0.1:8080)
falcon get <key>                         falcon put <key> <value> [--ttl 60]
falcon del <key>                         falcon scan --prefix user:
falcon topic publish events 'hi'         falcon queue push jobs 'work'
falcon queue pop jobs --group g1         falcon stream append clicks 'e' --key u1
falcon stream poll clicks --partition 0 --group g1
falcon health                            falcon metrics
```

Client verbs are scoped to the installed product(s): a `feat-cache` build
exposes the KV verbs but errors on `falcon topic ...`. Values omitted on the
command line are read from **stdin** (e.g. `echo hi | falcon put k`).

Run `falcon --help` or `falcon <command> --help` for every flag.

## Config

Falcon is configured through the **CLI and web UI only** — never environment
variables. All settings live in `~/.falcon/profile.toml`, written by:

```bash
falcon install <feature> [--region .. --http-bind .. --api-key .. --replicate --peer ..]
falcon config set <key> <value>          # e.g. falcon config set http-bind 0.0.0.0:9090
falcon config get <key>
falcon config list                       # every key and its current value
falcon status                            # installed products + build + settings
```

### Config reference — every key and what it does

| Key | Example | What it controls |
|-----|---------|------------------|
| `node.id` | `leader-us-east` | This node's unique id (identity in replication + logs). |
| `region` | `us-east-1` | The node's region label (display + replication routing / HLC tiebreak). |
| `http-bind` | `0.0.0.0:8080` | Address for the REST API, WebSocket, and the product **UI**. |
| `wire-bind` | `0.0.0.0:6380` | Address for the fast binary protocol (only used if `wire-enabled`). |
| `wire-enabled` | `true` | Turn the binary protocol server on/off. Off by default. |
| `api-key` | `s3cret` | Shared secret required on **every** connection. Empty = auth off. |
| `data-dir` | `/data` | Where durable data lives (WAL, stream/queue logs, shard objects). |
| `log-level` | `info` | Log verbosity (`error`/`warn`/`info`/`debug`/`trace`). |
| `replication.enabled` (alias `replicate`) | `true` | Turn multi-region replication on. Off by default. |
| `replication.role` | `leader` / `follower` | `leader` accepts writes and ships them; `follower` replays a leader's log. |
| `grpc-bind` | `0.0.0.0:7070` | Address the replication gRPC server listens on. |
| `leader-addr` | `http://10.0.0.1:7070` | The leader to follow (**required** when `role = follower`). |
| `peers` | `10.0.0.2:7070,10.0.0.3:7070` | Peer nodes for multi-region replication (or use `falcon peers add/remove`). |
| `storage.backend` | `local` / `s3` | Backing store for the object-store tier: local disk or S3-compatible. |
| `s3-url` | `https://s3.amazonaws.com` | S3-compatible endpoint URL (any provider). |
| `s3-region` | `us-east-1` | S3 region label (`auto` for Cloudflare R2). |
| `s3-bucket` | `my-bucket` | S3 bucket name. |
| `s3-access-key` | `AKIA...` | S3 access key id. |
| `s3-secret-key` | `...` | S3 secret access key (masked in the UI). |
| `s3-prefix` | `falcon` | Optional object-name prefix so keyspaces can share a bucket. |

Set any of them with `falcon config set <key> <value>`; read one back with
`falcon config get <key>`; see them all with `falcon config list`. The web UI's
config panel writes the same keys. Product-specific tuning (storage tiers, TTLs,
partition counts, topic durability, etc.) is documented in
[`config/default.toml`](config/default.toml).

`falcon serve` loads the profile; its flags (`--http-bind`, `--wire-bind`,
`--node-id`, `--region`, `--data-dir`, `--worker-threads`, `--log-level`)
override the profile for a single run. Order: **profile < serve flags**. The web
UI's config panel writes the same profile file (`POST /config`, auth-gated).

`config/default.toml` documents the full internal engine options for reference.

## Storage: local disk or third-party object storage

By default, data lives on **local disk** at `data-dir` (`./data`) — mount a
Docker volume there and it works. Set it with `falcon config set data-dir /path`.

### Attaching third-party object storage (S3-compatible)

Falcon can store data in **any S3-compatible object store** — AWS S3, MinIO,
Cloudflare R2, Backblaze B2, Wasabi, DigitalOcean Spaces, Ceph, self-hosted
gateways. You point it at an endpoint URL and give it credentials; there is no
provider-specific code. This uses the `sharded` object-store tier under the
hood (keys hash into a fixed set of bucket objects, with O(1) in-memory reads).

Attach at install time:

```bash
falcon install cache \
  --storage s3 \
  --s3-url https://s3.amazonaws.com \
  --s3-region us-east-1 \
  --s3-bucket my-falcon-bucket \
  --s3-access-key AKIA... \
  --s3-secret-key ...
falcon serve
```

Or configure it later (persists to the profile; the secret is masked in the UI):

```bash
falcon config set storage.backend s3
falcon config set s3-url http://localhost:9000        # e.g. MinIO
falcon config set s3-bucket my-bucket
falcon config set s3-access-key ...
falcon config set s3-secret-key ...
falcon config set s3-region auto                        # 'auto' for Cloudflare R2
```

Provider endpoint examples:

| Provider | `--s3-url` | `--s3-region` |
|----------|-----------|---------------|
| AWS S3 | `https://s3.<region>.amazonaws.com` | your region |
| MinIO (self-hosted) | `http://<host>:9000` | `us-east-1` |
| Cloudflare R2 | `https://<account>.r2.cloudflarestorage.com` | `auto` |
| Backblaze B2 | `https://s3.<region>.backblazeb2.com` | your region |

> The S3 backend is compiled into the **full** build and any build with the
> `feat-s3` cargo feature. Multiple keyspaces can share one bucket — each gets
> its own object-name prefix (override with `s3-prefix`).

## Binary protocol (`:6380`)

A length-prefixed TCP protocol with pipelining, for high throughput. Ops: `GET`,
`SET`, `DEL`, `PING`, `PUBLISH`, `SUBSCRIBE`, `PUSH`, `POP`, `ACK`,
`STREAM_APPEND`, `AUTH`. Send many requests back-to-back and read replies in
order (no per-op round-trip). See `crates/falcon-wire/src/protocol.rs` for the
frame layout.

## Concurrency

Every subsystem — KV, pub/sub, queues, event streams, realtime DB, and
replication — serves requests with **true concurrency across all CPU cores**,
verified by the `--bench-all` suite (all numbers there are multi-connection):

- **Multi-core runtime.** `falcon serve` runs an explicit multi-threaded Tokio
  runtime with one worker thread per logical CPU by default (`--worker-threads
  N` to pin). Work is scheduled across every core.
- **Connections run in parallel.** Each binary-protocol connection is its own
  Tokio task; the HTTP/WebSocket server (axum) handles every request
  concurrently. Thousands of clients make progress at once — nothing funnels
  through a global lock.
- **Storage is sharded, not globally locked.** Reads hit a concurrent `DashMap`
  (or sled) with no lock; writes take only a *per-key* lock (a 1024-way sharded
  lock table), so writes to different keys proceed fully in parallel.
- **Durability batches instead of serializing.** The KV WAL and the messaging
  log both use **group commit** — a background writer coalesces many concurrent
  writes into one fsync — so durable throughput *scales with* concurrency
  instead of pinning at one-fsync-at-a-time. Messaging fsyncs run on the
  blocking pool so a slow disk never stalls the async workers.
- **Ordering is preserved under concurrency.** Sequence assignment and the WAL
  append are atomic, so the replication log a follower streams is strictly
  ordered even when unrelated keys are written in parallel (regression-tested).
- **Streams parallelize by partition.** Different partitions are independent
  ordering domains written in parallel; a single key stays totally ordered.

The one deliberate exception: ops **pipelined on a single connection** are
dispatched in arrival order (Redis-like per-connection ordering). Concurrency
comes from using multiple connections — which real clients and the benchmark do.

## Pub/Sub and queues

Configure topics/queues (see `config/default.toml`):

```toml
[[topic]]
name = "events"
mode = "durable"        # or "ephemeral"

[[queue]]
name = "jobs"
ack_timeout_secs = 30
```

- **Topics**: `PUBLISH` to a topic; `SUBSCRIBE` turns a wire connection into a live
  push stream. Durable topics also persist and can be replayed from an offset.
- **Queues**: `PUSH` a job; `POP` (per consumer group) delivers it and starts an ack
  timer; `ACK` confirms it; unacked jobs are redelivered after the timeout.
  Multiple consumers in a group share the work; different groups each get the full
  stream.

## Event streaming (Falcon Event Stream)

A **stream** is the Kafka-shaped sibling of a topic: **partitioned**, durable,
replayable, with **per-consumer-group committed offsets**. Records route to a
partition by key hash (same key ⇒ same partition ⇒ **ordered**); each partition
is its own durable log; every consumer group keeps a durable offset *per
partition* and resumes there after a restart (**at-least-once**). Different
groups each see the full stream independently.

Configure streams (see `config/default.toml`):

```toml
[[stream]]
name = "user-events"
partitions = 8          # ordering domains / parallelism; a key is ordered within its partition
capacity = 1024         # live-tail broadcast buffer per partition
```

Drive it over **REST** — append (producer), poll + commit (consumer group):

```bash
# Append a record; ?key= picks the partition. Returns {partition, offset}.
curl -X POST 'localhost:8080/streams/user-events/records?key=user:42' -d 'signed_up'

# Poll a partition for a consumer group (records AFTER its committed offset).
curl 'localhost:8080/streams/user-events/poll?group=analytics&partition=3'

# After processing, durably commit progress (this is the at-least-once boundary).
curl -X POST 'localhost:8080/streams/user-events/commit?group=analytics&partition=3&offset=57'

# Stream metadata (partition count).
curl localhost:8080/streams/user-events
```

The high-throughput **producer path is also on the binary wire protocol**
(`STREAM_APPEND`, op `0x20`): keyspace = stream name, key = partition key,
value = payload; the reply carries `partition(4B) + offset(8B)`. Consumer
poll/commit are request/response and use REST.

**Topic vs. queue vs. stream:** topic = simple fan-out; queue = work
distribution with acks; **stream = ordered-by-key, replayable, partitioned
history with independent consumer groups.**

## Real-time subscriptions (WebSocket)

Off by default. Enable per keyspace (`subscriptions = true`), then connect to
`ws://localhost:8080/subscribe` and send:

```json
{"type":"subscribe","id":"sub1","keyspace":"default","key":"foo"}
```

(or `"prefix"` instead of `"key"`). You get `update` pushes on change.

## Multi-region replication

Every product can replicate across regions. Nodes talk to each other over a
dedicated **gRPC channel** — each node binds `grpc-bind` (default `:7070`) and
the other nodes dial it. Two topologies:

### How nodes connect

```
         region: us-east-1                 region: eu-west-1
      ┌───────────────────────┐         ┌───────────────────────┐
      │  leader                │        │  follower              │
      │  http  :8080           │        │  http  :8080           │
      │  grpc  :7070  ◀────────┼────────┼─ dials leader :7070    │
      │  (accepts writes)      │  gRPC  │  (replays leader's log)│
      └───────────────────────┘  async └───────────────────────┘
```

- The **leader** accepts writes and streams its change log to followers.
- A **follower** dials the leader's `grpc-bind` address (`leader-addr`) and
  applies the stream. It stays *live* but *not ready* until caught up.
- If auth is on, the same `api-key` secures the gRPC channel automatically —
  set the **same key on every node**.

### Single-leader (strong ordering — default)

```bash
# Leader (us-east). Expose :7070 to the other region(s).
falcon install kv --node-id us --region us-east-1 --replicate --role leader
falcon serve

# Follower (eu-west), dialing the leader's gRPC address.
falcon install kv --node-id eu --region eu-west-1 --replicate --role follower \
      --leader-addr http://<leader-host>:7070
falcon serve
```

### Multi-leader (active-active)

Every node is a leader and lists the others as peers; writes converge via
Hybrid Logical Clock **last-write-wins** (eventual consistency — concurrent
same-key writes resolve deterministically, one wins, no merge).

```bash
# On each node: role=leader, and add every OTHER node as a peer.
falcon install kv --node-id us --region us-east-1 --replicate --role leader
falcon peers add http://eu-host:7070
falcon peers add http://ap-host:7070
falcon serve
```

Manage the peer set any time (persists to the profile, takes effect on next
`serve`):

```bash
falcon peers add http://<host>:7070      # also turns replication on
falcon peers remove http://<host>:7070
falcon peers list                        # peers + role + grpc bind
```

> **Ports to open between regions:** `7070/tcp` (gRPC replication). `8080` (HTTP)
> and `6380` (wire) are for clients, not node-to-node traffic.

## API key (optional auth)

Off by default. Set it with `falcon config set api-key "..."` (or
`falcon install <feature> --api-key ...`, or the UI config panel) and
**every connection must present it** — all client protocols *and* container-to-container
replication:

- **HTTP / REST**: `Authorization: Bearer <key>` header, or `?api_key=<key>` query param
  (`/healthz` is exempt for liveness probes)
- **WebSocket** (`/subscribe`): `?api_key=<key>` — browsers can't set handshake headers
- **Binary wire protocol**: an `AUTH` frame with the key, first, before any other op
  (all KV, pub/sub, and queue ops are gated)
- **gRPC replication** (node-to-node / container-to-container): `authorization` metadata

The key is compared in constant time. When unset, auth is fully off with zero overhead.

> **Security note:** the `?api_key=` query form is only as safe as the transport — put
> the deployment behind TLS so the URL isn't sniffable, and note URLs can appear in
> proxy/access logs. Prefer the `Authorization` header wherever the client can set it;
> the query param exists for browser WebSocket clients that can't.

## Operations, metrics & autoscaling

Falcon is built to run as a single autoscalable container. Everything here is
**on by default** with production-safe values, tuned via `falcon config set`
(or the UI) — never environment variables.

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | **Liveness** — 200 while the process is up (restart signal). Unauthenticated. |
| `GET /readyz` | **Readiness** — 200 only once startup finished (503 otherwise). Route traffic on this; a catching-up follower stays *live* but *not ready*. Unauthenticated. |
| `GET /metrics` | **Prometheus** text metrics: op counts, latency histograms, hit-rate, WAL bytes, replication lag, connections. The signal HPA/KEDA scale on. Unauthenticated. |

- **Graceful shutdown** — on `SIGTERM` (k8s/docker stop) or Ctrl-C, Falcon stops
  accepting, drains in-flight requests, then force-flushes every buffered write
  (the sharded store's coalesce window, interval-fsync WAL) before exiting.
  Bounded by `[ops] shutdown_grace_secs`. **Zero-loss rollouts/autoscaling.**
- **WAL compaction** — a background task rewrites each warm-tier WAL as a live-key
  snapshot once it passes `[ops] compaction_min_bytes`, so disk usage and
  restart-replay time stay bounded no matter how long the container lives.
- **Anti-OOM** — `[storage] max_value_bytes` (default 64 MiB) rejects an oversized
  PUT with `413` before it can exhaust memory.

Example autoscale signals from `/metrics`: `rate(falcon_kv_put_total[1m])`
(throughput), `falcon_kv_put_latency_seconds` (tail latency),
`falcon_replication_lag_sequences` (follower lag).

## Docker

```bash
docker build -f docker/Dockerfile -t falcon .
docker run -p 8080:8080 -p 6380:6380 -v falcondata:/data falcon
docker compose -f docker/docker-compose.yml up   # 2-region cluster
```

## Benchmarks

Measured with the bundled `falcon-bench` load tester (`--release`, LTO) on a
development Mac (Apple Silicon, APFS). **These are real numbers from this repo —
reproduce them with the commands below.** A Linux server with
power-loss-protected NVMe does materially better on the write path (fsync is the
bottleneck there).

**Read path — Redis-class.** Reads are served from the in-memory index; the
binary wire protocol pipelines many ops per round-trip.

| Path | Throughput | p50 | p99 | p99.9 |
|------|-----------:|----:|----:|------:|
| Wire GET, pipeline d=128 | **5.6 M ops/sec** | 152 µs | 341 µs | — |
| Wire GET, pipeline d=16 | 1.85 M ops/sec | 63 µs | 140 µs | — |
| Wire GET, d=1 (no pipeline) | 176 K ops/sec | 42 µs | 104 µs | — |
| Sustained read load (64 conns) | **3.0 M ops/sec** | 328 µs | 615 µs | 723 µs |
| HTTP GET (JSON, 1 req/op) | 61 K ops/sec | 79 µs | 197 µs | — |

**Write path — a durability dial you control.** Every write goes through the
group-commit WAL. With `fsync`-every-write (the default, *zero acked-write
loss*) throughput is bound by disk fsync latency; `interval_fsync_ms` trades a
small bounded loss window for a ~400× throughput jump.

| Write mode | Throughput | p50 | p99 |
|------------|-----------:|----:|----:|
| `fsync` every write (max durability) | ~1 K ops/sec | 7 ms | 11 ms |
| `interval_fsync_ms = 10` (≤10 ms loss window) | **397 K ops/sec** | 1 ms | 5 ms |

**Stability:** every sustained load test reported `STABLE (no latency cliff /
queue buildup)` — throughput held flat under 64-connection saturation with no
tail runaway.

### Every subsystem (`--bench-all`)

`falcon-bench --bench-all` spawns real servers and benchmarks **every**
component, and each run **asserts correctness on four axes — a failed check
aborts the run**, so these numbers can't hide a regression:

- **FAST** — throughput / latency
- **RELIABLE** — stable under load; a slow consumer never loses or reorders
- **SAFE** — no errors; every accepted op accounted for
- **DURABLE** — data survives a hard process restart (verified by killing the
  server and re-reading)

#### Per-product results (measured)

Measured on an **Apple M5 (10 cores, 16 GB, macOS 26, APFS)** with
`falcon-bench --bench-all --bench-records 5000` (`--release`, LTO). All numbers
are **concurrent** (many connections/writers at once), not single-threaded, so
they reflect real capacity. Every row also passed its correctness checks —
reproduce with the command shown. Fsync-bound write paths go materially faster
on a Linux server with power-loss-protected NVMe.

**Falcon Cache** — the tiered tier serves reads from RAM; see the read-path
table above for hot-key latency (Redis-class, µs-scale on the wire protocol).

**Falcon KV Store** (durable KV, warm tier)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~2,300 ops/sec |
| Per-op latency (sequential, durable) | p50 3.1 ms · p99 6.0 ms · max 38.5 ms |
| Durability | 5000/5000 survived a hard restart |

**Falcon Pub/Sub** (durable topic)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~4,340 ops/sec |
| Correctness | ordered append log; 5000/5000 published; survives restart |

**Falcon Queue** (durable, at-least-once)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~4,350 ops/sec |
| Per-op latency (sequential, durable) | p50 347 µs · p99 536 µs · max 2.1 ms |
| Correctness | 5000/5000 delivered, acked jobs not redelivered, survives restart |

**Falcon Event Stream** (1 partition)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~4,520 ops/sec |
| Correctness | per-key ordered, no loss; resumes exactly at committed offset |

**Falcon KV Store — real-time updates** (32 concurrent WebSocket subscribers)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~2,150 ops/sec |
| Notify latency (sequential) | p50 4.0 ms · p99 4.2 ms · max 7.9 ms |
| Correctness | 640/640 writes notified; no drops/duplicates |

**Multi-region replication** (leader→follower, 16 concurrent writers)

| Metric | Result |
|--------|-------:|
| Concurrent peak throughput | ~910 ops/sec |
| Cross-region convergence | full batch converged in ~1.5 s (async gRPC) |
| Correctness | 496/496 converged, none lost; follower matches leader |

*Replication is async by design; the convergence figure is when the whole
concurrent batch appears on the follower, not per-write latency.*

> **A real bug this caught.** Writing the concurrent multi-region benchmark
> surfaced a genuine data-loss bug: under a burst of concurrent writes to
> *different* keys, sequence allocation and the WAL append weren't atomic, so
> the on-disk log order didn't match sequence order — which stranded a
> follower's log catch-up and silently dropped writes. Fixed by making
> sequence-assign + WAL-enqueue atomic (file order == sequence order) and adding
> a safety re-read poll to the leader's replication stream. Regression-tested in
> `crates/falcon-storage/tests/replog_ordering.rs`.

Two engine-level fixes got messaging here (durable pub/sub, queue, and stream
appends were previously fsync-bound at **~270 ops/sec — a ~16× improvement**):

1. **Group commit for the message log** — a background writer drains all queued
   appends and does one fsync per batch (the same design as the KV WAL).
2. **Non-blocking dispatch** — messaging fsyncs run on the blocking pool so a
   slow disk never stalls the async workers driving other connections.

**Stream partitions are an ordering-parallelism-vs-throughput dial:** on a
single disk each partition fsyncs independently, so fewer partitions = higher
single-node throughput (1 partition ≈ 4,400/sec; 8 partitions ≈ 2,300/sec) —
the same property Kafka has. Default is 1; raise it for parallel ordering
domains, and use a stream's `interval_fsync_ms` to trade a bounded loss window
for throughput at higher partition counts.

Reproduce:

```bash
cargo build --release -p falcon-cli -p falcon-bench
# Every subsystem, with fast/reliable/safe/durable checks:
./target/release/falcon-bench --bench-all
# KV throughput + latency percentiles (HTTP baseline + pipelined wire):
./target/release/falcon-bench --pipeline-depths 1,16,128
# Sustained read load:
./target/release/falcon-bench --load-test --load-secs 15 --load-conns 64 --load-write-ratio 0.0
# Write path with interval fsync:
./target/release/falcon-bench --load-test --load-secs 12 --load-write-ratio 1.0 --load-interval-fsync-ms 10
```

## Safety & durability

- **Zero unsafe code**, compiler-enforced (`#![forbid(unsafe_code)]` everywhere).
- **Durable writes** via group-commit WAL (batched fsync); crash recovery tested.
  Per-keyspace `interval_fsync_ms` trades a bounded crash-loss window for lower write
  latency when you want it.
- `cargo audit` clean (no known vulnerabilities).

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo audit

# Sustained load test (tail latency under load)
cargo build --release -p falcon-cli -p falcon-bench
./target/release/falcon-bench --load-test --load-secs 15 --load-conns 64 \
  --load-write-ratio 0.5 --key-count 1000
```
