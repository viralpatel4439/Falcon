use dashmap::DashMap;
use falcon_events::{now_millis, ChangeEvent, ChangeValue, EventBus, Hlc, HlcClock, Sequence};
use falcon_storage::{StorageEngine, StorageError, StorageTier, WarmEngine};
#[cfg(feature = "cold")]
use falcon_storage::{TierStats, TieredEngine};
use std::sync::Arc;

/// Ties a storage engine to its (optional) event bus and TTL tracking. The
/// event bus is only `Some` when subscriptions or replication are enabled
/// for this keyspace — see `Node::build`. This is the single place a write
/// becomes a `ChangeEvent`, so both WebSocket subscribers and replication
/// observe exactly the same stream — including TTL expiries, which go
/// through the same `delete` path so followers/subscribers stay consistent.
///
/// In multi-leader mode, writes are stamped with an HLC and applied through
/// the engine's last-write-wins path, and replicated events converge via
/// LWW — see `put`/`apply_replicated` below.
pub struct Keyspace {
    name: String,
    region: String,
    engine: Arc<dyn StorageEngine>,
    events: Option<EventBus>,
    /// key -> expiry time (unix millis). Only populated for keys written
    /// with a TTL; absent = never expires.
    expiry: DashMap<Vec<u8>, u128>,
    default_ttl_ms: u128,
    /// Set for multi-leader keyspaces: HLC clock + a concrete warm-engine
    /// handle for the LWW write path. `None` = single-leader (default).
    multi_leader: Option<MultiLeader>,
    /// Set on a NON-primary node in primary-queue mode: forwards a client
    /// write to the primary region (which commits it in one ordered queue and
    /// streams it back). `None` = write locally (primary node, or other modes).
    forwarder: std::sync::RwLock<Option<Arc<dyn WriteForwarder>>>,
}

struct MultiLeader {
    clock: HlcClock,
    warm: Arc<WarmEngine>,
}

/// A write forwarded to the primary in `primary-queue` mode. The forwarder
/// sends it over the replication channel and returns the committed sequence
/// the primary assigned. The committed change then arrives back on this node
/// via the normal replication stream (which is what actually mutates local
/// storage), so forwarding does NOT write locally.
#[async_trait::async_trait]
pub trait WriteForwarder: Send + Sync {
    async fn forward_put(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<Sequence, StorageError>;
    async fn forward_delete(&self, key: &[u8]) -> Result<Sequence, StorageError>;
}

impl Keyspace {
    pub fn new(
        name: String,
        region: String,
        engine: Arc<dyn StorageEngine>,
        events: Option<EventBus>,
        default_ttl_secs: u64,
    ) -> Self {
        Self {
            name,
            region,
            engine,
            events,
            expiry: DashMap::new(),
            default_ttl_ms: (default_ttl_secs as u128) * 1000,
            multi_leader: None,
            forwarder: std::sync::RwLock::new(None),
        }
    }

    /// Install the primary-queue forwarder (called by the replication layer on
    /// a non-primary node). Once set, client writes are forwarded to the
    /// primary instead of applied locally.
    pub fn set_forwarder(&self, forwarder: Arc<dyn WriteForwarder>) {
        *self.forwarder.write().unwrap() = Some(forwarder);
    }

    fn forwarder(&self) -> Option<Arc<dyn WriteForwarder>> {
        self.forwarder.read().unwrap().clone()
    }

    /// Enable multi-leader (active-active) writes: local writes are stamped
    /// with an HLC and replicated events converge via last-write-wins. Only
    /// valid for a warm-tier keyspace (validated at config load).
    pub fn with_multi_leader(mut self, region: String) -> Self {
        if let Ok(warm) = self.engine.clone().as_any_arc().downcast::<WarmEngine>() {
            self.multi_leader = Some(MultiLeader {
                clock: HlcClock::new(region),
                warm,
            });
        }
        self
    }

    pub fn is_multi_leader(&self) -> bool {
        self.multi_leader.is_some()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tier(&self) -> StorageTier {
        self.engine.tier()
    }

    pub fn engine(&self) -> &Arc<dyn StorageEngine> {
        &self.engine
    }

    pub fn events(&self) -> Option<&EventBus> {
        self.events.as_ref()
    }

    /// Tiering stats if this keyspace uses the tiered engine, else `None`.
    /// Only meaningful with the `cold` feature (the tiered tier); without it
    /// there is no tiered engine, so this is always `None`.
    #[cfg(feature = "cold")]
    pub fn tier_stats(&self) -> Option<TierStats> {
        if self.engine.tier() != StorageTier::Tiered {
            return None;
        }
        let any = self.engine.clone().as_any_arc();
        any.downcast::<TieredEngine>().ok().map(|e| e.stats())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // Lazy expiry: if this key has passed its TTL, delete it (emitting a
        // proper Delete event) and report a miss.
        if self.is_expired(key) {
            let _ = self.delete(key).await;
            return Ok(None);
        }
        self.engine.get(key).await
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.engine.scan_prefix(prefix).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<Sequence, StorageError> {
        self.put_with_ttl(key, value, None).await
    }

    /// Write a key with an optional per-write TTL (seconds). `None` uses the
    /// keyspace's `default_ttl_secs` (0 = never expires). A TTL of 0 clears
    /// any existing expiry for the key.
    pub async fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<Sequence, StorageError> {
        // Primary-queue (non-primary node): forward to the primary, which
        // commits in one ordered queue. The committed change comes back over
        // the replication stream and mutates local storage there — so we do
        // NOT write locally here, avoiding a lost/dropped concurrent write.
        if let Some(fwd) = self.forwarder() {
            return fwd.forward_put(key, value, ttl_secs).await;
        }
        // Multi-leader: stamp a fresh HLC and write via the LWW path so this
        // write carries an ordering that converges across regions.
        let (seq, hlc) = match &self.multi_leader {
            Some(ml) => {
                let hlc = ml.clock.now();
                let seq = ml.warm.put_lww(key, value, &hlc).await?;
                (seq, hlc)
            }
            None => (self.engine.put(key, value).await?, Hlc::zero()),
        };
        let ttl_ms = match ttl_secs {
            Some(s) => (s as u128) * 1000,
            None => self.default_ttl_ms,
        };
        if ttl_ms > 0 {
            self.expiry.insert(key.to_vec(), now_millis() + ttl_ms);
        } else {
            self.expiry.remove(key);
        }
        self.publish(key, ChangeValue::Put(value.to_vec()), seq, hlc);
        Ok(seq)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<Sequence, StorageError> {
        // Primary-queue (non-primary node): forward the delete to the primary.
        if let Some(fwd) = self.forwarder() {
            return fwd.forward_delete(key).await;
        }
        let (seq, hlc) = match &self.multi_leader {
            Some(ml) => {
                let hlc = ml.clock.now();
                let seq = ml.warm.delete_lww(key, &hlc).await?;
                (seq, hlc)
            }
            None => (self.engine.delete(key).await?, Hlc::zero()),
        };
        self.expiry.remove(key);
        self.publish(key, ChangeValue::Delete, seq, hlc);
        Ok(seq)
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        self.expiry
            .get(key)
            .map(|e| now_millis() >= *e.value())
            .unwrap_or(false)
    }

    /// Eagerly delete all keys whose TTL has passed. Called by the
    /// background reaper. Returns the number of keys reaped.
    pub async fn reap_expired(&self) -> usize {
        let now = now_millis();
        let expired: Vec<Vec<u8>> = self
            .expiry
            .iter()
            .filter(|e| now >= *e.value())
            .map(|e| e.key().clone())
            .collect();
        let mut reaped = 0;
        for key in expired {
            // Re-check under the delete path (another writer may have
            // refreshed the TTL in the meantime).
            if self.is_expired(&key) {
                let _ = self.delete(&key).await;
                reaped += 1;
            }
        }
        reaped
    }

    pub fn tracked_ttl_keys(&self) -> usize {
        self.expiry.len()
    }

    pub async fn apply_replicated(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        match &self.multi_leader {
            Some(ml) => {
                // Advance our HLC past what we've observed, then LWW-apply.
                // Only re-broadcast (to local subscribers + onward peers) if
                // it actually won — avoids re-propagating losing/duplicate
                // writes and gives a natural anti-entropy fixpoint.
                ml.clock.observe(&event.hlc);
                let applied = ml.warm.apply_lww(event).await?;
                if applied {
                    if let Some(bus) = &self.events {
                        bus.publish(event.clone());
                    }
                }
            }
            None => {
                self.engine.apply_replicated(event).await?;
                if let Some(bus) = &self.events {
                    bus.publish(event.clone());
                }
            }
        }
        Ok(())
    }

    /// Approximate durable byte size of this keyspace's engine.
    pub fn durable_bytes(&self) -> u64 {
        self.engine.durable_bytes()
    }

    /// Flush any buffered writes durably (graceful shutdown).
    pub async fn flush(&self) {
        if let Err(e) = self.engine.flush().await {
            tracing::warn!(keyspace = %self.name, error = %e, "flush failed");
        }
    }

    /// Compact this keyspace's durable log if supported. Returns whether it ran.
    pub async fn compact(&self) -> bool {
        match self.engine.compact().await {
            Ok(ran) => ran,
            Err(e) => {
                tracing::warn!(keyspace = %self.name, error = %e, "compaction failed");
                false
            }
        }
    }

    fn publish(&self, key: &[u8], value: ChangeValue, sequence: Sequence, hlc: Hlc) {
        if let Some(bus) = &self.events {
            bus.publish(ChangeEvent {
                keyspace: self.name.clone(),
                key: key.to_vec(),
                value,
                sequence,
                timestamp: now_millis(),
                origin_region: self.region.clone(),
                hlc,
            });
        }
    }
}
