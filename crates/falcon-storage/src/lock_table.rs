use std::hash::{Hash, Hasher};
use tokio::sync::{Mutex, MutexGuard};

const SHARD_COUNT: usize = 1024;

/// Gives every key its own write queue without one lock per key existing
/// in memory: keys hash into a fixed set of shards, so writes to different
/// keys almost always proceed fully in parallel, while repeated writes to
/// the *same* key always land on the same shard and are serialized in
/// arrival order. Reads (`get`/`scan_prefix`) never take these locks —
/// DashMap/sled are already safely concurrent for reads.
pub struct KeyLockTable {
    shards: Vec<Mutex<()>>,
}

impl KeyLockTable {
    pub fn new() -> Self {
        let shards = (0..SHARD_COUNT).map(|_| Mutex::new(())).collect();
        Self { shards }
    }

    pub async fn lock(&self, key: &[u8]) -> MutexGuard<'_, ()> {
        self.shards[shard_index(key)].lock().await
    }
}

impl Default for KeyLockTable {
    fn default() -> Self {
        Self::new()
    }
}

fn shard_index(key: &[u8]) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % SHARD_COUNT
}
