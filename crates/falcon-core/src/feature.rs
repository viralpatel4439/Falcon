//! The Falcon product model.
//!
//! Falcon ships as five installable products that each expose exactly one
//! subsystem. A user installs only what they want:
//!
//! - `cache` — in-RAM working set that spills to disk (tiered tier), with TTL.
//! - `kv` — durable key-value store (warm tier) with real-time WebSocket updates.
//! - `pubsub` — publish/subscribe topics.
//! - `queue` — durable work queues with competing consumers.
//! - `stream` — partitioned, replayable event logs.
//!
//! Every product supports multi-region low-latency replication — that is a
//! cross-cutting layer, not tied to any single feature.
//!
//! A `Feature` has two lives:
//!
//! 1. **Compile time** — each product is a Cargo feature (`feat-cache`, …) on
//!    the `falcon` binary. A cache-only build does not compile the messaging or
//!    durable-KV code at all. The `full` build (default) compiles everything.
//!    [`Feature::compiled_in`] reports what the running binary actually built.
//!
//! 2. **Runtime** — the installer records which product(s) a node runs in its
//!    profile file. A `full` binary can therefore still be scoped to a single
//!    product by its profile, and the API/UI/CLI gate on the active set.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// One installable Falcon product.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Feature {
    Cache,
    Kv,
    Pubsub,
    Queue,
    Stream,
}

impl Feature {
    /// Every product, in canonical order.
    pub const ALL: [Feature; 5] = [
        Feature::Cache,
        Feature::Kv,
        Feature::Pubsub,
        Feature::Queue,
        Feature::Stream,
    ];

    /// The lowercase CLI/profile name (`cache`, `kv`, …).
    pub fn as_str(self) -> &'static str {
        match self {
            Feature::Cache => "cache",
            Feature::Kv => "kv",
            Feature::Pubsub => "pubsub",
            Feature::Queue => "queue",
            Feature::Stream => "stream",
        }
    }

    /// The human product name for UIs and banners. These are the ONE canonical
    /// set of names — every UI, CLI message, and doc uses exactly these.
    pub fn product_name(self) -> &'static str {
        match self {
            Feature::Cache => "Falcon Cache",
            Feature::Kv => "Falcon KV Store",
            Feature::Pubsub => "Falcon Pub/Sub",
            Feature::Queue => "Falcon Queue",
            Feature::Stream => "Falcon Event Stream",
        }
    }

    /// A one-line description shown by `falcon install --help` / `status`.
    pub fn tagline(self) -> &'static str {
        match self {
            Feature::Cache => "low-latency RAM cache that spills to disk, with TTL",
            Feature::Kv => "durable key-value store with real-time updates",
            Feature::Pubsub => "publish/subscribe topics",
            Feature::Queue => "durable work queues with competing consumers",
            Feature::Stream => "partitioned, replayable event logs",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error parsing a feature name from the CLI or a profile file.
#[derive(Debug, thiserror::Error)]
#[error("unknown feature '{0}' (expected one of: cache, kv, pubsub, queue, stream)")]
pub struct ParseFeatureError(String);

impl FromStr for Feature {
    type Err = ParseFeatureError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cache" => Ok(Feature::Cache),
            "kv" | "kvstore" | "kv-store" => Ok(Feature::Kv),
            "pubsub" | "pub-sub" | "pub/sub" => Ok(Feature::Pubsub),
            "queue" => Ok(Feature::Queue),
            "stream" | "streaming" | "streams" => Ok(Feature::Stream),
            other => Err(ParseFeatureError(other.to_string())),
        }
    }
}

/// A set of features, order-preserving and deduplicated. Used both for the set
/// a binary compiled in and the set a node's profile activates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeatureSet(Vec<Feature>);

impl FeatureSet {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// The full set (every product) — what a `full` binary compiles in.
    pub fn all() -> Self {
        Self(Feature::ALL.to_vec())
    }

    pub fn contains(&self, f: Feature) -> bool {
        self.0.contains(&f)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Add a feature; no-op if already present. Returns whether it was new.
    pub fn insert(&mut self, f: Feature) -> bool {
        if self.contains(f) {
            false
        } else {
            self.0.push(f);
            true
        }
    }

    /// Remove a feature; returns whether it was present.
    pub fn remove(&mut self, f: Feature) -> bool {
        if let Some(i) = self.0.iter().position(|&x| x == f) {
            self.0.remove(i);
            true
        } else {
            false
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Feature> + '_ {
        self.0.iter().copied()
    }

    /// Every active feature must be present in `compiled` — otherwise the
    /// profile asks for a product this binary does not contain. Returns the
    /// first such feature, if any.
    pub fn first_uncompiled(&self, compiled: &FeatureSet) -> Option<Feature> {
        self.0.iter().copied().find(|&f| !compiled.contains(f))
    }
}

impl FromIterator<Feature> for FeatureSet {
    fn from_iter<I: IntoIterator<Item = Feature>>(iter: I) -> Self {
        let mut set = FeatureSet::new();
        for f in iter {
            set.insert(f);
        }
        set
    }
}

impl fmt::Display for FeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|x| x.as_str()).collect();
        f.write_str(&names.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roundtrip_and_aliases() {
        for f in Feature::ALL {
            assert_eq!(f.as_str().parse::<Feature>().unwrap(), f);
        }
        assert_eq!("KV-Store".parse::<Feature>().unwrap(), Feature::Kv);
        assert_eq!("pub/sub".parse::<Feature>().unwrap(), Feature::Pubsub);
        assert!("nope".parse::<Feature>().is_err());
    }

    #[test]
    fn set_insert_remove_dedup() {
        let mut s = FeatureSet::new();
        assert!(s.insert(Feature::Cache));
        assert!(!s.insert(Feature::Cache));
        assert_eq!(s.len(), 1);
        assert!(s.contains(Feature::Cache));
        assert!(s.remove(Feature::Cache));
        assert!(s.is_empty());
    }

    #[test]
    fn uncompiled_detection() {
        let compiled: FeatureSet = [Feature::Cache].into_iter().collect();
        let active: FeatureSet = [Feature::Cache, Feature::Pubsub].into_iter().collect();
        assert_eq!(active.first_uncompiled(&compiled), Some(Feature::Pubsub));
        assert_eq!(active.first_uncompiled(&FeatureSet::all()), None);
    }
}
