#![forbid(unsafe_code)]

//! Messaging layer: pub/sub topics (ephemeral or durable) and durable work
//! queues (at-least-once with ack + redelivery). Reuses the same durable
//! append-log pattern as the KV WAL. Exposed over the wire protocol,
//! WebSocket, and REST by the servers that hold a `Messaging` handle.

mod error;
mod log;
mod queue;
mod stream;
mod topic;

pub use error::MessagingError;
pub use log::Offset;
pub use queue::{Queue, QueueMessage};
pub use stream::{Stream, StreamRecord};
pub use topic::{Delivery, Topic, TopicMode};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Declarative setup for one topic.
#[derive(Clone, Debug)]
pub struct TopicSpec {
    pub name: String,
    pub mode: TopicMode,
    pub capacity: usize,
}

/// Declarative setup for one queue.
#[derive(Clone, Debug)]
pub struct QueueSpec {
    pub name: String,
    pub ack_timeout: Duration,
}

/// Declarative setup for one event stream.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    pub name: String,
    pub partitions: usize,
    pub capacity: usize,
    /// 0 = fsync every append (full durability). > 0 = coalesce fsyncs across
    /// partitions on this interval (higher throughput, bounded loss window).
    pub interval_fsync_ms: u64,
}

/// Owns all topics and queues, built once at startup. Cheap to clone-share
/// via `Arc`. Servers call `topic(...)`/`queue(...)` to route ops.
pub struct Messaging {
    topics: HashMap<String, Arc<Topic>>,
    queues: HashMap<String, Arc<Queue>>,
    streams: HashMap<String, Arc<Stream>>,
}

impl Messaging {
    pub fn build(
        data_dir: PathBuf,
        topics: &[TopicSpec],
        queues: &[QueueSpec],
        streams: &[StreamSpec],
    ) -> Result<Self, MessagingError> {
        // Each messaging product gets its OWN subdirectory under `data_dir`.
        // Running Pub/Sub, Queue, and Event Stream on one node then keeps their
        // files in separate directories (`pubsub/`, `queue/`, `stream/`) — no
        // resource of one product can ever collide with an identically named
        // resource of another (e.g. a `pubsub` topic named "events" vs. a
        // `stream` named "events").
        let topic_dir = data_dir.join("pubsub");
        let queue_dir = data_dir.join("queue");
        let stream_dir = data_dir.join("stream");
        std::fs::create_dir_all(&topic_dir)?;
        std::fs::create_dir_all(&queue_dir)?;
        std::fs::create_dir_all(&stream_dir)?;

        // Back-compat: older nodes kept every messaging file in one flat
        // `messaging/` folder. If that layout is present, move each product's
        // files into its new per-product directory so an upgraded node keeps its
        // durable topics/queues/streams.
        let legacy = data_dir.join("messaging");
        for spec in topics {
            migrate_legacy(
                &legacy.join(format!("topic_{}.log", spec.name)),
                &topic_dir.join(format!("topic_{}.log", spec.name)),
            );
        }
        for spec in queues {
            migrate_legacy(
                &legacy.join(format!("queue_{}.log", spec.name)),
                &queue_dir.join(format!("queue_{}.log", spec.name)),
            );
        }
        for spec in streams {
            migrate_legacy(
                &legacy.join(format!("stream_{}", spec.name)),
                &stream_dir.join(format!("stream_{}", spec.name)),
            );
        }

        let mut topic_map = HashMap::new();
        for spec in topics {
            let t = Topic::open(&spec.name, spec.mode, &topic_dir, spec.capacity)?;
            topic_map.insert(spec.name.clone(), Arc::new(t));
        }
        let mut queue_map = HashMap::new();
        for spec in queues {
            let q = Queue::open(&spec.name, &queue_dir, spec.ack_timeout)?;
            queue_map.insert(spec.name.clone(), Arc::new(q));
        }
        let mut stream_map = HashMap::new();
        for spec in streams {
            let s = Stream::open_with_fsync(
                &spec.name,
                spec.partitions,
                &stream_dir,
                spec.capacity,
                spec.interval_fsync_ms,
            )?;
            stream_map.insert(spec.name.clone(), Arc::new(s));
        }
        Ok(Self {
            topics: topic_map,
            queues: queue_map,
            streams: stream_map,
        })
    }

    pub fn topic(&self, name: &str) -> Option<&Arc<Topic>> {
        self.topics.get(name)
    }

    pub fn queue(&self, name: &str) -> Option<&Arc<Queue>> {
        self.queues.get(name)
    }

    pub fn stream(&self, name: &str) -> Option<&Arc<Stream>> {
        self.streams.get(name)
    }

    pub fn topic_names(&self) -> impl Iterator<Item = &str> {
        self.topics.keys().map(|s| s.as_str())
    }

    pub fn queue_names(&self) -> impl Iterator<Item = &str> {
        self.queues.keys().map(|s| s.as_str())
    }

    pub fn stream_names(&self) -> impl Iterator<Item = &str> {
        self.streams.keys().map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.topics.is_empty() && self.queues.is_empty() && self.streams.is_empty()
    }
}

/// Move a legacy file/dir into its new location if the new one doesn't yet exist.
/// Best-effort: a failed rename is ignored (the product then starts empty).
fn migrate_legacy(legacy: &std::path::Path, new: &std::path::Path) {
    if !new.exists() && legacy.exists() {
        let _ = std::fs::rename(legacy, new);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn products_use_separate_storage_directories() {
        // A Pub/Sub topic and an Event Stream that share the name "events" must
        // live in DIFFERENT directories so neither can read the other's files.
        let dir = tempfile::tempdir().unwrap();
        let m = Messaging::build(
            dir.path().to_path_buf(),
            &[TopicSpec {
                name: "events".into(),
                mode: TopicMode::Durable,
                capacity: 16,
            }],
            &[QueueSpec {
                name: "jobs".into(),
                ack_timeout: Duration::from_secs(30),
            }],
            &[StreamSpec {
                name: "events".into(),
                partitions: 1,
                capacity: 16,
                interval_fsync_ms: 0,
            }],
        )
        .unwrap();
        assert!(m.topic("events").is_some());
        assert!(m.stream("events").is_some());

        // Each product wrote under its own subdirectory.
        assert!(dir.path().join("pubsub").join("topic_events.log").exists());
        assert!(dir.path().join("queue").join("queue_jobs.log").exists());
        assert!(dir.path().join("stream").join("stream_events").is_dir());
    }

    #[test]
    fn legacy_flat_layout_is_migrated() {
        // Simulate an old node: files in a single `messaging/` folder.
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("messaging");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("queue_jobs.log"), b"old-data").unwrap();

        let _m = Messaging::build(
            dir.path().to_path_buf(),
            &[],
            &[QueueSpec {
                name: "jobs".into(),
                ack_timeout: Duration::from_secs(30),
            }],
            &[],
        )
        .unwrap();

        // The old queue file was moved into the new per-product directory.
        let moved = dir.path().join("queue").join("queue_jobs.log");
        assert!(moved.exists(), "legacy queue file was not migrated");
        assert_eq!(std::fs::read(moved).unwrap(), b"old-data");
    }
}