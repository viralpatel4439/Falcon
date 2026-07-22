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
        let mut topic_map = HashMap::new();
        for spec in topics {
            let t = Topic::open(&spec.name, spec.mode, &data_dir, spec.capacity)?;
            topic_map.insert(spec.name.clone(), Arc::new(t));
        }
        let mut queue_map = HashMap::new();
        for spec in queues {
            let q = Queue::open(&spec.name, &data_dir, spec.ack_timeout)?;
            queue_map.insert(spec.name.clone(), Arc::new(q));
        }
        let mut stream_map = HashMap::new();
        for spec in streams {
            let s = Stream::open(&spec.name, spec.partitions, &data_dir, spec.capacity)?;
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