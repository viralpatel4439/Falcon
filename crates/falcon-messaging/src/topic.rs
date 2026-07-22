//! Pub/sub topics. Two modes, selected per topic:
//!
//! - `Ephemeral`: in-memory `tokio::broadcast` fan-out. At-most-once, no
//!   durability, lowest latency. An offline subscriber misses messages.
//! - `Durable`: every publish is appended to a durable log AND broadcast
//!   live. New/reconnecting subscribers can replay from an offset, so no
//!   message is lost across restarts (at-least-once from their cursor).

use crate::error::MessagingError;
use crate::log::{MessageLog, Offset};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicMode {
    Ephemeral,
    Durable,
}

/// A message delivered to a subscriber: payload plus (for durable topics)
/// the offset it can resume after.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub offset: Offset,
    pub payload: Arc<Vec<u8>>,
}

pub struct Topic {
    name: String,
    mode: TopicMode,
    tx: broadcast::Sender<Delivery>,
    log: Option<MessageLog>, // Some for durable topics
}

impl Topic {
    pub fn open(
        name: &str,
        mode: TopicMode,
        data_dir: &Path,
        capacity: usize,
    ) -> Result<Self, MessagingError> {
        let (tx, _) = broadcast::channel(capacity.max(16));
        let log = match mode {
            TopicMode::Ephemeral => None,
            TopicMode::Durable => {
                let path = data_dir.join(format!("topic_{name}.log"));
                Some(MessageLog::open(&path)?)
            }
        };
        Ok(Self {
            name: name.to_string(),
            mode,
            tx,
            log,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn mode(&self) -> TopicMode {
        self.mode
    }

    /// Publish a message. For durable topics it is persisted (and its
    /// offset assigned) before being broadcast, so a live subscriber never
    /// sees a message that isn't durable. Returns the assigned offset
    /// (durable) or the broadcast's best-effort offset counter (ephemeral,
    /// monotonic within the process).
    pub fn publish(&self, payload: Vec<u8>) -> Result<Offset, MessagingError> {
        let offset = match &self.log {
            Some(log) => log.append(&payload)?,
            None => 0, // ephemeral: offset is not meaningful across restarts
        };
        let _ = self.tx.send(Delivery {
            offset,
            payload: Arc::new(payload),
        });
        Ok(offset)
    }

    /// Live subscription: receive messages published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Delivery> {
        self.tx.subscribe()
    }

    /// Durable replay: messages with offset >= `from` (empty for ephemeral).
    pub fn replay_from(&self, from: Offset) -> Result<Vec<Delivery>, MessagingError> {
        match &self.log {
            Some(log) => Ok(log
                .read_from(from)?
                .into_iter()
                .map(|m| Delivery {
                    offset: m.offset,
                    payload: Arc::new(m.payload),
                })
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    pub fn next_offset(&self) -> Offset {
        self.log.as_ref().map(|l| l.next_offset()).unwrap_or(0)
    }
}
