use crate::event::ChangeEvent;
use tokio::sync::broadcast;

const DEFAULT_CAPACITY: usize = 1024;

/// Broadcasts `ChangeEvent`s to any number of subscribers (WebSocket clients,
/// replication wake-up listeners). Cheap to hold, but deliberately NOT
/// constructed for a keyspace unless subscriptions or replication are
/// enabled for it — see `kv-core::Keyspace`.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<ChangeEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Best-effort publish; returns the number of active receivers notified.
    /// Never blocks and never fails the caller if there are no subscribers.
    pub fn publish(&self, event: ChangeEvent) -> usize {
        self.sender.send(event).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
