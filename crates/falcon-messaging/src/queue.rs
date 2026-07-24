//! Durable work queues with at-least-once delivery.
//!
//! A queue is a durable append log plus, per consumer group, a cursor and
//! an in-flight set. `pop` hands the next undelivered message to a consumer
//! and marks it in-flight with a delivery deadline; `ack` removes it.
//! Messages whose ack deadline passes are redelivered (at-least-once).
//! Competing consumers in the SAME group share the stream (work
//! distribution); different groups each get the full stream independently.

use crate::error::MessagingError;
use crate::log::{MessageLog, Offset};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct QueueMessage {
    pub offset: Offset,
    pub payload: Vec<u8>,
}

struct InFlight {
    deadline: Instant,
}

struct GroupState {
    /// Next offset never yet delivered to this group.
    cursor: Offset,
    /// Delivered-but-unacked offsets and their redelivery deadlines.
    in_flight: BTreeMap<Offset, InFlight>,
}

impl GroupState {
    fn new() -> Self {
        Self {
            cursor: 1,
            in_flight: BTreeMap::new(),
        }
    }
}

pub struct Queue {
    name: String,
    log: MessageLog,
    ack_timeout: Duration,
    groups: Mutex<HashMap<String, GroupState>>,
}

impl Queue {
    pub fn open(name: &str, data_dir: &Path, ack_timeout: Duration) -> Result<Self, MessagingError> {
        let path = data_dir.join(format!("queue_{name}.log"));
        let log = MessageLog::open(&path)?;
        Ok(Self {
            name: name.to_string(),
            log,
            ack_timeout,
            groups: Mutex::new(HashMap::new()),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Enqueue a message (durable). Returns its offset.
    pub fn push(&self, payload: &[u8]) -> Result<Offset, MessagingError> {
        self.log.append(payload)
    }

    /// Deliver the next available message to a consumer in `group`.
    /// Prefers a redelivery (an in-flight message whose ack deadline has
    /// passed) over a fresh one, so timed-out work is retried promptly.
    /// Returns `None` if the queue is currently drained for this group.
    pub fn pop(&self, group: &str) -> Result<Option<QueueMessage>, MessagingError> {
        let mut groups = self.groups.lock().expect("queue groups mutex poisoned");
        let state = groups.entry(group.to_string()).or_insert_with(GroupState::new);

        // 1. Redeliver the oldest timed-out in-flight message, if any.
        let now = Instant::now();
        let redeliver: Option<Offset> = state
            .in_flight
            .iter()
            .find(|(_, f)| f.deadline <= now)
            .map(|(&off, _)| off);
        if let Some(off) = redeliver {
            state.in_flight.insert(
                off,
                InFlight {
                    deadline: now + self.ack_timeout,
                },
            );
            drop(groups);
            let msgs = self.log.read_from(off)?;
            if let Some(m) = msgs.into_iter().find(|m| m.offset == off) {
                return Ok(Some(QueueMessage {
                    offset: m.offset,
                    payload: m.payload,
                }));
            }
            return Ok(None);
        }

        // 2. Otherwise deliver the next fresh message at/after the cursor.
        //
        // The `groups` lock is held across the cursor read, the log read, and
        // the cursor advance so the whole reserve-and-mark step is atomic. If we
        // dropped the lock around `read_from`, two concurrent `pop`s for the same
        // group could both observe the same cursor, both select the same offset,
        // and hand the SAME job to two competing consumers — breaking work
        // distribution. `read_from` only reads the append log and never touches
        // `groups`, so holding the lock across it cannot deadlock.
        let cursor = state.cursor;
        let msgs = self.log.read_from(cursor)?;
        let next = msgs.into_iter().find(|m| m.offset >= cursor);
        match next {
            Some(m) => {
                // Advance cursor past this offset and mark it in-flight.
                state.cursor = m.offset + 1;
                state.in_flight.insert(
                    m.offset,
                    InFlight {
                        deadline: Instant::now() + self.ack_timeout,
                    },
                );
                Ok(Some(QueueMessage {
                    offset: m.offset,
                    payload: m.payload,
                }))
            }
            None => Ok(None),
        }
    }

    /// Acknowledge a delivered message so it is not redelivered.
    pub fn ack(&self, group: &str, offset: Offset) {
        let mut groups = self.groups.lock().expect("queue groups mutex poisoned");
        if let Some(state) = groups.get_mut(group) {
            state.in_flight.remove(&offset);
        }
    }

    /// Number of in-flight (delivered, unacked) messages for a group.
    pub fn in_flight_count(&self, group: &str) -> usize {
        let groups = self.groups.lock().expect("queue groups mutex poisoned");
        groups.get(group).map(|s| s.in_flight.len()).unwrap_or(0)
    }

    pub fn depth(&self) -> Offset {
        self.log.next_offset().saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn open_queue(name: &str) -> (tempfile::TempDir, Arc<Queue>) {
        let dir = tempfile::tempdir().unwrap();
        let q = Arc::new(Queue::open(name, dir.path(), Duration::from_secs(30)).unwrap());
        (dir, q)
    }

    #[test]
    fn competing_consumers_never_get_the_same_job() {
        // Many concurrent poppers in ONE group must each receive a DISTINCT
        // offset — no job may be handed to two consumers at once. This is the
        // regression test for the cursor race where two pops could both observe
        // the same cursor and deliver the same offset.
        let (_dir, q) = open_queue("jobs");
        const N: usize = 500;
        for i in 0..N {
            q.push(format!("job-{i}").as_bytes()).unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..16 {
            let q = Arc::clone(&q);
            handles.push(std::thread::spawn(move || {
                let mut got = Vec::new();
                while let Some(m) = q.pop("workers").unwrap() {
                    got.push(m.offset);
                }
                got
            }));
        }

        let mut all: Vec<Offset> = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }

        // Every fresh delivery is unique...
        let unique: HashSet<Offset> = all.iter().copied().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "a job was delivered to more than one consumer"
        );
        // ...and every pushed job was delivered exactly once.
        assert_eq!(unique.len(), N, "not every job was delivered exactly once");
    }

    #[test]
    fn different_groups_each_see_every_job() {
        let (_dir, q) = open_queue("jobs");
        for i in 0..10 {
            q.push(format!("job-{i}").as_bytes()).unwrap();
        }
        for group in ["a", "b"] {
            let mut count = 0;
            while let Some(m) = q.pop(group).unwrap() {
                q.ack(group, m.offset);
                count += 1;
            }
            assert_eq!(count, 10, "group {group} did not see all jobs");
        }
    }
}
