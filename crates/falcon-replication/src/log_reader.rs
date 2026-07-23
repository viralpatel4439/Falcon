use falcon_events::{ChangeEvent, Sequence};
use falcon_storage::{ColdEngine, StorageEngine, StorageError, StorageTier, TieredEngine, WarmEngine};
use std::sync::Arc;

/// Reads the durable, resumable log behind a warm or cold storage engine.
/// This is the seam a future Raft-based log would implement instead.
pub trait ReplicationLogReader: Send + Sync {
    fn read_from(&self, sequence: Sequence) -> Result<Vec<ChangeEvent>, StorageError>;
    fn current_sequence(&self) -> Sequence;
}

pub struct WarmLogReader(pub Arc<WarmEngine>);

impl ReplicationLogReader for WarmLogReader {
    fn read_from(&self, sequence: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        self.0.read_replog_from(sequence)
    }

    fn current_sequence(&self) -> Sequence {
        self.0.last_applied_sequence()
    }
}

pub struct ColdLogReader(pub Arc<ColdEngine>);

impl ReplicationLogReader for ColdLogReader {
    fn read_from(&self, sequence: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        self.0.read_replog_from(sequence)
    }

    fn current_sequence(&self) -> Sequence {
        self.0.last_applied_sequence()
    }
}

pub struct TieredLogReader(pub Arc<TieredEngine>);

impl ReplicationLogReader for TieredLogReader {
    fn read_from(&self, sequence: Sequence) -> Result<Vec<ChangeEvent>, StorageError> {
        self.0.read_replog_from(sequence)
    }

    fn current_sequence(&self) -> Sequence {
        self.0.last_applied_sequence()
    }
}

/// Builds a log reader from a type-erased engine by downcasting to its
/// concrete warm/cold type. Returns `None` for the hot tier, which is not
/// eligible for replication (rejected earlier at config-validation time).
///
/// Note: `engine` must be the *same* `Arc` stored in the `Keyspace` so that
/// writes observed through one handle are visible through the other.
pub fn build_log_reader(engine: &Arc<dyn StorageEngine>) -> Option<Arc<dyn ReplicationLogReader>> {
    match engine.tier() {
        StorageTier::Hot => None,
        StorageTier::Warm => {
            let any = Arc::clone(engine).as_any_arc();
            let concrete = any.downcast::<WarmEngine>().ok()?;
            Some(Arc::new(WarmLogReader(concrete)) as Arc<dyn ReplicationLogReader>)
        }
        StorageTier::Cold => {
            let any = Arc::clone(engine).as_any_arc();
            let concrete = any.downcast::<ColdEngine>().ok()?;
            Some(Arc::new(ColdLogReader(concrete)) as Arc<dyn ReplicationLogReader>)
        }
        StorageTier::Tiered => {
            let any = Arc::clone(engine).as_any_arc();
            let concrete = any.downcast::<TieredEngine>().ok()?;
            Some(Arc::new(TieredLogReader(concrete)) as Arc<dyn ReplicationLogReader>)
        }
        // The sharded object-store tier has no ordered durable log to stream,
        // so it can't be a replication *source* (leader) — it can still be a
        // replication target (apply_replicated works). None reflects that.
        StorageTier::Sharded => None,
    }
}
