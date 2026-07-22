#![forbid(unsafe_code)]

mod cold;
mod engine;
mod file_per_key;
mod hot;
mod lock_table;
mod object_store;
mod sharded_store;
mod tiered;
mod warm;
mod wal;
mod wal_writer;

pub use cold::ColdEngine;
pub use engine::{StorageEngine, StorageError, StorageTier};
pub use file_per_key::FilePerKeyEngine;
pub use hot::HotEngine;
pub use lock_table::KeyLockTable;
pub use object_store::{LocalDirStore, ObjectStore};
pub use sharded_store::{FlushPolicy, ShardedObjectStore};
pub use tiered::{TierStats, TieredEngine};
pub use warm::WarmEngine;
pub use wal::{frame_record, SparseIndex, Wal, WalOp, WalRecord};
pub use wal_writer::{FsyncPolicy, WalWriter};