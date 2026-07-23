#![forbid(unsafe_code)]

#[cfg(feature = "cold")]
mod cold;
mod engine;
mod hot;
mod lock_table;
mod object_store;
#[cfg(feature = "s3")]
mod s3_store;
mod sharded_store;
#[cfg(feature = "cold")]
mod tiered;
mod warm;
mod wal;
mod wal_writer;

#[cfg(feature = "cold")]
pub use cold::ColdEngine;
pub use engine::{StorageEngine, StorageError, StorageTier};
pub use hot::HotEngine;
pub use lock_table::KeyLockTable;
pub use object_store::{LocalDirStore, ObjectStore};
#[cfg(feature = "s3")]
pub use s3_store::{S3Config, S3Store};
pub use sharded_store::{FlushPolicy, ShardedObjectStore};
#[cfg(feature = "cold")]
pub use tiered::{TierStats, TieredEngine};
pub use warm::WarmEngine;
pub use wal::{frame_record, SparseIndex, Wal, WalOp, WalRecord};
pub use wal_writer::{FsyncPolicy, WalWriter};