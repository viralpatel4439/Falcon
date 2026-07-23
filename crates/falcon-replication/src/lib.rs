#![forbid(unsafe_code)]

mod client;
mod log_reader;
mod server;

pub use client::{run_follower, run_peer_follower, ApplyFn};
pub use log_reader::{build_log_reader, ReplicationLogReader, WarmLogReader};
#[cfg(feature = "cold")]
pub use log_reader::{ColdLogReader, TieredLogReader};
pub use server::{KeyspaceReplicationSource, ReplicationServerImpl};