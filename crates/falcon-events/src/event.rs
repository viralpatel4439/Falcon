use crate::hlc::Hlc;
use serde::{Deserialize, Serialize};

pub type Sequence = u64;
pub type Timestamp = u128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub keyspace: String,
    pub key: Vec<u8>,
    pub value: ChangeValue,
    pub sequence: Sequence,
    pub timestamp: Timestamp,
    pub origin_region: String,
    /// Hybrid logical clock stamp. Authoritative for cross-region
    /// last-write-wins ordering in multi-leader mode. Defaults to
    /// `Hlc::zero()` on the single-leader path, where `sequence` orders
    /// writes instead — so single-leader behavior is unchanged.
    #[serde(default = "Hlc::zero")]
    pub hlc: Hlc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeValue {
    Put(Vec<u8>),
    Delete,
}

impl ChangeEvent {
    pub fn is_tombstone(&self) -> bool {
        matches!(self.value, ChangeValue::Delete)
    }
}

pub fn now_millis() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}
